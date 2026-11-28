package engine

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"freud/internal/db"
	"freud/internal/runner"
	"freud/internal/term"
	"freud/internal/workflow"
)

func (e *Engine) executeSubflowTask(
	ctx context.Context,
	parentRunID string,
	taskRunID int64,
	parentWF *workflow.Workflow,
	task workflow.Task,
	parentOpts runOptions,
) (*runner.Result, error) {
	if len(parentOpts.flowStack) >= maxSubflowDepth {
		return &runner.Result{ExitCode: 1}, fmt.Errorf("subflow depth limit (%d) exceeded calling %q", maxSubflowDepth, task.Flow)
	}
	for _, activeName := range parentOpts.flowStack {
		if activeName == task.Flow {
			chain := append(append([]string{}, parentOpts.flowStack...), task.Flow)
			return &runner.Result{ExitCode: 1}, fmt.Errorf("subflow cycle detected: %s", strings.Join(chain, " -> "))
		}
	}

	flowsDir := e.flowsDir
	if flowsDir == "" {
		flowsDir = parentWF.BaseDir
	}
	if flowsDir == "" {
		flowsDir = "."
	}

	childWF, err := workflow.FindByName(flowsDir, task.Flow)
	if err != nil {
		return &runner.Result{ExitCode: 1}, err
	}

	nextStack := make([]string, len(parentOpts.flowStack)+1)
	copy(nextStack, parentOpts.flowStack)
	nextStack[len(parentOpts.flowStack)] = childWF.Name

	childOpts := runOptions{
		baseEnv:      task.Env,
		runEnv:       parentOpts.runEnv,
		parentRunID:  parentRunID,
		parentTaskID: task.ID,
		triggeredBy:  parentOpts.triggeredBy,
		flowStack:    nextStack,
	}

	childStartCtx := ctx
	if !task.ShouldWait() {
		childStartCtx = context.WithoutCancel(ctx)
	}

	childRunID, childDone, err := e.startAsyncWithOptions(childStartCtx, childWF, childOpts)
	if err != nil {
		return &runner.Result{ExitCode: 1}, fmt.Errorf("start subflow %q: %w", task.Flow, err)
	}

	var logBuf strings.Builder
	fmt.Fprintf(&logBuf, "[subflow] Started workflow %q (run_id=%s, wait=%v)\n", childWF.Name, childRunID, task.ShouldWait())
	_ = e.store.UpdateTaskRunLogs(ctx, taskRunID, logBuf.String())

	if !task.ShouldWait() {
		fmt.Fprintf(&logBuf, "[subflow] Dispatched async subflow %q (run_id=%s)\n", childWF.Name, childRunID)
		e.wg.Add(1)
		go func() {
			defer e.wg.Done()
			if childErr := <-childDone; childErr != nil {
				ec := e.errColor()
				e.logErr("%s Async child run %s (%s) %s: %v\n",
					term.Tag(ec, "subflow"), childRunID, childWF.Name, term.Status(ec, "FAILED"), childErr)
			}
		}()
		return &runner.Result{ExitCode: 0, Logs: logBuf.String()}, nil
	}

	childErr := <-childDone
	if childErr != nil {
		fmt.Fprintf(&logBuf, "[subflow] Child run %s FAILED: %v\n", childRunID, childErr)
		return &runner.Result{ExitCode: 1, Logs: logBuf.String()}, fmt.Errorf("subflow %q (run_id=%s) failed: %w", childWF.Name, childRunID, childErr)
	}

	fmt.Fprintf(&logBuf, "[subflow] Child run %s SUCCEEDED\n", childRunID)
	return &runner.Result{ExitCode: 0, Logs: logBuf.String()}, nil
}

func (e *Engine) runTaskWithLiveLogs(ctx context.Context, runID, hostWorkspace string, taskRunID int64, task workflow.Task, logPrefix string) (*runner.Result, error) {
	sr, ok := e.runner.(StreamingTaskRunner)
	if !ok {
		return e.runner.RunTask(ctx, runID, hostWorkspace, task)
	}

	var (
		mu     sync.Mutex
		latest string
		dirty  bool
		stopCh = make(chan struct{})
		doneCh = make(chan struct{})
	)

	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(logFlushInterval)
		defer ticker.Stop()

		flush := func() {
			mu.Lock()
			if !dirty {
				mu.Unlock()
				return
			}
			snapshot := logPrefix + latest
			dirty = false
			mu.Unlock()

			e.updateTaskLogs(taskRunID, snapshot)
		}

		for {
			select {
			case <-stopCh:
				flush()
				return
			case <-ticker.C:
				flush()
			}
		}
	}()

	res, err := sr.RunTaskStream(ctx, runID, hostWorkspace, task, func(snapshot string) {
		mu.Lock()
		latest = snapshot
		dirty = true
		mu.Unlock()
	})
	close(stopCh)
	<-doneCh
	return res, err
}

func (e *Engine) recordSkippedTask(ctx context.Context, runID string, task workflow.Task, reason string) error {
	taskRunID, err := e.store.CreateTaskRunWithMeta(ctx, runID, task.ID, task.Runtime, task.Image)
	if err != nil {
		return fmt.Errorf("record skipped task %q: %w", task.ID, err)
	}
	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	msg := "[freud] Skipped"
	if reason != "" {
		msg += ": " + reason
	}
	if err := e.store.FinishTaskRun(dbCtx, taskRunID, db.StatusSkipped, 0, msg); err != nil {
		return fmt.Errorf("finish skipped task %q: %w", task.ID, err)
	}
	return nil
}

func (e *Engine) updateTaskLogs(taskRunID int64, logs string) {
	dbCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = e.store.UpdateTaskRunLogs(dbCtx, taskRunID, logs)
}

func (e *Engine) markTaskFinished(taskRunID int64, status string, exitCode int64, logs string) {
	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := e.store.FinishTaskRun(dbCtx, taskRunID, status, exitCode, logs); err != nil {
		ec := e.errColor()
		e.logErr("%s %s: failed to persist task_run %d status: %v\n",
			term.Tag(ec, "engine"), term.Status(ec, "Warning"), taskRunID, err)
	}
}

func (e *Engine) markRunFailed(runID string) {
	e.markRunStatus(runID, db.StatusFailed)
}

func (e *Engine) markRunStatus(runID, status string) {
	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := e.store.FinishRun(dbCtx, runID, status); err != nil {
		ec := e.errColor()
		e.logErr("%s %s: failed to persist run %s %s status: %v\n",
			term.Tag(ec, "engine"), term.Status(ec, "Warning"), runID, status, err)
	}
}
