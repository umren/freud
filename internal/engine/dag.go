package engine

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"freud/internal/db"
	"freud/internal/env"
	"freud/internal/runner"
	"freud/internal/term"
	"freud/internal/workflow"
)

type dagTaskState int

const (
	dagPending dagTaskState = iota
	dagRunning
	dagSuccess
	dagFailed
	dagSkipped
	dagAborted
)

type dagCompletion struct {
	taskID   string
	state    dagTaskState
	exitCode int64
	err      error
}

type dagExecution struct {
	engine        *Engine
	ctx           context.Context
	runID         string
	hostWorkspace string
	wf            *workflow.Workflow
	orderedTasks  []workflow.Task
	opts          runOptions

	states     map[string]dagTaskState
	depsOf     map[string][]string
	remaining  map[string]int
	dependents map[string][]string

	sem       chan struct{}
	resultsCh chan dagCompletion
	inFlight  int

	failMu   sync.RWMutex
	firstErr error
}

func (e *Engine) executeRun(ctx context.Context, runID, hostWorkspace string, wf *workflow.Workflow, orderedTasks []workflow.Task, opts runOptions) error {
	dag := newDAGExecution(e, ctx, runID, hostWorkspace, wf, orderedTasks, opts)
	return dag.run()
}

func newDAGExecution(
	e *Engine,
	ctx context.Context,
	runID, hostWorkspace string,
	wf *workflow.Workflow,
	orderedTasks []workflow.Task,
	opts runOptions,
) *dagExecution {
	n := len(orderedTasks)
	states := make(map[string]dagTaskState, n)
	depsOf := make(map[string][]string, n)
	remaining := make(map[string]int, n)
	dependents := make(map[string][]string, n)

	for _, t := range orderedTasks {
		states[t.ID] = dagPending
		seen := make(map[string]struct{}, len(t.DependsOn))
		var uniq []string
		for _, dep := range t.DependsOn {
			if _, dup := seen[dep]; dup {
				continue
			}
			seen[dep] = struct{}{}
			uniq = append(uniq, dep)
			dependents[dep] = append(dependents[dep], t.ID)
		}
		depsOf[t.ID] = uniq
		remaining[t.ID] = len(uniq)
	}

	var sem chan struct{}
	if wf.Concurrency > 0 {
		sem = make(chan struct{}, wf.Concurrency)
	}

	return &dagExecution{
		engine:        e,
		ctx:           ctx,
		runID:         runID,
		hostWorkspace: hostWorkspace,
		wf:            wf,
		orderedTasks:  orderedTasks,
		opts:          opts,
		states:        states,
		depsOf:        depsOf,
		remaining:     remaining,
		dependents:    dependents,
		sem:           sem,
		resultsCh:     make(chan dagCompletion, n),
	}
}

func (d *dagExecution) hasFailure() bool {
	d.failMu.RLock()
	defer d.failMu.RUnlock()
	return d.firstErr != nil
}

func (d *dagExecution) setFailure(err error) {
	if err == nil {
		return
	}
	d.failMu.Lock()
	if d.firstErr == nil {
		d.firstErr = err
	}
	d.failMu.Unlock()
}

func (d *dagExecution) firstError() error {
	d.failMu.RLock()
	defer d.failMu.RUnlock()
	return d.firstErr
}

func (d *dagExecution) run() error {
	c := d.engine.outColor()
	d.engine.logOut("%s Starting workflow %s %s\n",
		term.Tag(c, "engine"),
		term.Key(c, fmt.Sprintf("%q", d.wf.Name)),
		term.Muted(c, fmt.Sprintf("(run_id=%s, workspace=%s)", d.runID, d.hostWorkspace)),
	)

	for {
		d.dispatchRegularTasks()
		if d.inFlight == 0 && d.dispatchGlobalFailureTasks() {
			continue
		}
		if d.inFlight == 0 {
			break
		}

		comp := <-d.resultsCh
		d.inFlight--
		d.setFailure(comp.err)
		d.markTerminal(comp.taskID, comp.state)
	}

	return d.finalizeRun()
}

func (d *dagExecution) dispatchRegularTasks() {
	for progressed := true; progressed; {
		progressed = false
		for _, task := range d.orderedTasks {
			if d.states[task.ID] != dagPending || d.isGlobalOnFailure(task) {
				continue
			}
			if d.engine.isKilled(d.runID) || d.ctx.Err() != nil {
				d.markTerminal(task.ID, dagAborted)
				progressed = true
				continue
			}
			if d.remaining[task.ID] > 0 {
				continue
			}

			if !d.canProceedByTriggerRule(task) {
				progressed = true
				continue
			}
			d.evaluateIfAndLaunch(task)
			progressed = true
		}
	}
}

func (d *dagExecution) dispatchGlobalFailureTasks() bool {
	for _, task := range d.orderedTasks {
		if d.states[task.ID] == dagPending && !d.isGlobalOnFailure(task) {
			return false
		}
	}

	launched := false
	for _, task := range d.orderedTasks {
		if d.states[task.ID] != dagPending || !d.isGlobalOnFailure(task) || d.remaining[task.ID] > 0 {
			continue
		}
		if d.engine.isKilled(d.runID) || d.ctx.Err() != nil {
			d.markTerminal(task.ID, dagAborted)
			continue
		}
		if !d.hasFailure() {
			_ = d.skipTask(task, "when=on_failure and workflow succeeded")
			continue
		}
		if d.evaluateIfAndLaunch(task) {
			launched = true
		}
	}
	return launched
}

func (d *dagExecution) isGlobalOnFailure(t workflow.Task) bool {
	return t.NormalizedWhen() == workflow.WhenOnFailure && len(d.depsOf[t.ID]) == 0
}

func (d *dagExecution) canProceedByTriggerRule(task workflow.Task) bool {
	var anyFailed, anySkipped, anyAborted bool
	for _, depID := range d.depsOf[task.ID] {
		switch d.states[depID] {
		case dagFailed:
			anyFailed = true
		case dagSkipped:
			anySkipped = true
		case dagAborted:
			anyAborted = true
		}
	}

	switch task.NormalizedWhen() {
	case workflow.WhenOnSuccess:
		if anyFailed || anyAborted || d.hasFailure() {
			d.markTerminal(task.ID, dagAborted)
			return false
		}
		if anySkipped {
			_ = d.skipTask(task, "upstream task skipped")
			return false
		}
	case workflow.WhenOnFailure:
		if !anyFailed {
			_ = d.skipTask(task, "when=on_failure and no dependency failed")
			return false
		}
	case workflow.WhenAlways:
		if anyAborted && d.engine.isKilled(d.runID) {
			d.markTerminal(task.ID, dagAborted)
			return false
		}
	}
	return true
}

func (d *dagExecution) evaluateIfAndLaunch(task workflow.Task) bool {
	execTask := d.prepareExecTask(task)
	if task.If != "" {
		condOK, condErr := workflow.EvalCondition(task.If, execTask.Env)
		if condErr != nil {
			d.setFailure(fmt.Errorf("task %q condition failed: %w", task.ID, condErr))
			d.markTerminal(task.ID, dagFailed)
			return false
		}
		if !condOK {
			_ = d.skipTask(task, fmt.Sprintf("condition %q evaluated to false", task.If))
			return false
		}
	}

	d.launchTask(task, execTask)
	return true
}

func (d *dagExecution) markTerminal(id string, st dagTaskState) {
	d.states[id] = st
	for _, childID := range d.dependents[id] {
		d.remaining[childID]--
	}
}

func (d *dagExecution) skipTask(task workflow.Task, reason string) error {
	if err := d.engine.recordSkippedTask(d.ctx, d.runID, task, reason); err != nil {
		d.markTerminal(task.ID, dagFailed)
		d.setFailure(err)
		return err
	}
	d.markTerminal(task.ID, dagSkipped)
	c := d.engine.outColor()
	d.engine.logOut("%s Task %s %s %s\n",
		term.Tag(c, "engine"),
		term.Key(c, fmt.Sprintf("%q", task.ID)),
		term.Status(c, db.StatusSkipped),
		term.Muted(c, "("+reason+")"),
	)
	return nil
}

func (d *dagExecution) launchTask(task, execTask workflow.Task) {
	d.states[task.ID] = dagRunning
	d.inFlight++
	whenRule := task.NormalizedWhen()

	go func() {
		if d.sem != nil {
			select {
			case d.sem <- struct{}{}:
				defer func() { <-d.sem }()
			case <-d.ctx.Done():
				d.resultsCh <- dagCompletion{taskID: task.ID, state: dagAborted}
				return
			}
		}
		if d.engine.isKilled(d.runID) || d.ctx.Err() != nil {
			d.resultsCh <- dagCompletion{taskID: task.ID, state: dagAborted}
			return
		}
		if whenRule == workflow.WhenOnSuccess && d.hasFailure() {
			d.resultsCh <- dagCompletion{taskID: task.ID, state: dagAborted}
			return
		}

		st, exitCode, err := d.executeTaskWithRetries(task, execTask)
		d.resultsCh <- dagCompletion{
			taskID:   task.ID,
			state:    st,
			exitCode: exitCode,
			err:      err,
		}
	}()
}

func (d *dagExecution) prepareExecTask(task workflow.Task) workflow.Task {
	execTask := task
	execTask.Env = env.MergeRunEnv(d.opts.baseEnv, task.Env, d.opts.runEnv)
	execTask.Include = mergeIncludes(d.wf.Include, task.Include)
	if execTask.BaseDir == "" {
		if d.wf.BaseDir != "" {
			execTask.BaseDir = d.wf.BaseDir
		} else {
			execTask.BaseDir = "."
		}
	}
	return execTask
}

func (d *dagExecution) executeTaskWithRetries(task, execTask workflow.Task) (dagTaskState, int64, error) {
	c := d.engine.outColor()
	d.logTaskStart(c, task)

	taskRunID, err := d.engine.store.CreateTaskRunWithMeta(d.ctx, d.runID, task.ID, task.Runtime, task.Image)
	if err != nil {
		if d.engine.isKilled(d.runID) {
			return dagAborted, -1, ErrRunKilled
		}
		return dagFailed, -1, fmt.Errorf("record task %q start: %w", task.ID, err)
	}

	attempts := 1 + task.Retries
	timeout := task.TimeoutDuration()
	retryDelay := task.RetryDelayDuration()

	var combinedLogs strings.Builder
	var lastExitCode int64 = -1
	var lastErr error

	for attempt := 1; attempt <= attempts; attempt++ {
		if attempt > 1 {
			if stoppedState, stopCode, stopErr, stopped := d.waitRetryDelay(c, task, taskRunID, attempt, attempts, retryDelay, lastExitCode, &combinedLogs); stopped {
				return stoppedState, stopCode, stopErr
			}
		}

		attemptCtx := d.ctx
		var timeoutCancel context.CancelFunc
		if timeout > 0 {
			attemptCtx, timeoutCancel = context.WithTimeout(d.ctx, timeout)
		}

		var res *runner.Result
		var runErr error
		if task.Flow != "" {
			res, runErr = d.engine.executeSubflowTask(attemptCtx, d.runID, taskRunID, d.wf, execTask, d.opts)
		} else {
			res, runErr = d.engine.runTaskWithLiveLogs(attemptCtx, d.runID, d.hostWorkspace, taskRunID, execTask, combinedLogs.String())
		}
		timedOut := timeout > 0 && errors.Is(attemptCtx.Err(), context.DeadlineExceeded) && d.ctx.Err() == nil
		if timeoutCancel != nil {
			timeoutCancel()
		}

		var exitCode int64 = -1
		if res != nil {
			exitCode = res.ExitCode
			combinedLogs.WriteString(res.Logs)
		}
		lastExitCode = exitCode

		if d.engine.isKilled(d.runID) {
			appendLogLine(&combinedLogs, "[freud] Execution killed by user")
			d.engine.markTaskFinished(taskRunID, db.StatusKilled, exitCode, combinedLogs.String())
			d.engine.logOut("%s Task %s %s\n", term.Tag(c, "engine"), term.Key(c, fmt.Sprintf("%q", task.ID)), term.Status(c, "KILLED"))
			return dagAborted, exitCode, ErrRunKilled
		}

		if runErr == nil && exitCode == 0 {
			d.engine.markTaskFinished(taskRunID, db.StatusSuccess, exitCode, combinedLogs.String())
			d.engine.logOut("%s Task %s %s %s\n",
				term.Tag(c, "engine"),
				term.Key(c, fmt.Sprintf("%q", task.ID)),
				term.Status(c, "SUCCEEDED"),
				term.Muted(c, fmt.Sprintf("(exit_code=%d)", exitCode)),
			)
			return dagSuccess, exitCode, nil
		}

		if timedOut {
			runErr = fmt.Errorf("task %q timed out after %s", task.ID, timeout)
		} else if runErr == nil {
			runErr = fmt.Errorf("task %q exited with non-zero code %d", task.ID, exitCode)
		}
		lastErr = runErr
		appendLogLine(&combinedLogs, fmt.Sprintf("[freud error] %v", runErr))

		if d.ctx.Err() != nil {
			break
		}
	}

	d.engine.markTaskFinished(taskRunID, db.StatusFailed, lastExitCode, combinedLogs.String())
	d.engine.logOut("%s Task %s %s %s\n",
		term.Tag(c, "engine"),
		term.Key(c, fmt.Sprintf("%q", task.ID)),
		term.Status(c, "FAILED"),
		term.Muted(c, fmt.Sprintf("(exit_code=%d)", lastExitCode)),
	)
	return dagFailed, lastExitCode, lastErr
}

func (d *dagExecution) waitRetryDelay(
	c bool,
	task workflow.Task,
	taskRunID int64,
	attempt, attempts int,
	retryDelay time.Duration,
	lastExitCode int64,
	combinedLogs *strings.Builder,
) (dagTaskState, int64, error, bool) {
	d.engine.logOut("%s Retrying task %s %s\n",
		term.Tag(c, "engine"),
		term.Key(c, fmt.Sprintf("%q", task.ID)),
		term.Muted(c, fmt.Sprintf("(attempt=%d/%d, delay=%s)", attempt, attempts, retryDelay)),
	)
	if retryDelay > 0 {
		timer := time.NewTimer(retryDelay)
		select {
		case <-timer.C:
		case <-d.ctx.Done():
			timer.Stop()
			if d.engine.isKilled(d.runID) {
				appendLogLine(combinedLogs, "[freud] Execution killed by user")
				d.engine.markTaskFinished(taskRunID, db.StatusKilled, lastExitCode, combinedLogs.String())
				return dagAborted, lastExitCode, ErrRunKilled, true
			}
			d.engine.markTaskFinished(taskRunID, db.StatusFailed, lastExitCode, combinedLogs.String())
			return dagFailed, lastExitCode, d.ctx.Err(), true
		}
	}

	appendLogLine(combinedLogs, fmt.Sprintf("[freud] Retry attempt %d/%d", attempt, attempts))
	d.engine.updateTaskLogs(taskRunID, combinedLogs.String())
	return dagPending, lastExitCode, nil, false
}

func (d *dagExecution) logTaskStart(c bool, task workflow.Task) {
	switch {
	case task.Flow != "":
		d.engine.logOut("%s Running subflow task %s %s\n",
			term.Tag(c, "engine"),
			term.Key(c, fmt.Sprintf("%q", task.ID)),
			term.Muted(c, fmt.Sprintf("(flow=%q, wait=%v)", task.Flow, task.ShouldWait())),
		)
	case task.Image != "":
		d.engine.logOut("%s Running task %s %s\n",
			term.Tag(c, "engine"),
			term.Key(c, fmt.Sprintf("%q", task.ID)),
			term.Muted(c, fmt.Sprintf("(runtime=%s, image=%s)", task.Runtime, task.Image)),
		)
	default:
		d.engine.logOut("%s Running task %s %s\n",
			term.Tag(c, "engine"),
			term.Key(c, fmt.Sprintf("%q", task.ID)),
			term.Muted(c, fmt.Sprintf("(runtime=%s)", task.Runtime)),
		)
	}
}

func (d *dagExecution) finalizeRun() error {
	if d.engine.isKilled(d.runID) {
		return d.engine.finishCanceledRun(d.runID, d.wf.Name, d.ctx.Err())
	}
	if d.ctx.Err() != nil && !d.hasFailure() {
		return d.engine.finishCanceledRun(d.runID, d.wf.Name, d.ctx.Err())
	}
	if err := d.firstError(); err != nil {
		d.engine.markRunFailed(d.runID)
		return err
	}

	if err := d.engine.store.FinishRun(d.ctx, d.runID, db.StatusSuccess); err != nil {
		if d.engine.isKilled(d.runID) {
			return d.engine.finishCanceledRun(d.runID, d.wf.Name, d.ctx.Err())
		}
		return fmt.Errorf("record workflow run completion: %w", err)
	}

	c := d.engine.outColor()
	d.engine.logOut("%s Workflow %s %s %s\n",
		term.Tag(c, "engine"),
		term.Key(c, fmt.Sprintf("%q", d.wf.Name)),
		term.Muted(c, fmt.Sprintf("(run_id=%s)", d.runID)),
		term.Status(c, "SUCCEEDED"),
	)
	return nil
}

func appendLogLine(sb *strings.Builder, line string) {
	if sb.Len() > 0 && !strings.HasSuffix(sb.String(), "\n") {
		sb.WriteString("\n")
	}
	sb.WriteString(line)
	sb.WriteString("\n")
}
