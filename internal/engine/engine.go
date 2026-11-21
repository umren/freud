package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"freud/internal/db"
	"freud/internal/env"
	"freud/internal/runner"
	"freud/internal/workflow"
)

const (
	defaultWorkspacesDir = "workspaces"
	scriptsDirName       = ".scripts"
	logFlushInterval     = 250 * time.Millisecond
	maxSubflowDepth      = 10
)

// TaskRunner defines the execution backend for running a single workflow task.
type TaskRunner interface {
	RunTask(ctx context.Context, runID, hostWorkspace string, task workflow.Task) (*runner.Result, error)
}

// StreamingTaskRunner is an optional TaskRunner capability for real-time log streaming.
type StreamingTaskRunner interface {
	TaskRunner
	RunTaskStream(ctx context.Context, runID, hostWorkspace string, task workflow.Task, onLog func(string)) (*runner.Result, error)
}

// Engine orchestrates workflow execution, workspace management, and SQLite state tracking.
type Engine struct {
	store          *db.Store
	runner         TaskRunner
	workspacesBase string
	flowsDir       string
	baseEnv        map[string]string
	wg             sync.WaitGroup
}

type runOptions struct {
	baseEnv      map[string]string
	parentRunID  string
	parentTaskID string
	flowStack    []string
}

// New creates a workflow execution Engine.
func New(store *db.Store, taskRunner TaskRunner, workspacesBase string) *Engine {
	if workspacesBase == "" {
		workspacesBase = defaultWorkspacesDir
	}
	return &Engine{
		store:          store,
		runner:         taskRunner,
		workspacesBase: workspacesBase,
	}
}

// SetEnv sets base environment variables (such as those loaded from .env) injected into every task.
func (e *Engine) SetEnv(baseEnv map[string]string) {
	e.baseEnv = baseEnv
}

// SetFlowsDir sets the directory used to resolve subflows referenced by `flow: <name>`.
func (e *Engine) SetFlowsDir(dir string) {
	e.flowsDir = dir
}

// Wait blocks until any fire-and-forget subflows (`wait: false`) complete.
func (e *Engine) Wait() {
	e.wg.Wait()
}

// Run executes all tasks in the workflow in topological order and persists run metadata to SQLite.
func (e *Engine) Run(ctx context.Context, wf *workflow.Workflow) (string, error) {
	opts := runOptions{
		baseEnv:   e.baseEnv,
		flowStack: []string{wf.Name},
	}
	runID, hostWorkspace, orderedTasks, err := e.prepareRun(ctx, wf, opts)
	if err != nil {
		return "", err
	}
	if err := e.executeRun(ctx, runID, hostWorkspace, wf, orderedTasks, opts); err != nil {
		return runID, err
	}
	return runID, nil
}

// StartAsync validates the workflow, creates the run record synchronously, and executes tasks asynchronously.
// The returned channel receives the final execution error (or nil) once the run completes.
func (e *Engine) StartAsync(ctx context.Context, wf *workflow.Workflow) (string, <-chan error, error) {
	opts := runOptions{
		baseEnv:   e.baseEnv,
		flowStack: []string{wf.Name},
	}
	return e.startAsyncWithOptions(ctx, wf, opts)
}

func (e *Engine) startAsyncWithOptions(ctx context.Context, wf *workflow.Workflow, opts runOptions) (string, <-chan error, error) {
	runID, hostWorkspace, orderedTasks, err := e.prepareRun(ctx, wf, opts)
	if err != nil {
		return "", nil, err
	}

	doneCh := make(chan error, 1)
	go func() {
		doneCh <- e.executeRun(ctx, runID, hostWorkspace, wf, orderedTasks, opts)
		close(doneCh)
	}()
	return runID, doneCh, nil
}

func (e *Engine) prepareRun(ctx context.Context, wf *workflow.Workflow, opts runOptions) (string, string, []workflow.Task, error) {
	orderedTasks, err := wf.TopologicalOrder()
	if err != nil {
		return "", "", nil, fmt.Errorf("resolve execution order: %w", err)
	}

	runID, err := generateRunID()
	if err != nil {
		return "", "", nil, fmt.Errorf("generate run id: %w", err)
	}

	hostWorkspace := filepath.Join(e.workspacesBase, runID)
	scriptsDir := filepath.Join(hostWorkspace, scriptsDirName)
	if err := os.MkdirAll(scriptsDir, 0o755); err != nil {
		return "", "", nil, fmt.Errorf("create workspace directory %q: %w", scriptsDir, err)
	}

	defJSON := marshalWorkflowSnapshot(wf)
	if err := e.store.CreateRunWithParent(ctx, runID, wf.Name, defJSON, opts.parentRunID, opts.parentTaskID); err != nil {
		return "", "", nil, fmt.Errorf("record workflow run start: %w", err)
	}

	return runID, hostWorkspace, orderedTasks, nil
}

func (e *Engine) executeRun(ctx context.Context, runID, hostWorkspace string, wf *workflow.Workflow, orderedTasks []workflow.Task, opts runOptions) error {
	fmt.Printf("[engine] Starting workflow %q (run_id=%s, workspace=%s)\n", wf.Name, runID, hostWorkspace)

	for _, task := range orderedTasks {
		if task.Flow != "" {
			fmt.Printf("[engine] Running subflow task %q (flow=%q, wait=%v)\n", task.ID, task.Flow, task.ShouldWait())
		} else if task.Image != "" {
			fmt.Printf("[engine] Running task %q (runtime=%s, image=%s)\n", task.ID, task.Runtime, task.Image)
		} else {
			fmt.Printf("[engine] Running task %q (runtime=%s)\n", task.ID, task.Runtime)
		}

		taskRunID, err := e.store.CreateTaskRunWithMeta(ctx, runID, task.ID, task.Runtime, task.Image)
		if err != nil {
			e.markRunFailed(runID)
			return fmt.Errorf("record task %q start: %w", task.ID, err)
		}

		execTask := task
		execTask.Env = env.MergeTaskEnv(opts.baseEnv, task.Env)
		execTask.Include = mergeIncludes(wf.Include, task.Include)
		if execTask.BaseDir == "" {
			if wf.BaseDir != "" {
				execTask.BaseDir = wf.BaseDir
			} else {
				execTask.BaseDir = "."
			}
		}

		var res *runner.Result
		var runErr error
		if task.Flow != "" {
			res, runErr = e.executeSubflowTask(ctx, runID, taskRunID, wf, execTask, opts)
		} else {
			res, runErr = e.runTaskWithLiveLogs(ctx, runID, hostWorkspace, taskRunID, execTask)
		}

		var exitCode int64 = -1
		var logs string
		if res != nil {
			exitCode = res.ExitCode
			logs = res.Logs
		}

		if runErr != nil || exitCode != 0 {
			if runErr != nil {
				if logs != "" {
					logs += "\n"
				}
				logs += fmt.Sprintf("[freud error] %v", runErr)
			}

			e.markTaskFinished(taskRunID, db.StatusFailed, exitCode, logs)
			e.markRunFailed(runID)

			fmt.Printf("[engine] Task %q FAILED (exit_code=%d). Aborting workflow.\n", task.ID, exitCode)
			if runErr != nil {
				return fmt.Errorf("task %q failed: %w", task.ID, runErr)
			}
			return fmt.Errorf("task %q exited with non-zero code %d", task.ID, exitCode)
		}

		if err := e.store.FinishTaskRun(ctx, taskRunID, db.StatusSuccess, exitCode, logs); err != nil {
			e.markRunFailed(runID)
			return fmt.Errorf("record task %q completion: %w", task.ID, err)
		}

		fmt.Printf("[engine] Task %q SUCCEEDED (exit_code=%d)\n", task.ID, exitCode)
	}

	if err := e.store.FinishRun(ctx, runID, db.StatusSuccess); err != nil {
		return fmt.Errorf("record workflow run completion: %w", err)
	}

	fmt.Printf("[engine] Workflow %q (run_id=%s) SUCCEEDED\n", wf.Name, runID)
	return nil
}

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
		parentRunID:  parentRunID,
		parentTaskID: task.ID,
		flowStack:    nextStack,
	}

	childRunID, childDone, err := e.startAsyncWithOptions(ctx, childWF, childOpts)
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
				fmt.Fprintf(os.Stderr, "[subflow] Async child run %s (%s) failed: %v\n", childRunID, childWF.Name, childErr)
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

func (e *Engine) runTaskWithLiveLogs(ctx context.Context, runID, hostWorkspace string, taskRunID int64, task workflow.Task) (*runner.Result, error) {
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
			snapshot := latest
			dirty = false
			mu.Unlock()

			dbCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = e.store.UpdateTaskRunLogs(dbCtx, taskRunID, snapshot)
			cancel()
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

func (e *Engine) markTaskFinished(taskRunID int64, status string, exitCode int64, logs string) {
	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := e.store.FinishTaskRun(dbCtx, taskRunID, status, exitCode, logs); err != nil {
		fmt.Fprintf(os.Stderr, "[engine] Warning: failed to persist task_run %d status: %v\n", taskRunID, err)
	}
}

func (e *Engine) markRunFailed(runID string) {
	dbCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := e.store.FinishRun(dbCtx, runID, db.StatusFailed); err != nil {
		fmt.Fprintf(os.Stderr, "[engine] Warning: failed to persist run %s FAILED status: %v\n", runID, err)
	}
}

type runDefinitionSnapshot struct {
	Name  string            `json:"name"`
	Cron  string            `json:"cron,omitempty"`
	Tasks []taskDefSnapshot `json:"tasks"`
}

type taskDefSnapshot struct {
	ID        string   `json:"id"`
	Runtime   string   `json:"runtime"`
	Image     string   `json:"image,omitempty"`
	Flow      string   `json:"flow,omitempty"`
	DependsOn []string `json:"depends_on,omitempty"`
}

func marshalWorkflowSnapshot(wf *workflow.Workflow) string {
	snap := runDefinitionSnapshot{
		Name:  wf.Name,
		Cron:  wf.Cron,
		Tasks: make([]taskDefSnapshot, 0, len(wf.Tasks)),
	}
	for _, t := range wf.Tasks {
		snap.Tasks = append(snap.Tasks, taskDefSnapshot{
			ID:        t.ID,
			Runtime:   t.Runtime,
			Image:     t.Image,
			Flow:      t.Flow,
			DependsOn: t.DependsOn,
		})
	}
	data, err := json.Marshal(snap)
	if err != nil {
		return ""
	}
	return string(data)
}

func mergeIncludes(workflowIncludes, taskIncludes []string) []string {
	if len(workflowIncludes) == 0 && len(taskIncludes) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(workflowIncludes)+len(taskIncludes))
	merged := make([]string, 0, len(workflowIncludes)+len(taskIncludes))
	for _, p := range workflowIncludes {
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			merged = append(merged, p)
		}
	}
	for _, p := range taskIncludes {
		if _, ok := seen[p]; !ok {
			seen[p] = struct{}{}
			merged = append(merged, p)
		}
	}
	return merged
}

func generateRunID() (string, error) {
	buf := make([]byte, 4)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102-150405"), hex.EncodeToString(buf)), nil
}
