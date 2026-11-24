package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"freud/internal/db"
	"freud/internal/env"
	"freud/internal/runner"
	"freud/internal/term"
	"freud/internal/workflow"
)

const (
	defaultWorkspacesDir = "workspaces"
	scriptsDirName       = ".scripts"
	logFlushInterval     = 250 * time.Millisecond
	maxSubflowDepth      = 10
)

var (
	// ErrRunNotActive is returned when Kill is called for a run that is no longer running.
	ErrRunNotActive = errors.New("run is not active")
	// ErrRunKilled is returned when a workflow run is stopped via Kill.
	ErrRunKilled = errors.New("workflow run killed")
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
	out            io.Writer
	errOut         io.Writer
	wg             sync.WaitGroup

	runsMu     sync.Mutex
	activeRuns map[string]*activeRun
}

type activeRun struct {
	cancel   context.CancelFunc
	killed   bool
	children []string
}

type ctxKey string

const triggeredByCtxKey ctxKey = "freud_triggered_by"

// WithTriggeredBy attaches the initiating user/source name (e.g. "admin", "cron", "tui") to ctx.
func WithTriggeredBy(ctx context.Context, actor string) context.Context {
	if strings.TrimSpace(actor) == "" {
		return ctx
	}
	return context.WithValue(ctx, triggeredByCtxKey, strings.TrimSpace(actor))
}

// TriggeredByFromContext extracts the initiating actor name from ctx, if any.
func TriggeredByFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(triggeredByCtxKey).(string); ok {
		return v
	}
	return ""
}

type runOptions struct {
	baseEnv      map[string]string
	parentRunID  string
	parentTaskID string
	triggeredBy  string
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
		out:            os.Stdout,
		errOut:         os.Stderr,
		activeRuns:     make(map[string]*activeRun),
	}
}

// SetOutput overrides stdout and stderr writers used for engine status logging.
func (e *Engine) SetOutput(out, errOut io.Writer) {
	e.out = out
	e.errOut = errOut
}

func (e *Engine) stdout() io.Writer {
	if e.out != nil {
		return e.out
	}
	return os.Stdout
}

func (e *Engine) stderr() io.Writer {
	if e.errOut != nil {
		return e.errOut
	}
	return os.Stderr
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

// Kill cancels a currently active workflow run and any active child subflows it spawned.
func (e *Engine) Kill(runID string) error {
	e.runsMu.Lock()
	root, ok := e.activeRuns[runID]
	if !ok {
		e.runsMu.Unlock()
		return fmt.Errorf("%w: %s", ErrRunNotActive, runID)
	}

	var cancels []context.CancelFunc
	visited := make(map[string]bool)
	var collect func(id string, ar *activeRun)
	collect = func(id string, ar *activeRun) {
		if visited[id] || ar == nil {
			return
		}
		visited[id] = true
		ar.killed = true
		if ar.cancel != nil {
			cancels = append(cancels, ar.cancel)
		}
		for _, childID := range ar.children {
			if child, exists := e.activeRuns[childID]; exists {
				collect(childID, child)
			}
		}
	}
	collect(runID, root)
	e.runsMu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}
	return nil
}

func (e *Engine) registerActiveRun(runID, parentRunID string, cancel context.CancelFunc) {
	e.runsMu.Lock()
	ar := &activeRun{cancel: cancel}
	e.activeRuns[runID] = ar
	shouldCancelNow := false
	if parentRunID != "" {
		if parent, ok := e.activeRuns[parentRunID]; ok {
			parent.children = append(parent.children, runID)
			if parent.killed {
				ar.killed = true
				shouldCancelNow = true
			}
		}
	}
	e.runsMu.Unlock()

	if shouldCancelNow && cancel != nil {
		cancel()
	}
}

func (e *Engine) unregisterActiveRun(runID, parentRunID string) {
	e.runsMu.Lock()
	delete(e.activeRuns, runID)
	if parentRunID != "" {
		if parent, ok := e.activeRuns[parentRunID]; ok {
			filtered := parent.children[:0]
			for _, id := range parent.children {
				if id != runID {
					filtered = append(filtered, id)
				}
			}
			parent.children = filtered
		}
	}
	e.runsMu.Unlock()
}

func (e *Engine) isKilled(runID string) bool {
	e.runsMu.Lock()
	ar, ok := e.activeRuns[runID]
	killed := ok && ar.killed
	e.runsMu.Unlock()
	return killed
}

// Run executes all tasks in the workflow in topological order and persists run metadata to SQLite.
func (e *Engine) Run(ctx context.Context, wf *workflow.Workflow) (string, error) {
	opts := runOptions{
		baseEnv:     e.baseEnv,
		triggeredBy: TriggeredByFromContext(ctx),
		flowStack:   []string{wf.Name},
	}
	runID, hostWorkspace, orderedTasks, err := e.prepareRun(ctx, wf, opts)
	if err != nil {
		return "", err
	}
	runCtx, cancel := context.WithCancel(ctx)
	e.registerActiveRun(runID, opts.parentRunID, cancel)
	defer func() {
		cancel()
		e.unregisterActiveRun(runID, opts.parentRunID)
	}()

	if err := e.executeRun(runCtx, runID, hostWorkspace, wf, orderedTasks, opts); err != nil {
		return runID, err
	}
	return runID, nil
}

// StartAsync validates the workflow, creates the run record synchronously, and executes tasks asynchronously.
// The returned channel receives the final execution error (or nil) once the run completes.
func (e *Engine) StartAsync(ctx context.Context, wf *workflow.Workflow) (string, <-chan error, error) {
	opts := runOptions{
		baseEnv:     e.baseEnv,
		triggeredBy: TriggeredByFromContext(ctx),
		flowStack:   []string{wf.Name},
	}
	return e.startAsyncWithOptions(ctx, wf, opts)
}

func (e *Engine) startAsyncWithOptions(ctx context.Context, wf *workflow.Workflow, opts runOptions) (string, <-chan error, error) {
	runID, hostWorkspace, orderedTasks, err := e.prepareRun(ctx, wf, opts)
	if err != nil {
		return "", nil, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	e.registerActiveRun(runID, opts.parentRunID, cancel)

	doneCh := make(chan error, 1)
	go func() {
		defer cancel()
		defer e.unregisterActiveRun(runID, opts.parentRunID)
		doneCh <- e.executeRun(runCtx, runID, hostWorkspace, wf, orderedTasks, opts)
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
	if err := e.store.CreateRunFull(ctx, runID, wf.Name, defJSON, opts.parentRunID, opts.parentTaskID, opts.triggeredBy); err != nil {
		return "", "", nil, fmt.Errorf("record workflow run start: %w", err)
	}

	return runID, hostWorkspace, orderedTasks, nil
}

func (e *Engine) executeRun(ctx context.Context, runID, hostWorkspace string, wf *workflow.Workflow, orderedTasks []workflow.Task, opts runOptions) error {
	out := e.stdout()
	c := term.ColorEnabledWriter(out)
	fmt.Fprintf(out, "%s Starting workflow %s %s\n",
		term.Tag(c, "engine"),
		term.Key(c, fmt.Sprintf("%q", wf.Name)),
		term.Muted(c, fmt.Sprintf("(run_id=%s, workspace=%s)", runID, hostWorkspace)),
	)

	for _, task := range orderedTasks {
		if ctx.Err() != nil {
			return e.finishCanceledRun(runID, wf.Name, ctx.Err())
		}

		if task.Flow != "" {
			fmt.Fprintf(out, "%s Running subflow task %s %s\n",
				term.Tag(c, "engine"),
				term.Key(c, fmt.Sprintf("%q", task.ID)),
				term.Muted(c, fmt.Sprintf("(flow=%q, wait=%v)", task.Flow, task.ShouldWait())),
			)
		} else if task.Image != "" {
			fmt.Fprintf(out, "%s Running task %s %s\n",
				term.Tag(c, "engine"),
				term.Key(c, fmt.Sprintf("%q", task.ID)),
				term.Muted(c, fmt.Sprintf("(runtime=%s, image=%s)", task.Runtime, task.Image)),
			)
		} else {
			fmt.Fprintf(out, "%s Running task %s %s\n",
				term.Tag(c, "engine"),
				term.Key(c, fmt.Sprintf("%q", task.ID)),
				term.Muted(c, fmt.Sprintf("(runtime=%s)", task.Runtime)),
			)
		}

		taskRunID, err := e.store.CreateTaskRunWithMeta(ctx, runID, task.ID, task.Runtime, task.Image)
		if err != nil {
			if e.isKilled(runID) {
				return e.finishCanceledRun(runID, wf.Name, ctx.Err())
			}
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

		if e.isKilled(runID) {
			if logs != "" {
				logs += "\n"
			}
			logs += "[freud] Execution killed by user"
			e.markTaskFinished(taskRunID, db.StatusKilled, exitCode, logs)
			e.markRunStatus(runID, db.StatusKilled)
			fmt.Fprintf(out, "%s Task %s %s\n", term.Tag(c, "engine"), term.Key(c, fmt.Sprintf("%q", task.ID)), term.Status(c, "KILLED"))
			fmt.Fprintf(out, "%s Workflow %s %s %s\n",
				term.Tag(c, "engine"),
				term.Key(c, fmt.Sprintf("%q", wf.Name)),
				term.Muted(c, fmt.Sprintf("(run_id=%s)", runID)),
				term.Status(c, "KILLED"),
			)
			return fmt.Errorf("task %q: %w", task.ID, ErrRunKilled)
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

			fmt.Fprintf(out, "%s Task %s %s %s. Aborting workflow.\n",
				term.Tag(c, "engine"),
				term.Key(c, fmt.Sprintf("%q", task.ID)),
				term.Status(c, "FAILED"),
				term.Muted(c, fmt.Sprintf("(exit_code=%d)", exitCode)),
			)
			if runErr != nil {
				return fmt.Errorf("task %q failed: %w", task.ID, runErr)
			}
			return fmt.Errorf("task %q exited with non-zero code %d", task.ID, exitCode)
		}

		e.markTaskFinished(taskRunID, db.StatusSuccess, exitCode, logs)
		fmt.Fprintf(out, "%s Task %s %s %s\n",
			term.Tag(c, "engine"),
			term.Key(c, fmt.Sprintf("%q", task.ID)),
			term.Status(c, "SUCCEEDED"),
			term.Muted(c, fmt.Sprintf("(exit_code=%d)", exitCode)),
		)
	}

	if e.isKilled(runID) {
		return e.finishCanceledRun(runID, wf.Name, ctx.Err())
	}

	if err := e.store.FinishRun(ctx, runID, db.StatusSuccess); err != nil {
		if e.isKilled(runID) {
			return e.finishCanceledRun(runID, wf.Name, ctx.Err())
		}
		return fmt.Errorf("record workflow run completion: %w", err)
	}

	fmt.Fprintf(out, "%s Workflow %s %s %s\n",
		term.Tag(c, "engine"),
		term.Key(c, fmt.Sprintf("%q", wf.Name)),
		term.Muted(c, fmt.Sprintf("(run_id=%s)", runID)),
		term.Status(c, "SUCCEEDED"),
	)
	return nil
}

func (e *Engine) finishCanceledRun(runID, workflowName string, ctxErr error) error {
	if e.isKilled(runID) {
		out := e.stdout()
		c := term.ColorEnabledWriter(out)
		e.markRunStatus(runID, db.StatusKilled)
		fmt.Fprintf(out, "%s Workflow %s %s %s\n",
			term.Tag(c, "engine"),
			term.Key(c, fmt.Sprintf("%q", workflowName)),
			term.Muted(c, fmt.Sprintf("(run_id=%s)", runID)),
			term.Status(c, "KILLED"),
		)
		return ErrRunKilled
	}
	e.markRunFailed(runID)
	if ctxErr != nil {
		return ctxErr
	}
	return context.Canceled
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
				errOut := e.stderr()
				ec := term.ColorEnabledWriter(errOut)
				fmt.Fprintf(errOut, "%s Async child run %s (%s) %s: %v\n",
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
		errOut := e.stderr()
		ec := term.ColorEnabledWriter(errOut)
		fmt.Fprintf(errOut, "%s %s: failed to persist task_run %d status: %v\n",
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
		errOut := e.stderr()
		ec := term.ColorEnabledWriter(errOut)
		fmt.Fprintf(errOut, "%s %s: failed to persist run %s %s status: %v\n",
			term.Tag(ec, "engine"), term.Status(ec, "Warning"), runID, status, err)
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
