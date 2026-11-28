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

	outMu      sync.Mutex
	runsMu     sync.Mutex
	activeRuns map[string]*activeRun
}

type activeRun struct {
	cancel   context.CancelFunc
	killed   bool
	children []string
}

type ctxKey string

const (
	triggeredByCtxKey ctxKey = "freud_triggered_by"
	runEnvCtxKey      ctxKey = "freud_run_env"
)

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

// WithRunEnv attaches per-run environment variable overrides to ctx.
func WithRunEnv(ctx context.Context, runEnv map[string]string) context.Context {
	if len(runEnv) == 0 {
		return ctx
	}
	return context.WithValue(ctx, runEnvCtxKey, cloneEnvMap(runEnv))
}

// RunEnvFromContext extracts a defensive copy of per-run environment overrides from ctx, if any.
func RunEnvFromContext(ctx context.Context) map[string]string {
	if ctx == nil {
		return nil
	}
	raw, ok := ctx.Value(runEnvCtxKey).(map[string]string)
	if !ok {
		return nil
	}
	return cloneEnvMap(raw)
}

func cloneEnvMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(src))
	for k, v := range src {
		cloned[k] = v
	}
	return cloned
}

type runOptions struct {
	baseEnv      map[string]string
	runEnv       map[string]string
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
	e.outMu.Lock()
	defer e.outMu.Unlock()
	e.out = out
	e.errOut = errOut
}

func (e *Engine) stdoutUnlocked() io.Writer {
	if e.out != nil {
		return e.out
	}
	return os.Stdout
}

func (e *Engine) stderrUnlocked() io.Writer {
	if e.errOut != nil {
		return e.errOut
	}
	return os.Stderr
}

func (e *Engine) logOut(format string, args ...any) {
	e.outMu.Lock()
	defer e.outMu.Unlock()
	fmt.Fprintf(e.stdoutUnlocked(), format, args...)
}

func (e *Engine) logErr(format string, args ...any) {
	e.outMu.Lock()
	defer e.outMu.Unlock()
	fmt.Fprintf(e.stderrUnlocked(), format, args...)
}

func (e *Engine) outColor() bool {
	e.outMu.Lock()
	w := e.stdoutUnlocked()
	e.outMu.Unlock()
	return term.ColorEnabledWriter(w)
}

func (e *Engine) errColor() bool {
	e.outMu.Lock()
	w := e.stderrUnlocked()
	e.outMu.Unlock()
	return term.ColorEnabledWriter(w)
}

// SetEnv sets base environment variables (such as those loaded from .env) injected into every task.
func (e *Engine) SetEnv(baseEnv map[string]string) {
	e.baseEnv = baseEnv
}

// EnvKeys returns the sorted names of base environment variables configured for tasks (without values).
func (e *Engine) EnvKeys() []string {
	return env.SortedKeys(e.baseEnv)
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
	opts := e.newRunOptions(ctx, wf)
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
	return e.startAsyncWithOptions(ctx, wf, e.newRunOptions(ctx, wf))
}

func (e *Engine) newRunOptions(ctx context.Context, wf *workflow.Workflow) runOptions {
	return runOptions{
		baseEnv:     e.baseEnv,
		runEnv:      RunEnvFromContext(ctx),
		triggeredBy: TriggeredByFromContext(ctx),
		flowStack:   []string{wf.Name},
	}
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

func (e *Engine) finishCanceledRun(runID, workflowName string, ctxErr error) error {
	if e.isKilled(runID) {
		c := e.outColor()
		e.markRunStatus(runID, db.StatusKilled)
		e.logOut("%s Workflow %s %s %s\n",
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
