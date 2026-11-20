package engine

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"freud/internal/db"
	"freud/internal/env"
	"freud/internal/runner"
	"freud/internal/workflow"
)

const (
	defaultWorkspacesDir = "workspaces"
	scriptsDirName       = ".scripts"
)

// TaskRunner defines the execution backend for running a single workflow task.
type TaskRunner interface {
	RunTask(ctx context.Context, runID, hostWorkspace string, task workflow.Task) (*runner.Result, error)
}

// Engine orchestrates workflow execution, workspace management, and SQLite state tracking.
type Engine struct {
	store          *db.Store
	runner         TaskRunner
	workspacesBase string
	baseEnv        map[string]string
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

// Run executes all tasks in the workflow in topological order and persists run metadata to SQLite.
func (e *Engine) Run(ctx context.Context, wf *workflow.Workflow) (string, error) {
	orderedTasks, err := wf.TopologicalOrder()
	if err != nil {
		return "", fmt.Errorf("resolve execution order: %w", err)
	}

	runID, err := generateRunID()
	if err != nil {
		return "", fmt.Errorf("generate run id: %w", err)
	}

	hostWorkspace := filepath.Join(e.workspacesBase, runID)
	scriptsDir := filepath.Join(hostWorkspace, scriptsDirName)
	if err := os.MkdirAll(scriptsDir, 0o755); err != nil {
		return "", fmt.Errorf("create workspace directory %q: %w", scriptsDir, err)
	}

	if err := e.store.CreateRun(ctx, runID, wf.Name); err != nil {
		return "", fmt.Errorf("record workflow run start: %w", err)
	}

	fmt.Printf("[engine] Starting workflow %q (run_id=%s, workspace=%s)\n", wf.Name, runID, hostWorkspace)

	for _, task := range orderedTasks {
		if task.Image != "" {
			fmt.Printf("[engine] Running task %q (runtime=%s, image=%s)\n", task.ID, task.Runtime, task.Image)
		} else {
			fmt.Printf("[engine] Running task %q (runtime=%s)\n", task.ID, task.Runtime)
		}

		taskRunID, err := e.store.CreateTaskRun(ctx, runID, task.ID)
		if err != nil {
			e.markRunFailed(runID)
			return runID, fmt.Errorf("record task %q start: %w", task.ID, err)
		}

		execTask := task
		execTask.Env = env.MergeTaskEnv(e.baseEnv, task.Env)
		execTask.Include = mergeIncludes(wf.Include, task.Include)
		if execTask.BaseDir == "" {
			if wf.BaseDir != "" {
				execTask.BaseDir = wf.BaseDir
			} else {
				execTask.BaseDir = "."
			}
		}

		res, runErr := e.runner.RunTask(ctx, runID, hostWorkspace, execTask)

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
				logs += fmt.Sprintf("[orchestrator error] %v", runErr)
			}

			e.markTaskFinished(taskRunID, db.StatusFailed, exitCode, logs)
			e.markRunFailed(runID)

			fmt.Printf("[engine] Task %q FAILED (exit_code=%d). Aborting workflow.\n", task.ID, exitCode)
			if runErr != nil {
				return runID, fmt.Errorf("task %q failed: %w", task.ID, runErr)
			}
			return runID, fmt.Errorf("task %q exited with non-zero code %d", task.ID, exitCode)
		}

		if err := e.store.FinishTaskRun(ctx, taskRunID, db.StatusSuccess, exitCode, logs); err != nil {
			e.markRunFailed(runID)
			return runID, fmt.Errorf("record task %q completion: %w", task.ID, err)
		}

		fmt.Printf("[engine] Task %q SUCCEEDED (exit_code=%d)\n", task.ID, exitCode)
	}

	if err := e.store.FinishRun(ctx, runID, db.StatusSuccess); err != nil {
		return runID, fmt.Errorf("record workflow run completion: %w", err)
	}

	fmt.Printf("[engine] Workflow %q (run_id=%s) SUCCEEDED\n", wf.Name, runID)
	return runID, nil
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
