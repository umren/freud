package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"freud/internal/db"
	"freud/internal/runner"
	"freud/internal/workflow"
)

type mockRunner struct {
	executed []string
	failTask string
	exitCode int64
}

func (m *mockRunner) RunTask(_ context.Context, _ string, hostWorkspace string, task workflow.Task) (*runner.Result, error) {
	m.executed = append(m.executed, task.ID)

	scriptsDir := filepath.Join(hostWorkspace, ".scripts")
	if _, err := os.Stat(scriptsDir); err != nil {
		return nil, err
	}

	if m.failTask == task.ID {
		return &runner.Result{ExitCode: m.exitCode, Logs: "task error output"}, nil
	}
	return &runner.Result{ExitCode: 0, Logs: "ok: " + task.ID}, nil
}

func TestEngineRunSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := db.Open(filepath.Join(tmpDir, "orchestrator.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	mr := &mockRunner{}
	eng := New(store, mr, filepath.Join(tmpDir, "workspaces"))

	wf := &workflow.Workflow{
		Name: "success-wf",
		Tasks: []workflow.Task{
			{ID: "t2", Runtime: workflow.RuntimeJS, DependsOn: []string{"t1"}, Script: "console.log(2)"},
			{ID: "t1", Runtime: workflow.RuntimePython, Script: "print(1)"},
		},
	}

	runID, err := eng.Run(context.Background(), wf)
	if err != nil {
		t.Fatalf("engine run failed: %v", err)
	}

	if len(mr.executed) != 2 || mr.executed[0] != "t1" || mr.executed[1] != "t2" {
		t.Fatalf("unexpected execution order: %v", mr.executed)
	}

	runRec, err := store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if runRec.Status != db.StatusSuccess {
		t.Fatalf("expected run status SUCCESS, got %s", runRec.Status)
	}
}

func TestEngineRunStopsOnFailure(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := db.Open(filepath.Join(tmpDir, "orchestrator.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	mr := &mockRunner{failTask: "t1", exitCode: 2}
	eng := New(store, mr, filepath.Join(tmpDir, "workspaces"))

	wf := &workflow.Workflow{
		Name: "fail-wf",
		Tasks: []workflow.Task{
			{ID: "t1", Runtime: workflow.RuntimePython, Script: "exit(2)"},
			{ID: "t2", Runtime: workflow.RuntimeJS, DependsOn: []string{"t1"}, Script: "console.log(2)"},
		},
	}

	runID, err := eng.Run(context.Background(), wf)
	if err == nil {
		t.Fatalf("expected error on failed task, got nil")
	}

	if len(mr.executed) != 1 || mr.executed[0] != "t1" {
		t.Fatalf("expected execution to stop after t1, executed: %v", mr.executed)
	}

	runRec, err := store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if runRec.Status != db.StatusFailed {
		t.Fatalf("expected run status FAILED, got %s", runRec.Status)
	}

	taskRecs, err := store.ListTaskRuns(context.Background(), runID)
	if err != nil {
		t.Fatalf("list task runs: %v", err)
	}
	if len(taskRecs) != 1 || taskRecs[0].Status != db.StatusFailed || taskRecs[0].ExitCode.Int64 != 2 {
		t.Fatalf("unexpected task run records: %+v", taskRecs)
	}
}
