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
	executed         []string
	capturedEnvs     map[string]map[string]string
	capturedIncludes map[string][]string
	capturedBaseDirs map[string]string
	failTask         string
	exitCode         int64
}

func (m *mockRunner) RunTask(_ context.Context, _ string, hostWorkspace string, task workflow.Task) (*runner.Result, error) {
	m.executed = append(m.executed, task.ID)
	if m.capturedEnvs == nil {
		m.capturedEnvs = make(map[string]map[string]string)
	}
	if m.capturedIncludes == nil {
		m.capturedIncludes = make(map[string][]string)
	}
	if m.capturedBaseDirs == nil {
		m.capturedBaseDirs = make(map[string]string)
	}
	m.capturedEnvs[task.ID] = task.Env
	m.capturedIncludes[task.ID] = task.Include
	m.capturedBaseDirs[task.ID] = task.BaseDir

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

func TestEngineInjectsBaseEnv(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := db.Open(filepath.Join(tmpDir, "orchestrator.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	mr := &mockRunner{}
	eng := New(store, mr, filepath.Join(tmpDir, "workspaces"))
	eng.SetEnv(map[string]string{
		"SHARED_SECRET": "top-secret",
		"ENV_NAME":      "dev",
	})

	wf := &workflow.Workflow{
		Name: "env-wf",
		Tasks: []workflow.Task{
			{
				ID:      "t1",
				Runtime: workflow.RuntimePython,
				Env: map[string]string{
					"ENV_NAME": "prod",
					"ALIAS":    "${SHARED_SECRET}",
				},
				Script: "print(1)",
			},
			{
				ID:        "t2",
				Runtime:   workflow.RuntimeJS,
				DependsOn: []string{"t1"},
				Script:    "console.log(2)",
			},
		},
	}

	if _, err := eng.Run(context.Background(), wf); err != nil {
		t.Fatalf("engine run failed: %v", err)
	}

	t1Env := mr.capturedEnvs["t1"]
	if t1Env["SHARED_SECRET"] != "top-secret" || t1Env["ENV_NAME"] != "prod" || t1Env["ALIAS"] != "top-secret" {
		t.Fatalf("unexpected t1 env: %+v", t1Env)
	}

	t2Env := mr.capturedEnvs["t2"]
	if t2Env["SHARED_SECRET"] != "top-secret" || t2Env["ENV_NAME"] != "dev" {
		t.Fatalf("unexpected t2 env: %+v", t2Env)
	}
}

func TestEngineMergesIncludesAndBaseDir(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := db.Open(filepath.Join(tmpDir, "orchestrator.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	mr := &mockRunner{}
	eng := New(store, mr, filepath.Join(tmpDir, "workspaces"))

	wf := &workflow.Workflow{
		Name:    "inc-wf",
		BaseDir: "/tmp/wfdir",
		Include: []string{"includes/common.py"},
		Tasks: []workflow.Task{
			{
				ID:      "t1",
				Runtime: workflow.RuntimePython,
				Include: []string{"includes/common.py", "includes/extra.py"},
				Script:  "print(1)",
			},
		},
	}

	if _, err := eng.Run(context.Background(), wf); err != nil {
		t.Fatalf("engine run failed: %v", err)
	}

	if mr.capturedBaseDirs["t1"] != "/tmp/wfdir" {
		t.Fatalf("expected BaseDir /tmp/wfdir, got %q", mr.capturedBaseDirs["t1"])
	}
	gotInc := mr.capturedIncludes["t1"]
	if len(gotInc) != 2 || gotInc[0] != "includes/common.py" || gotInc[1] != "includes/extra.py" {
		t.Fatalf("unexpected merged includes: %v", gotInc)
	}
}
