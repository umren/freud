package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

type streamingMockRunner struct {
	startedCh chan struct{}
	releaseCh chan struct{}
}

func (s *streamingMockRunner) RunTask(ctx context.Context, runID, hostWorkspace string, task workflow.Task) (*runner.Result, error) {
	return s.RunTaskStream(ctx, runID, hostWorkspace, task, nil)
}

func (s *streamingMockRunner) RunTaskStream(_ context.Context, _ string, _ string, _ workflow.Task, onLog func(string)) (*runner.Result, error) {
	if onLog != nil {
		onLog("line 1 streaming\n")
	}
	close(s.startedCh)
	<-s.releaseCh
	if onLog != nil {
		onLog("line 1 streaming\nline 2 done\n")
	}
	return &runner.Result{ExitCode: 0, Logs: "line 1 streaming\nline 2 done\n"}, nil
}

func TestEngineStartAsyncAndLiveLogs(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := db.Open(filepath.Join(tmpDir, "orchestrator.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	sr := &streamingMockRunner{
		startedCh: make(chan struct{}),
		releaseCh: make(chan struct{}),
	}
	eng := New(store, sr, filepath.Join(tmpDir, "workspaces"))

	wf := &workflow.Workflow{
		Name: "async-live-wf",
		Tasks: []workflow.Task{
			{ID: "t1", Runtime: workflow.RuntimePython, Script: "print(1)"},
		},
	}

	runID, doneCh, err := eng.StartAsync(context.Background(), wf)
	if err != nil {
		t.Fatalf("StartAsync failed: %v", err)
	}

	// Run record must already exist synchronously before background goroutine completes.
	rec, err := store.GetRun(context.Background(), runID)
	if err != nil || rec.Status != db.StatusRunning {
		t.Fatalf("expected immediate RUNNING record, got %+v, err=%v", rec, err)
	}

	<-sr.startedCh

	// Wait briefly for the 250ms log flusher to persist partial output while task is still running.
	deadline := time.Now().Add(2 * time.Second)
	flushed := false
	for time.Now().Before(deadline) {
		tasks, err := store.ListTaskRuns(context.Background(), runID)
		if err == nil && len(tasks) == 1 && tasks[0].Logs.String == "line 1 streaming\n" {
			flushed = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	close(sr.releaseCh)

	if !flushed {
		t.Fatalf("expected live partial logs to flush while task was running")
	}

	if runErr := <-doneCh; runErr != nil {
		t.Fatalf("async run failed: %v", runErr)
	}

	finalRec, err := store.GetRun(context.Background(), runID)
	if err != nil || finalRec.Status != db.StatusSuccess {
		t.Fatalf("expected final SUCCESS status, got %+v, err=%v", finalRec, err)
	}
}

func TestEngineSubflowSuccessFailureAndCycles(t *testing.T) {
	tmpDir := t.TempDir()
	flowsDir := filepath.Join(tmpDir, "flows")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatalf("mkdir flows: %v", err)
	}

	childYAML := `name: child-flow
tasks:
  - id: child_task
    runtime: python
    script: print("child")
`
	parentYAML := `name: parent-flow
tasks:
  - id: call_child
    flow: child-flow
    env:
      SHARED: "overridden"
`
	if err := os.WriteFile(filepath.Join(flowsDir, "child.yaml"), []byte(childYAML), 0o644); err != nil {
		t.Fatalf("write child: %v", err)
	}
	if err := os.WriteFile(filepath.Join(flowsDir, "parent.yaml"), []byte(parentYAML), 0o644); err != nil {
		t.Fatalf("write parent: %v", err)
	}

	store, err := db.Open(filepath.Join(tmpDir, "orchestrator.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	mr := &mockRunner{}
	eng := New(store, mr, filepath.Join(tmpDir, "workspaces"))
	eng.SetFlowsDir(flowsDir)
	eng.SetEnv(map[string]string{"SHARED": "base", "GLOBAL": "yes"})

	parentWF, err := workflow.ParseFile(filepath.Join(flowsDir, "parent.yaml"))
	if err != nil {
		t.Fatalf("parse parent: %v", err)
	}

	parentRunID, err := eng.Run(context.Background(), parentWF)
	if err != nil {
		t.Fatalf("parent run failed: %v", err)
	}
	eng.Wait()

	if mr.capturedEnvs["child_task"]["SHARED"] != "overridden" || mr.capturedEnvs["child_task"]["GLOBAL"] != "yes" {
		t.Fatalf("expected env propagation to child task, got %+v", mr.capturedEnvs["child_task"])
	}

	children, err := store.ListChildRuns(context.Background(), parentRunID)
	if err != nil || len(children) != 1 {
		t.Fatalf("expected 1 child run, got %+v, err=%v", children, err)
	}
	if children[0].ParentRunID.String != parentRunID || children[0].ParentTaskID.String != "call_child" || children[0].Status != db.StatusSuccess {
		t.Fatalf("unexpected child run record: %+v", children[0])
	}

	// Cycle detection: cyc-a -> cyc-b -> cyc-a.
	cycA := "name: cyc-a\ntasks:\n  - id: s1\n    flow: cyc-b\n"
	cycB := "name: cyc-b\ntasks:\n  - id: s1\n    flow: cyc-a\n"
	if err := os.WriteFile(filepath.Join(flowsDir, "cyc-a.yaml"), []byte(cycA), 0o644); err != nil {
		t.Fatalf("write cyc-a: %v", err)
	}
	if err := os.WriteFile(filepath.Join(flowsDir, "cyc-b.yaml"), []byte(cycB), 0o644); err != nil {
		t.Fatalf("write cyc-b: %v", err)
	}
	cycWF, err := workflow.ParseFile(filepath.Join(flowsDir, "cyc-a.yaml"))
	if err != nil {
		t.Fatalf("parse cyc-a: %v", err)
	}
	if _, err := eng.Run(context.Background(), cycWF); err == nil || !strings.Contains(err.Error(), "subflow cycle detected") {
		t.Fatalf("expected subflow cycle error, got %v", err)
	}
}

type blockingKillRunner struct {
	startedCh chan struct{}
	executed  []string
}

func (r *blockingKillRunner) RunTask(ctx context.Context, _ string, _ string, task workflow.Task) (*runner.Result, error) {
	r.executed = append(r.executed, task.ID)
	if task.ID == "wait_step" {
		close(r.startedCh)
		<-ctx.Done()
		return &runner.Result{ExitCode: -1, Logs: "partial log before kill"}, ctx.Err()
	}
	return &runner.Result{ExitCode: 0, Logs: "should not run"}, nil
}

func TestEngineKillActiveRun(t *testing.T) {
	tmpDir := t.TempDir()
	store, err := db.Open(filepath.Join(tmpDir, "orchestrator.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	br := &blockingKillRunner{startedCh: make(chan struct{})}
	eng := New(store, br, filepath.Join(tmpDir, "workspaces"))

	wf := &workflow.Workflow{
		Name: "killable-flow",
		Tasks: []workflow.Task{
			{ID: "wait_step", Runtime: workflow.RuntimePython, Script: "import time; time.sleep(10)"},
			{ID: "second_step", Runtime: workflow.RuntimePython, DependsOn: []string{"wait_step"}, Script: "print(2)"},
		},
	}

	runID, doneCh, err := eng.StartAsync(context.Background(), wf)
	if err != nil {
		t.Fatalf("StartAsync failed: %v", err)
	}

	select {
	case <-br.startedCh:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for task to start")
	}

	if err := eng.Kill(runID); err != nil {
		t.Fatalf("Kill failed: %v", err)
	}

	select {
	case runErr := <-doneCh:
		if !errors.Is(runErr, ErrRunKilled) {
			t.Fatalf("expected ErrRunKilled, got %v", runErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for killed run to finish")
	}

	runRec, err := store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("GetRun failed: %v", err)
	}
	if runRec.Status != db.StatusKilled {
		t.Fatalf("expected run status %s, got %s", db.StatusKilled, runRec.Status)
	}

	tasks, err := store.ListTaskRuns(context.Background(), runID)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("expected 1 task_run (second_step skipped), got %d (err=%v)", len(tasks), err)
	}
	if tasks[0].Status != db.StatusKilled {
		t.Fatalf("expected task status %s, got %s", db.StatusKilled, tasks[0].Status)
	}
	if !strings.Contains(tasks[0].Logs.String, "partial log before kill") || !strings.Contains(tasks[0].Logs.String, "killed by user") {
		t.Fatalf("unexpected killed task logs: %q", tasks[0].Logs.String)
	}
	if len(br.executed) != 1 || br.executed[0] != "wait_step" {
		t.Fatalf("expected only wait_step to execute, got %v", br.executed)
	}

	if err := eng.Kill(runID); !errors.Is(err, ErrRunNotActive) {
		t.Fatalf("expected ErrRunNotActive after completion, got %v", err)
	}
}

func TestEngineSubflowWaitFalseFailedChildAndDepthLimit(t *testing.T) {
	tmpDir := t.TempDir()
	flowsDir := filepath.Join(tmpDir, "flows")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatalf("mkdir flows: %v", err)
	}

	writeFlow := func(file, body string) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(flowsDir, file), []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", file, err)
		}
	}

	// 1) Failing child subflow fails parent.
	writeFlow("fail-child.yaml", "name: fail-child\ntasks:\n  - id: boom\n    runtime: python\n    script: raise SystemExit(2)\n")
	writeFlow("fail-parent.yaml", "name: fail-parent\ntasks:\n  - id: call_fail\n    flow: fail-child\n")

	// 2) Async wait: false subflow completes via eng.Wait().
	writeFlow("async-child.yaml", "name: async-child\ntasks:\n  - id: bg\n    runtime: python\n    script: print('bg')\n")
	writeFlow("async-parent.yaml", "name: async-parent\ntasks:\n  - id: fire\n    flow: async-child\n    wait: false\n")

	// 3) Subflow depth limit (> maxSubflowDepth).
	for i := 0; i <= maxSubflowDepth; i++ {
		if i == maxSubflowDepth {
			writeFlow(fmt.Sprintf("d%d.yaml", i), fmt.Sprintf("name: d%d\ntasks:\n  - id: leaf\n    runtime: python\n    script: print(1)\n", i))
		} else {
			writeFlow(fmt.Sprintf("d%d.yaml", i), fmt.Sprintf("name: d%d\ntasks:\n  - id: next\n    flow: d%d\n", i, i+1))
		}
	}

	store, err := db.Open(filepath.Join(tmpDir, "orchestrator.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	mr := &mockRunner{failTask: "boom", exitCode: 2}
	eng := New(store, mr, filepath.Join(tmpDir, "workspaces"))
	eng.SetFlowsDir(flowsDir)
	eng.SetOutput(io.Discard, io.Discard)

	failParentWF, err := workflow.ParseFile(filepath.Join(flowsDir, "fail-parent.yaml"))
	if err != nil {
		t.Fatalf("parse fail-parent: %v", err)
	}
	failParentRunID, err := eng.Run(context.Background(), failParentWF)
	if err == nil {
		t.Fatalf("expected fail-parent to fail when child subflow fails")
	}
	rec, _ := store.GetRun(context.Background(), failParentRunID)
	if rec.Status != db.StatusFailed {
		t.Fatalf("expected FAILED status, got %s", rec.Status)
	}

	asyncParentWF, err := workflow.ParseFile(filepath.Join(flowsDir, "async-parent.yaml"))
	if err != nil {
		t.Fatalf("parse async-parent: %v", err)
	}
	asyncParentRunID, err := eng.Run(context.Background(), asyncParentWF)
	if err != nil {
		t.Fatalf("async-parent failed: %v", err)
	}
	eng.Wait()

	children, err := store.ListChildRuns(context.Background(), asyncParentRunID)
	if err != nil || len(children) != 1 || children[0].Status != db.StatusSuccess {
		t.Fatalf("expected 1 SUCCEEDED async child run, got %+v, err=%v", children, err)
	}

	d0WF, err := workflow.ParseFile(filepath.Join(flowsDir, "d0.yaml"))
	if err != nil {
		t.Fatalf("parse d0: %v", err)
	}
	if _, err := eng.Run(context.Background(), d0WF); err == nil || !strings.Contains(err.Error(), "subflow depth limit") {
		t.Fatalf("expected subflow depth limit error, got %v", err)
	}
}

func TestEnginePerRunEnvAndSubflowPropagation(t *testing.T) {
	tmpDir := t.TempDir()
	flowsDir := filepath.Join(tmpDir, "flows")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	childYAML := `name: child-env
tasks:
  - id: child_step
    runtime: python
    env:
      CHILD_EXP: "d=${TARGET_DATE}"
      TARGET_DATE: "child-default"
    script: print(1)
`
	if err := os.WriteFile(filepath.Join(flowsDir, "child.yaml"), []byte(childYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := db.Open(filepath.Join(tmpDir, "runenv.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	mr := &mockRunner{}
	eng := New(store, mr, filepath.Join(tmpDir, "workspaces"))
	eng.SetFlowsDir(flowsDir)
	eng.SetEnv(map[string]string{
		"BASE_KEY":    "from-dotenv",
		"TARGET_DATE": "dotenv-date",
	})

	parentWF := &workflow.Workflow{
		Name: "parent-env",
		Tasks: []workflow.Task{
			{
				ID:      "p_step",
				Runtime: workflow.RuntimePython,
				Env: map[string]string{
					"TARGET_DATE": "yaml-default",
					"DERIVED":     "prefix-${TARGET_DATE}",
				},
				Script: "print(1)",
			},
			{
				ID:        "sub_step",
				Runtime:   workflow.RuntimeSubflow,
				Flow:      "child-env",
				DependsOn: []string{"p_step"},
			},
		},
	}

	ctx := WithRunEnv(WithTriggeredBy(context.Background(), "tester"), map[string]string{
		"TARGET_DATE": "2026-11-20",
		"EXTRA":       "one-off",
	})
	if _, err := eng.Run(ctx, parentWF); err != nil {
		t.Fatalf("parentWF run failed: %v", err)
	}

	pEnv := mr.capturedEnvs["p_step"]
	if pEnv["BASE_KEY"] != "from-dotenv" || pEnv["TARGET_DATE"] != "2026-11-20" || pEnv["DERIVED"] != "prefix-2026-11-20" || pEnv["EXTRA"] != "one-off" {
		t.Fatalf("unexpected parent task env: %+v", pEnv)
	}

	cEnv := mr.capturedEnvs["child_step"]
	if cEnv["TARGET_DATE"] != "2026-11-20" || cEnv["CHILD_EXP"] != "d=2026-11-20" || cEnv["EXTRA"] != "one-off" {
		t.Fatalf("unexpected child subflow task env: %+v", cEnv)
	}
}
