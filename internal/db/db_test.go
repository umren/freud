package db

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestStoreLifecycleAndPragmas(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	var journalMode string
	if err := store.db.QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("expected WAL journal_mode, got %q", journalMode)
	}

	var busyTimeout int
	if err := store.db.QueryRow("PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
		t.Fatalf("query busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("expected busy_timeout 5000, got %d", busyTimeout)
	}

	ctx := context.Background()
	runID := "run-123"
	if err := store.CreateRun(ctx, runID, "demo-wf"); err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	taskRunID, err := store.CreateTaskRun(ctx, runID, "extract")
	if err != nil {
		t.Fatalf("CreateTaskRun failed: %v", err)
	}

	if err := store.FinishTaskRun(ctx, taskRunID, StatusSuccess, 0, "hello logs"); err != nil {
		t.Fatalf("FinishTaskRun failed: %v", err)
	}

	if err := store.FinishRun(ctx, runID, StatusSuccess); err != nil {
		t.Fatalf("FinishRun failed: %v", err)
	}

	runRec, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun failed: %v", err)
	}
	if runRec.Status != StatusSuccess || !runRec.FinishedAt.Valid {
		t.Fatalf("unexpected run record: %+v", runRec)
	}

	taskRecs, err := store.ListTaskRuns(ctx, runID)
	if err != nil {
		t.Fatalf("ListTaskRuns failed: %v", err)
	}
	if len(taskRecs) != 1 {
		t.Fatalf("expected 1 task run, got %d", len(taskRecs))
	}
	if taskRecs[0].Logs.String != "hello logs" || taskRecs[0].ExitCode.Int64 != 0 {
		t.Fatalf("unexpected task run record: %+v", taskRecs[0])
	}
}

func TestStoreQueriesAndIncrementalLogs(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "queries.db"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.CreateRunWithDefinition(ctx, "r1", "wf-a", `{"name":"wf-a"}`); err != nil {
		t.Fatalf("CreateRunWithDefinition r1: %v", err)
	}
	tr1, err := store.CreateTaskRunWithMeta(ctx, "r1", "t1", "python", "python:3.13-slim")
	if err != nil {
		t.Fatalf("CreateTaskRunWithMeta: %v", err)
	}
	if err := store.UpdateTaskRunLogs(ctx, tr1, "partial line 1\n"); err != nil {
		t.Fatalf("UpdateTaskRunLogs: %v", err)
	}
	tasks, err := store.ListTaskRuns(ctx, "r1")
	if err != nil || len(tasks) != 1 || tasks[0].Logs.String != "partial line 1\n" {
		t.Fatalf("expected incremental log update, got %+v, err=%v", tasks, err)
	}
	if err := store.FinishTaskRun(ctx, tr1, StatusSuccess, 0, "partial line 1\ndone\n"); err != nil {
		t.Fatalf("FinishTaskRun: %v", err)
	}
	if err := store.FinishRun(ctx, "r1", StatusSuccess); err != nil {
		t.Fatalf("FinishRun r1: %v", err)
	}

	if err := store.CreateRun(ctx, "r2", "wf-a"); err != nil {
		t.Fatalf("CreateRun r2: %v", err)
	}
	if err := store.FinishRun(ctx, "r2", StatusFailed); err != nil {
		t.Fatalf("FinishRun r2: %v", err)
	}

	if err := store.CreateRun(ctx, "r3", "wf-b"); err != nil {
		t.Fatalf("CreateRun r3: %v", err)
	}

	stats, err := store.GetRunStats(ctx)
	if err != nil {
		t.Fatalf("GetRunStats: %v", err)
	}
	if stats.Total != 3 || stats.Success != 1 || stats.Failed != 1 || stats.Running != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	allRuns, err := store.ListRuns(ctx, 10, "")
	if err != nil || len(allRuns) != 3 || allRuns[0].ID != "r3" || allRuns[2].ID != "r1" {
		t.Fatalf("expected newest-first order [r3, r2, r1], got %+v, err=%v", allRuns, err)
	}

	failedRuns, err := store.ListRuns(ctx, 10, StatusFailed)
	if err != nil || len(failedRuns) != 1 || failedRuns[0].ID != "r2" {
		t.Fatalf("unexpected failed filter result: %+v, err=%v", failedRuns, err)
	}

	latest, err := store.GetLatestRunByWorkflow(ctx)
	if err != nil {
		t.Fatalf("GetLatestRunByWorkflow: %v", err)
	}
	if latest["wf-a"].ID != "r2" || latest["wf-b"].ID != "r3" {
		t.Fatalf("unexpected latest map: %+v", latest)
	}
}

func TestStoreParentChildRunsAndKilledStats(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "subflows.db"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.CreateRun(ctx, "parent-1", "main-flow"); err != nil {
		t.Fatalf("CreateRun parent-1: %v", err)
	}
	if err := store.CreateRunWithParent(ctx, "child-1", "sub-flow", "", "parent-1", "call_sub"); err != nil {
		t.Fatalf("CreateRunWithParent: %v", err)
	}
	if err := store.FinishRun(ctx, "child-1", StatusKilled); err != nil {
		t.Fatalf("FinishRun child-1: %v", err)
	}
	if err := store.FinishRun(ctx, "parent-1", StatusKilled); err != nil {
		t.Fatalf("FinishRun parent-1: %v", err)
	}

	children, err := store.ListChildRuns(ctx, "parent-1")
	if err != nil || len(children) != 1 {
		t.Fatalf("expected 1 child run, got %+v, err=%v", children, err)
	}
	if children[0].ID != "child-1" || children[0].ParentTaskID.String != "call_sub" {
		t.Fatalf("unexpected child record: %+v", children[0])
	}

	byWF, err := store.ListRunsByWorkflow(ctx, "sub-flow", 10)
	if err != nil || len(byWF) != 1 || byWF[0].ID != "child-1" {
		t.Fatalf("ListRunsByWorkflow mismatch: %+v, err=%v", byWF, err)
	}

	stats, err := store.GetRunStats(ctx)
	if err != nil {
		t.Fatalf("GetRunStats: %v", err)
	}
	if stats.Total != 2 || stats.Killed != 2 {
		t.Fatalf("expected Total=2 Killed=2, got %+v", stats)
	}
}

func TestDetectDialectAndRebind(t *testing.T) {
	if got := DetectDialect("orchestrator.db"); got != DialectSQLite {
		t.Fatalf("expected sqlite for filename, got %s", got)
	}
	if got := DetectDialect(":memory:"); got != DialectSQLite {
		t.Fatalf("expected sqlite for :memory:, got %s", got)
	}
	if got := DetectDialect("postgres://user:pass@localhost:5432/freud?sslmode=disable"); got != DialectPostgres {
		t.Fatalf("expected postgres for postgres://, got %s", got)
	}
	if got := DetectDialect("POSTGRESQL://localhost/freud"); got != DialectPostgres {
		t.Fatalf("expected postgres for POSTGRESQL://, got %s", got)
	}

	got := rebindPostgres("SELECT * FROM runs WHERE status = '?' AND workflow_name = ? LIMIT ?")
	want := "SELECT * FROM runs WHERE status = '?' AND workflow_name = $1 LIMIT $2"
	if got != want {
		t.Fatalf("rebindPostgres mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}

func TestPostgresStoreIntegration(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("FREUD_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("FREUD_TEST_POSTGRES_DSN not set")
	}

	store, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open postgres failed: %v", err)
	}
	defer store.Close()

	if store.Dialect() != DialectPostgres {
		t.Fatalf("expected DialectPostgres, got %s", store.Dialect())
	}

	ctx := context.Background()
	_, _ = store.db.ExecContext(ctx, "TRUNCATE TABLE task_runs, runs RESTART IDENTITY CASCADE")

	if err := store.CreateRunWithParent(ctx, "pg-parent", "pg-main", `{"name":"pg-main"}`, "", ""); err != nil {
		t.Fatalf("CreateRunWithParent: %v", err)
	}
	trID, err := store.CreateTaskRunWithMeta(ctx, "pg-parent", "step1", "python", "python:3.13-slim")
	if err != nil || trID <= 0 {
		t.Fatalf("CreateTaskRunWithMeta: id=%d err=%v", trID, err)
	}
	if err := store.UpdateTaskRunLogs(ctx, trID, "pg log line 1\n"); err != nil {
		t.Fatalf("UpdateTaskRunLogs: %v", err)
	}
	if err := store.FinishTaskRun(ctx, trID, StatusSuccess, 0, "pg log line 1\ndone\n"); err != nil {
		t.Fatalf("FinishTaskRun: %v", err)
	}

	if err := store.CreateRunWithParent(ctx, "pg-child", "pg-sub", "", "pg-parent", "step1"); err != nil {
		t.Fatalf("CreateRunWithParent child: %v", err)
	}
	if err := store.FinishRun(ctx, "pg-child", StatusSuccess); err != nil {
		t.Fatalf("FinishRun child: %v", err)
	}
	if err := store.FinishRun(ctx, "pg-parent", StatusSuccess); err != nil {
		t.Fatalf("FinishRun parent: %v", err)
	}

	rec, err := store.GetRun(ctx, "pg-parent")
	if err != nil || rec.Status != StatusSuccess {
		t.Fatalf("GetRun: %+v err=%v", rec, err)
	}
	children, err := store.ListChildRuns(ctx, "pg-parent")
	if err != nil || len(children) != 1 || children[0].ID != "pg-child" {
		t.Fatalf("ListChildRuns: %+v err=%v", children, err)
	}
	tasks, err := store.ListTaskRuns(ctx, "pg-parent")
	if err != nil || len(tasks) != 1 || tasks[0].Logs.String != "pg log line 1\ndone\n" {
		t.Fatalf("ListTaskRuns: %+v err=%v", tasks, err)
	}
	stats, err := store.GetRunStats(ctx)
	if err != nil || stats.Total != 2 || stats.Success != 2 {
		t.Fatalf("GetRunStats: %+v err=%v", stats, err)
	}
	latest, err := store.GetLatestRunByWorkflow(ctx)
	if err != nil || latest["pg-main"].ID != "pg-parent" {
		t.Fatalf("GetLatestRunByWorkflow: %+v err=%v", latest, err)
	}
}
