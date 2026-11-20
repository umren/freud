package db

import (
	"context"
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
