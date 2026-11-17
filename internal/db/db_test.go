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
