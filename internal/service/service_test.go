package service

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"freud/internal/db"
)

func TestServiceFlowAndExecutionDTOs(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	validYAML := `name: sample-flow
cron: "0 * * * *"
tasks:
  - id: step1
    runtime: python
    dependencies: [pyyaml]
    script: print("ok")
`
	if err := os.WriteFile(filepath.Join(flowsDir, "sample.yaml"), []byte(validYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(flowsDir, "broken.yaml"), []byte("name: broken\ntasks: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := db.Open(filepath.Join(tmp, "svc.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.CreateRunFull(ctx, "run-p1", "sample-flow", "", "", "", "alice"); err != nil {
		t.Fatal(err)
	}
	trID, err := store.CreateTaskRunWithMeta(ctx, "run-p1", "step1", "python", "python:3.13-slim")
	if err != nil {
		t.Fatal(err)
	}
	_ = store.FinishTaskRun(ctx, trID, db.StatusSuccess, 0, "done")
	_ = store.FinishRun(ctx, "run-p1", db.StatusSuccess)
	if err := store.CreateRunFull(ctx, "run-c1", "child-flow", "", "run-p1", "step1", "alice"); err != nil {
		t.Fatal(err)
	}
	_ = store.FinishRun(ctx, "run-c1", db.StatusSuccess)
	_ = store.RecordAudit(ctx, "alice", "FLOW_RUN", "sample-flow", "api", "run_id=run-p1")

	// FindFlow: traversal blocked, invalid YAML mapped, lookup by workflow name allowed when requested.
	if _, err := FindFlow(flowsDir, "../secret.yaml", true); !errors.Is(err, ErrFlowNotFound) {
		t.Fatalf("expected ErrFlowNotFound on traversal, got %v", err)
	}
	if _, err := FindFlow(flowsDir, "broken.yaml", false); !errors.Is(err, ErrInvalidFlow) {
		t.Fatalf("expected ErrInvalidFlow on invalid YAML, got %v", err)
	}
	if _, err := FindFlow(flowsDir, "sample-flow", false); !errors.Is(err, ErrFlowNotFound) {
		t.Fatalf("expected ErrFlowNotFound when allowByName=false, got %v", err)
	}
	if entry, err := FindFlow(flowsDir, "sample-flow", true); err != nil || entry.FileName != "sample.yaml" {
		t.Fatalf("expected allowByName lookup to succeed, entry=%+v err=%v", entry, err)
	}

	// ListFlows & GetFlowDetail.
	flows, err := ListFlows(ctx, store, flowsDir)
	if err != nil || len(flows) != 2 {
		t.Fatalf("expected 2 flows, got %+v err=%v", flows, err)
	}
	detail, err := GetFlowDetail(ctx, store, flowsDir, "sample.yaml", false, 5)
	if err != nil || detail.Name != "sample-flow" || len(detail.RecentRuns) != 1 || detail.NextRun == "" {
		t.Fatalf("unexpected FlowDetail: %+v err=%v", detail, err)
	}

	// ListExecutions: status validation & clamping.
	if _, _, err := ListExecutions(ctx, store, "BOGUS", 10, 50, 200); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("expected ErrInvalidStatus, got %v", err)
	}
	stats, runs, err := ListExecutions(ctx, store, db.StatusSuccess, 999, 50, 10)
	if err != nil || stats.Total != 2 || len(runs) != 2 {
		t.Fatalf("unexpected ListExecutions: stats=%+v runs=%+v err=%v", stats, runs, err)
	}

	// GetExecutionDetail & missing run error.
	if _, err := GetExecutionDetail(ctx, store, "missing-run"); !errors.Is(err, ErrExecutionNotFound) {
		t.Fatalf("expected ErrExecutionNotFound, got %v", err)
	}
	execDetail, err := GetExecutionDetail(ctx, store, "run-p1")
	if err != nil || execDetail.Run.TriggeredBy != "alice" || len(execDetail.Tasks) != 1 || len(execDetail.ChildRuns) != 1 {
		t.Fatalf("unexpected ExecutionDetail: %+v err=%v", execDetail, err)
	}

	// ListAuditLogs.
	auditLogs, err := ListAuditLogs(ctx, store, 0, 50, 200, "FLOW_RUN")
	if err != nil || len(auditLogs) != 1 || auditLogs[0].Source != "api" {
		t.Fatalf("unexpected AuditLogs: %+v err=%v", auditLogs, err)
	}
}
