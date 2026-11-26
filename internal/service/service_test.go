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

	// ListExecutions: status validation, search, and pagination.
	if _, err := ListExecutions(ctx, store, "BOGUS", "", 10, 0, 50, 200); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("expected ErrInvalidStatus, got %v", err)
	}
	execPage, err := ListExecutions(ctx, store, db.StatusSuccess, "", 1, 0, 50, 10)
	if err != nil || execPage.Total != 2 || len(execPage.Runs) != 1 || !execPage.HasMore {
		t.Fatalf("unexpected ListExecutions page 1: %+v err=%v", execPage, err)
	}
	execPage2, err := ListExecutions(ctx, store, db.StatusSuccess, "", 1, 1, 50, 10)
	if err != nil || execPage2.Total != 2 || len(execPage2.Runs) != 1 || execPage2.HasMore {
		t.Fatalf("unexpected ListExecutions page 2: %+v err=%v", execPage2, err)
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
	auditPage, err := ListAuditLogs(ctx, store, 0, 0, 50, 200, "FLOW_RUN", "sample")
	if err != nil || auditPage.Total != 1 || len(auditPage.Logs) != 1 || auditPage.Logs[0].Source != "api" || auditPage.HasMore {
		t.Fatalf("unexpected AuditLogsPage: %+v err=%v", auditPage, err)
	}

	// Includes listing and path-safe reading.
	incDir := filepath.Join(tmp, "includes")
	if err := os.MkdirAll(filepath.Join(incDir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(incDir, "sub", "helper.py"), []byte("def ping():\n    return 'ok'\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	incFiles, err := ListIncludes(incDir)
	if err != nil || len(incFiles) != 1 || incFiles[0].Path != "sub/helper.py" {
		t.Fatalf("unexpected ListIncludes: %+v err=%v", incFiles, err)
	}
	if _, err := ReadInclude(incDir, "../svc.db"); !errors.Is(err, ErrInvalidInclude) {
		t.Fatalf("expected ErrInvalidInclude on traversal, got %v", err)
	}
	if _, err := ReadInclude(incDir, "missing.py"); !errors.Is(err, ErrIncludeNotFound) {
		t.Fatalf("expected ErrIncludeNotFound, got %v", err)
	}
	incData, err := ReadInclude(incDir, "sub/helper.py")
	if err != nil || !errors.Is(err, nil) || incData.Path != "sub/helper.py" || len(incData.Content) == 0 {
		t.Fatalf("unexpected ReadInclude: %+v err=%v", incData, err)
	}
}
