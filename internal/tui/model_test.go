package tui

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"

	"freud/internal/db"
	"freud/internal/workflow"
)

type mockTUIRunner struct {
	store     *db.Store
	startedWF string
	killedID  string
}

func (m *mockTUIRunner) StartAsync(ctx context.Context, wf *workflow.Workflow) (string, <-chan error, error) {
	m.startedWF = wf.Name
	runID := "tui-run-001"
	if err := m.store.CreateRunWithDefinition(ctx, runID, wf.Name, ""); err != nil {
		return "", nil, err
	}
	ch := make(chan error, 1)
	close(ch)
	return runID, ch, nil
}

func (m *mockTUIRunner) Kill(runID string) error {
	m.killedID = runID
	return m.store.FinishRun(context.Background(), runID, db.StatusKilled)
}

func TestTUIViewsFlowsFullCodeRunAndKill(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatalf("mkdir flows: %v", err)
	}

	var longScript strings.Builder
	for i := 1; i <= 25; i++ {
		longScript.WriteString("      echo line\n")
	}
	flowYAML := "name: demo-flow\ncron: \"*/10 * * * *\"\ntasks:\n  - id: long_step\n    image: alpine:3.20\n    script: |\n" + longScript.String()
	if err := os.WriteFile(filepath.Join(flowsDir, "demo.yaml"), []byte(flowYAML), 0o644); err != nil {
		t.Fatalf("write flow: %v", err)
	}

	store, err := db.Open(filepath.Join(tmp, "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	if err := store.CreateRun(context.Background(), "run-existing-1", "demo-flow"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	taskRunID, err := store.CreateTaskRunWithMeta(context.Background(), "run-existing-1", "long_step", "shell", "alpine:3.20")
	if err != nil {
		t.Fatalf("CreateTaskRunWithMeta: %v", err)
	}
	if err := store.UpdateTaskRunLogs(context.Background(), taskRunID, "hello from running container\n"); err != nil {
		t.Fatalf("UpdateTaskRunLogs: %v", err)
	}

	runner := &mockTUIRunner{store: store}
	m := NewModel(Config{
		Store:    store,
		FlowsDir: flowsDir,
		Runner:   runner,
	})

	// Initial refresh loads stats, flows, schedules, and existing run.
	msg := m.refreshCmd()()
	m.Update(msg)

	dashView := m.View()
	if !strings.Contains(dashView, "Total Runs") || !strings.Contains(dashView, "run-existing-1") || !strings.Contains(dashView, "Upcoming Cron Schedules") {
		t.Fatalf("dashboard view missing expected sections:\n%s", dashView)
	}

	// Switch to Flows tab -> truncated preview initially, full code when toggled with 'v'.
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'2'}})
	m.Update(m.refreshCmd()())
	flowsView := m.View()
	if !strings.Contains(flowsView, "demo-flow") || !strings.Contains(flowsView, "more lines hidden") {
		t.Fatalf("expected truncated YAML preview in Flows tab, got:\n%s", flowsView)
	}

	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'v'}})
	fullView := m.View()
	if !strings.Contains(fullView, "Full YAML: demo.yaml") {
		t.Fatalf("expected Full YAML header after pressing v, got:\n%s", fullView)
	}
	m.Update(tea.KeyMsg{Type: tea.KeyEsc})

	// Run selected flow with 'r' -> switches to Execution Detail for tui-run-001.
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'r'}})
	if cmd == nil {
		t.Fatalf("expected StartAsync cmd on 'r'")
	}
	m.Update(cmd())
	if runner.startedWF != "demo-flow" || m.activeTab != tabExecutions || m.selectedRunID != "tui-run-001" {
		t.Fatalf("unexpected state after run: wf=%q tab=%d selectedRunID=%q", runner.startedWF, m.activeTab, m.selectedRunID)
	}

	// Open run-existing-1 to check logs and kill it with 'k'.
	m.openExecutionDetail("run-existing-1")
	m.Update(m.refreshCmd()())
	detailView := m.View()
	if !strings.Contains(detailView, "hello from running container") {
		t.Fatalf("expected task log output in execution detail, got:\n%s", detailView)
	}

	_, killCmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'k'}})
	if killCmd == nil {
		t.Fatalf("expected Kill cmd on 'k'")
	}
	m.Update(killCmd())
	m.Update(m.refreshCmd()())
	if runner.killedID != "run-existing-1" {
		t.Fatalf("expected killedID=run-existing-1, got %q", runner.killedID)
	}
	if m.snap.SelectedRun == nil || m.snap.SelectedRun.Status != db.StatusKilled {
		t.Fatalf("expected KILLED run in snapshot, got %+v", m.snap.SelectedRun)
	}

	m.wg.Wait()
}

func TestTUIDashboardExecutionsFilterMultiTaskAndSubflows(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatalf("mkdir flows: %v", err)
	}

	store, err := db.Open(filepath.Join(tmp, "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.CreateRunWithDefinition(ctx, "parent-run", "pipeline", ""); err != nil {
		t.Fatalf("CreateRun parent: %v", err)
	}
	tr1, err := store.CreateTaskRunWithMeta(ctx, "parent-run", "step_a", "python", "")
	if err != nil {
		t.Fatalf("CreateTaskRun step_a: %v", err)
	}
	if err := store.FinishTaskRun(ctx, tr1, db.StatusSuccess, 0, "logs from step_a\n"); err != nil {
		t.Fatalf("FinishTaskRun step_a: %v", err)
	}
	tr2, err := store.CreateTaskRunWithMeta(ctx, "parent-run", "step_b", "js", "")
	if err != nil {
		t.Fatalf("CreateTaskRun step_b: %v", err)
	}
	if err := store.FinishTaskRun(ctx, tr2, db.StatusFailed, 1, "logs from step_b\n"); err != nil {
		t.Fatalf("FinishTaskRun step_b: %v", err)
	}
	if err := store.FinishRun(ctx, "parent-run", db.StatusFailed); err != nil {
		t.Fatalf("FinishRun parent: %v", err)
	}

	if err := store.CreateRunWithParent(ctx, "child-run", "child-wf", "", "parent-run", "step_a"); err != nil {
		t.Fatalf("CreateRunWithParent: %v", err)
	}
	if err := store.FinishRun(ctx, "child-run", db.StatusSuccess); err != nil {
		t.Fatalf("FinishRun child: %v", err)
	}

	m := NewModel(Config{
		Store:    store,
		FlowsDir: flowsDir,
		Runner:   &mockTUIRunner{store: store},
	})

	// Resize to narrow and wide terminals cleanly.
	m.Update(tea.WindowSizeMsg{Width: 76, Height: 24})
	m.Update(m.refreshCmd()())
	if view := m.View(); !strings.Contains(view, "Upcoming Cron Schedules") {
		t.Fatalf("expected narrow dashboard render")
	}
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 36})

	// Dashboard navigation down -> up -> enter opens selected run (child-run is newest).
	m.Update(tea.KeyMsg{Type: tea.KeyDown})
	m.Update(tea.KeyMsg{Type: tea.KeyUp})
	m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	m.Update(m.refreshCmd()())
	if m.activeTab != tabExecutions || m.selectedRunID != "child-run" {
		t.Fatalf("expected child-run detail opened from dashboard, tab=%d id=%q", m.activeTab, m.selectedRunID)
	}

	// Jump from child-run to parent-run with 'p'.
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'p'}})
	m.Update(m.refreshCmd()())
	if m.selectedRunID != "parent-run" {
		t.Fatalf("expected jump to parent-run, got %q", m.selectedRunID)
	}

	// Switch between tasks (right -> left) and toggle auto-follow + top/bottom.
	if !strings.Contains(m.View(), "logs from step_a") {
		t.Fatalf("expected step_a logs first")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyRight})
	if !strings.Contains(m.View(), "logs from step_b") {
		t.Fatalf("expected step_b logs after right key, got:\n%s", m.View())
	}
	m.Update(tea.KeyMsg{Type: tea.KeyLeft})
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'g'}})
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'a'}})
	if !m.autoFollow {
		t.Fatalf("expected autoFollow toggled back on")
	}

	// Jump back to child-run using 'c', then Esc back to Executions list.
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'c'}})
	m.Update(m.refreshCmd()())
	if m.selectedRunID != "child-run" {
		t.Fatalf("expected jump to child-run via 'c', got %q", m.selectedRunID)
	}
	m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	m.Update(m.refreshCmd()())
	if m.selectedRunID != "" {
		t.Fatalf("expected back to Executions list")
	}

	// Filter cycling: ALL -> RUNNING -> SUCCESS -> FAILED -> KILLED.
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}}) // RUNNING (none)
	m.Update(m.refreshCmd()())
	if !strings.Contains(m.View(), "No executions match filter RUNNING") {
		t.Fatalf("expected empty RUNNING filter view, got:\n%s", m.View())
	}
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}}) // SUCCESS (child-run)
	m.Update(m.refreshCmd()())
	if !strings.Contains(m.View(), "child-run") || strings.Contains(m.View(), "parent-run") {
		t.Fatalf("expected only child-run under SUCCESS filter, got:\n%s", m.View())
	}

	// Switch to Audit tab ('4') and verify audit logs & filter cycling.
	if err := store.RecordAudit(ctx, "admin", "LOGIN", "", "web", "signed in"); err != nil {
		t.Fatalf("RecordAudit LOGIN: %v", err)
	}
	if err := store.RecordAudit(ctx, "local", "FLOW_RUN", "sample", "tui", "started"); err != nil {
		t.Fatalf("RecordAudit FLOW_RUN: %v", err)
	}
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'4'}})
	m.Update(m.refreshCmd()())
	auditView := m.View()
	if !strings.Contains(auditView, "User Activity & Audit Log") || !strings.Contains(auditView, "FLOW_RUN") || !strings.Contains(auditView, "LOGIN") {
		t.Fatalf("expected Audit tab view with recorded actions, got:\n%s", auditView)
	}
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'f'}}) // LOGIN filter
	m.Update(m.refreshCmd()())
	filteredAudit := m.View()
	if !strings.Contains(filteredAudit, "Filter: LOGIN") || !strings.Contains(filteredAudit, "admin") || strings.Contains(filteredAudit, "FLOW_RUN") {
		t.Fatalf("expected LOGIN-filtered audit view, got:\n%s", filteredAudit)
	}
}
