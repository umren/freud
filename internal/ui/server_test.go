package ui

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"freud/internal/db"
	"freud/internal/workflow"
)

func TestUIServerRoutesAndSecurity(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	includesDir := filepath.Join(tmp, "includes")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatalf("mkdir flows: %v", err)
	}
	if err := os.MkdirAll(includesDir, 0o755); err != nil {
		t.Fatalf("mkdir includes: %v", err)
	}

	flowYAML := `name: demo-flow
cron: "*/15 * * * *"
tasks:
  - id: step1
    runtime: python
    script: print("step1")
  - id: step2
    runtime: js
    depends_on: [step1]
    script: console.log("step2")
`
	if err := os.WriteFile(filepath.Join(flowsDir, "demo.yaml"), []byte(flowYAML), 0o644); err != nil {
		t.Fatalf("write flow: %v", err)
	}
	if err := os.WriteFile(filepath.Join(includesDir, "helper.py"), []byte("def hello():\n    return 'world'\n"), 0o644); err != nil {
		t.Fatalf("write include: %v", err)
	}
	if err := os.WriteFile(filepath.Join(tmp, "secret.txt"), []byte("TOP_SECRET"), 0o600); err != nil {
		t.Fatalf("write secret: %v", err)
	}

	store, err := db.Open(filepath.Join(tmp, "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	defJSON := `{"name":"demo-flow","cron":"*/15 * * * *","tasks":[{"id":"step1","runtime":"python"},{"id":"step2","runtime":"js","depends_on":["step1"]}]}`
	if err := store.CreateRunWithDefinition(ctx, "run-001", "demo-flow", defJSON); err != nil {
		t.Fatalf("CreateRunWithDefinition: %v", err)
	}
	tr1, err := store.CreateTaskRunWithMeta(ctx, "run-001", "step1", "python", "python:3.13-slim")
	if err != nil {
		t.Fatalf("CreateTaskRunWithMeta: %v", err)
	}
	if err := store.FinishTaskRun(ctx, tr1, db.StatusSuccess, 0, "step1 ok"); err != nil {
		t.Fatalf("FinishTaskRun: %v", err)
	}
	if err := store.FinishRun(ctx, "run-001", db.StatusSuccess); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}

	srv, err := NewServer(store, Config{
		Addr:        ":4321",
		FlowsDir:    flowsDir,
		IncludesDir: includesDir,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	h := srv.Handler()

	routes := []struct {
		path    string
		contain string
	}{
		{"/", "Next Scheduled Runs"},
		{"/partials/dashboard", "demo-flow"},
		{"/flows", "demo.yaml"},
		{"/flows/demo.yaml", `<span class="tok-type">print</span>`},
		{"/executions", "run-001"},
		{"/partials/executions?status=SUCCESS", "run-001"},
		{"/executions/run-001", "step1 ok"},
		{"/partials/executions/run-001", "Task Execution Timeline"},
		{"/includes", "helper.py"},
		{"/includes?file=helper.py", `<span class="tok-kw">def</span>`},
		{"/static/htmx.min.js", "htmx"},
		{"/static/alpine.min.js", "Alpine"},
		{"/static/app.css", "--accent"},
	}

	for _, rt := range routes {
		req := httptest.NewRequest(http.MethodGet, rt.path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("GET %s returned %d: %s", rt.path, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), rt.contain) {
			t.Fatalf("GET %s body missing %q", rt.path, rt.contain)
		}
	}

	// Verify path traversal on /includes does not expose files outside includes/.
	req := httptest.NewRequest(http.MethodGet, "/includes?file=../secret.txt", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("GET traversal returned %d", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "TOP_SECRET") || !strings.Contains(body, "invalid file path") {
		t.Fatalf("expected traversal rejection without secret leak, got:\n%s", body)
	}
}

func TestBuildDAGLayout(t *testing.T) {
	dag := BuildDAGLayout([]DAGTaskInput{
		{ID: "a", Runtime: "python", Status: "SUCCESS"},
		{ID: "b", Runtime: "js", DependsOn: []string{"a"}, Status: "FAILED"},
		{ID: "c", Runtime: "shell", DependsOn: []string{"a"}, Status: "PENDING"},
		{ID: "d", Runtime: "python", DependsOn: []string{"b", "c"}, Status: "PENDING"},
	})
	if len(dag.Nodes) != 4 {
		t.Fatalf("expected 4 nodes, got %d", len(dag.Nodes))
	}
	if len(dag.Edges) != 4 {
		t.Fatalf("expected 4 edges, got %d", len(dag.Edges))
	}
	if dag.Width <= 0 || dag.Height <= 0 {
		t.Fatalf("invalid dimensions %dx%d", dag.Width, dag.Height)
	}
}

type mockAsyncRunner struct {
	store *db.Store
}

func (m *mockAsyncRunner) StartAsync(ctx context.Context, wf *workflow.Workflow) (string, <-chan error, error) {
	runID := "ui-run-001"
	if err := m.store.CreateRunWithDefinition(ctx, runID, wf.Name, ""); err != nil {
		return "", nil, err
	}
	ch := make(chan error, 1)
	ch <- nil
	close(ch)
	return runID, ch, nil
}

func TestUIRunFlowNowAndRedirect(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatalf("mkdir flows: %v", err)
	}
	flowYAML := "name: manual-flow\ntasks:\n  - id: step1\n    runtime: python\n    script: print(1)\n"
	if err := os.WriteFile(filepath.Join(flowsDir, "manual.yaml"), []byte(flowYAML), 0o644); err != nil {
		t.Fatalf("write flow: %v", err)
	}

	store, err := db.Open(filepath.Join(tmp, "test.db"))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer store.Close()

	srv, err := NewServer(store, Config{
		FlowsDir: flowsDir,
		Runner:   &mockAsyncRunner{store: store},
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	h := srv.Handler()

	// Flow detail page must render the Run Now button.
	detailReq := httptest.NewRequest(http.MethodGet, "/flows/manual.yaml", nil)
	detailRec := httptest.NewRecorder()
	h.ServeHTTP(detailRec, detailReq)
	if detailRec.Code != http.StatusOK || !strings.Contains(detailRec.Body.String(), `/flows/manual.yaml/run`) {
		t.Fatalf("expected Run Now form on flow detail, code=%d", detailRec.Code)
	}

	// POST /flows/manual.yaml/run must redirect 303 to /executions/<runID>.
	runReq := httptest.NewRequest(http.MethodPost, "/flows/manual.yaml/run", nil)
	runRec := httptest.NewRecorder()
	h.ServeHTTP(runRec, runReq)
	if runRec.Code != http.StatusSeeOther {
		t.Fatalf("expected 303 See Other, got %d: %s", runRec.Code, runRec.Body.String())
	}
	loc := runRec.Header().Get("Location")
	if loc != "/executions/ui-run-001" {
		t.Fatalf("expected redirect to /executions/ui-run-001, got %q", loc)
	}

	srv.Wait()

	// Immediately following the redirect must return 200 OK.
	execReq := httptest.NewRequest(http.MethodGet, loc, nil)
	execRec := httptest.NewRecorder()
	h.ServeHTTP(execRec, execReq)
	if execRec.Code != http.StatusOK || !strings.Contains(execRec.Body.String(), "manual-flow") {
		t.Fatalf("expected 200 OK on %s, got %d", loc, execRec.Code)
	}
}
