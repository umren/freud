package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"freud/internal/db"
	"freud/internal/engine"
	"freud/internal/workflow"
)

type mockMCPRunner struct {
	store    *db.Store
	killedID string
}

func (m *mockMCPRunner) StartAsync(ctx context.Context, wf *workflow.Workflow) (string, <-chan error, error) {
	actor := engine.TriggeredByFromContext(ctx)
	runID := "mcp-run-1"
	if err := m.store.CreateRunFull(ctx, runID, wf.Name, "", "", "", actor); err != nil {
		return "", nil, err
	}
	trID, err := m.store.CreateTaskRunWithMeta(ctx, runID, "step1", "python", "python:3.13-slim")
	if err != nil {
		return "", nil, err
	}
	_ = m.store.FinishTaskRun(ctx, trID, db.StatusSuccess, 0, "hello from mcp")
	_ = m.store.FinishRun(ctx, runID, db.StatusSuccess)

	ch := make(chan error, 1)
	close(ch)
	return runID, ch, nil
}

func (m *mockMCPRunner) Kill(runID string) error {
	m.killedID = runID
	return m.store.FinishRun(context.Background(), runID, db.StatusKilled)
}

func setupMCPTestServer(t *testing.T) (*Server, *db.Store, *mockMCPRunner) {
	t.Helper()
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	flowYAML := `name: mcp-demo
cron: "0 * * * *"
tasks:
  - id: step1
    runtime: python
    script: print("hi")
`
	if err := os.WriteFile(filepath.Join(flowsDir, "mcp.yaml"), []byte(flowYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := db.Open(filepath.Join(tmp, "mcp.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	runner := &mockMCPRunner{store: store}
	srv := NewServer(store, Config{
		FlowsDir: flowsDir,
		Runner:   runner,
	})
	return srv, store, runner
}

func TestMCPStdioLifecycleAndTools(t *testing.T) {
	srv, store, _ := setupMCPTestServer(t)

	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26"}}`,
		`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"list_flows","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"run_flow","arguments":{"file":"mcp.yaml","wait":true}}}`,
		`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"list_audit_logs","arguments":{"action":"FLOW_RUN"}}}`,
		`{"jsonrpc":"2.0","id":6,"method":"resources/read","params":{"uri":"freud://flows/mcp.yaml"}}`,
	}, "\n") + "\n"

	var out bytes.Buffer
	ctx := WithActor(context.Background(), "agent-alice", "token=dev")
	if err := srv.ServeStdio(ctx, strings.NewReader(input), &out); err != nil {
		t.Fatalf("ServeStdio failed: %v", err)
	}
	srv.Wait()

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	// Notification produces no response line, so 6 responses expected.
	if len(lines) != 6 {
		t.Fatalf("expected 6 JSON-RPC responses, got %d:\n%s", len(lines), out.String())
	}

	var initResp jsonRPCResponse
	if err := json.Unmarshal([]byte(lines[0]), &initResp); err != nil || initResp.Error != nil {
		t.Fatalf("unexpected init response: %s (%v)", lines[0], err)
	}
	if !strings.Contains(lines[1], "run_flow") || !strings.Contains(lines[1], "kill_execution") {
		t.Fatalf("tools/list missing expected tools: %s", lines[1])
	}
	if !strings.Contains(lines[2], "mcp-demo") {
		t.Fatalf("list_flows missing mcp-demo: %s", lines[2])
	}
	if !strings.Contains(lines[3], "hello from mcp") || !strings.Contains(lines[3], "agent-alice") {
		t.Fatalf("run_flow wait=true missing logs or actor: %s", lines[3])
	}
	if !strings.Contains(lines[4], `"source": "mcp"`) && !strings.Contains(lines[4], `\"source\": \"mcp\"`) {
		t.Fatalf("list_audit_logs missing mcp source: %s", lines[4])
	}
	if !strings.Contains(lines[5], "print(\\\"hi\\\")") && !strings.Contains(lines[5], "mcp-demo") {
		t.Fatalf("resources/read missing YAML content: %s", lines[5])
	}

	runRec, err := store.GetRun(context.Background(), "mcp-run-1")
	if err != nil || runRec.TriggeredBy.String != "agent-alice" {
		t.Fatalf("expected run triggered_by=agent-alice, got %+v err=%v", runRec, err)
	}
}

func TestMCPHTTPAndKillTool(t *testing.T) {
	srv, store, runner := setupMCPTestServer(t)

	if err := store.CreateRunFull(context.Background(), "live-mcp-run", "mcp-demo", "", "", "", "admin"); err != nil {
		t.Fatal(err)
	}

	// Notification returns 202 Accepted.
	req := httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","method":"notifications/initialized"}`))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202 for notification, got %d", rec.Code)
	}

	// Tool call kill_execution over HTTP.
	callBody := `{"jsonrpc":"2.0","id":"k1","method":"tools/call","params":{"name":"kill_execution","arguments":{"run_id":"live-mcp-run"}}}`
	req = httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(callBody))
	req = req.WithContext(WithActor(req.Context(), "admin", "token=ops"))
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || runner.killedID != "live-mcp-run" || !strings.Contains(rec.Body.String(), "KILLED") {
		t.Fatalf("unexpected kill_execution HTTP result: code=%d killed=%q body=%s", rec.Code, runner.killedID, rec.Body.String())
	}

	// GET returns 405 cleanly per Streamable HTTP when SSE is unsupported.
	req = httptest.NewRequest(http.MethodGet, "/mcp", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on GET /mcp, got %d", rec.Code)
	}

	// DELETE returns 204; PUT returns 405.
	req = httptest.NewRequest(http.MethodDelete, "/mcp", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204 on DELETE /mcp, got %d", rec.Code)
	}
	req = httptest.NewRequest(http.MethodPut, "/mcp", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405 on PUT /mcp, got %d", rec.Code)
	}

	// Batch JSON-RPC request over HTTP handles multiple calls plus notification cleanly.
	batchBody := `[
		{"jsonrpc":"2.0","id":10,"method":"ping"},
		{"jsonrpc":"2.0","method":"notifications/initialized"},
		{"jsonrpc":"2.0","id":11,"method":"resources/list"},
		{"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"get_flow","arguments":{"file":"mcp-demo"}}},
		{"jsonrpc":"2.0","id":13,"method":"tools/call","params":{"name":"list_executions","arguments":{"status":"KILLED","limit":5}}},
		{"jsonrpc":"2.0","id":14,"method":"tools/call","params":{"name":"get_execution","arguments":{"run_id":"live-mcp-run"}}},
		{"jsonrpc":"2.0","id":15,"method":"tools/call","params":{"name":"run_flow","arguments":{"file":"mcp.yaml","wait":false}}}
	]`
	req = httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(batchBody))
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	srv.Wait()
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for batch HTTP /mcp, got %d: %s", rec.Code, rec.Body.String())
	}
	var batchOut []jsonRPCResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &batchOut); err != nil || len(batchOut) != 6 {
		t.Fatalf("expected 6 batch responses (notification omitted), got len=%d err=%v body=%s", len(batchOut), err, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "freud://flows/mcp.yaml") || !strings.Contains(rec.Body.String(), "live-mcp-run") {
		t.Fatalf("unexpected batch response body: %s", rec.Body.String())
	}

	// Protocol and tool error handling cases.
	errCases := []struct {
		body    string
		wantSub string
	}{
		{`{invalid json`, "-32700"},
		{`[]`, "-32600"},
		{`{"jsonrpc":"2.0","id":20,"method":"no_such_method"}`, "-32601"},
		{`{"jsonrpc":"2.0","id":21,"method":"tools/call","params":{"name":"no_such_tool"}}`, "-32601"},
		{`{"jsonrpc":"2.0","id":22,"method":"tools/call","params":{"name":"get_flow","arguments":{"file":"../secret.yaml"}}}`, `"isError":true`},
		{`{"jsonrpc":"2.0","id":23,"method":"tools/call","params":{"name":"list_executions","arguments":{"status":"BAD"}}}`, `"isError":true`},
		{`{"jsonrpc":"2.0","id":24,"method":"tools/call","params":{"name":"get_execution","arguments":{"run_id":"no-such-run"}}}`, `"isError":true`},
		{`{"jsonrpc":"2.0","id":25,"method":"tools/call","params":{"name":"kill_execution","arguments":{"run_id":"no-such-run"}}}`, `"isError":true`},
	}
	for _, tc := range errCases {
		req = httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(tc.body))
		rec = httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if !strings.Contains(rec.Body.String(), tc.wantSub) {
			t.Fatalf("for %s expected substring %q, got: %s", tc.body, tc.wantSub, rec.Body.String())
		}
	}
}

func TestMCPIncludesSpecEnvContextAndTail(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	includesDir := filepath.Join(tmp, "includes")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(includesDir, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(flowsDir, "demo.yaml"), []byte("name: demo\ntasks:\n  - id: s1\n    runtime: python\n    script: print(1)\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(includesDir, "sub", "helper.py"), []byte("def greet():\n    return 'hi'\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := db.Open(filepath.Join(tmp, "ctx.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Populate multi-line task logs to test tail_lines truncation.
	ctx := context.Background()
	_ = store.CreateRunFull(ctx, "long-run", "demo", "", "", "", "local")
	trID, _ := store.CreateTaskRunWithMeta(ctx, "long-run", "s1", "python", "python:3.13")
	_ = store.FinishTaskRun(ctx, trID, db.StatusSuccess, 0, "line1\nline2\nline3\nline4\nline5\n")
	_ = store.FinishRun(ctx, "long-run", db.StatusSuccess)

	// Guard secret value never appears in get_context output.
	t.Setenv("FREUD_TEST_ONLY_SECRET_VAL", "SHOULD_NOT_APPEAR")

	srv := NewServer(store, Config{
		FlowsDir:    flowsDir,
		IncludesDir: includesDir,
		EnvKeys:     []string{"API_KEY", "ENV_NAME"},
	})

	input := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"resources/list"}`,
		`{"jsonrpc":"2.0","id":2,"method":"resources/read","params":{"uri":"freud://docs/workflow-spec"}}`,
		`{"jsonrpc":"2.0","id":3,"method":"resources/read","params":{"uri":"freud://includes/sub/helper.py"}}`,
		`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"list_includes","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"get_include","arguments":{"path":"includes/sub/helper.py"}}}`,
		`{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"validate_flow","arguments":{"yaml":"name: draft\ntasks:\n  - id: a\n    runtime: python\n    script: print(1)"}}}`,
		`{"jsonrpc":"2.0","id":7,"method":"tools/call","params":{"name":"validate_flow","arguments":{"yaml":"name: bad\ntasks: []"}}}`,
		`{"jsonrpc":"2.0","id":8,"method":"tools/call","params":{"name":"get_context","arguments":{}}}`,
		`{"jsonrpc":"2.0","id":9,"method":"tools/call","params":{"name":"get_execution","arguments":{"run_id":"long-run","tail_lines":2}}}`,
	}, "\n") + "\n"

	var out bytes.Buffer
	if err := srv.ServeStdio(ctx, strings.NewReader(input), &out); err != nil {
		t.Fatalf("ServeStdio failed: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 9 {
		t.Fatalf("expected 9 responses, got %d:\n%s", len(lines), out.String())
	}

	if !strings.Contains(lines[0], "freud://docs/workflow-spec") || !strings.Contains(lines[0], "freud://includes/sub/helper.py") {
		t.Fatalf("resources/list missing spec or include URI: %s", lines[0])
	}
	if !strings.Contains(lines[1], "Freud Workflow YAML Specification") {
		t.Fatalf("workflow-spec resource missing spec title: %s", lines[1])
	}
	if !strings.Contains(lines[2], "def greet()") {
		t.Fatalf("include resource missing helper.py content: %s", lines[2])
	}
	if !strings.Contains(lines[3], "sub/helper.py") || !strings.Contains(lines[4], "def greet()") {
		t.Fatalf("list_includes / get_include mismatch: %s | %s", lines[3], lines[4])
	}
	if !strings.Contains(lines[5], `\"valid\": true`) || !strings.Contains(lines[6], `\"valid\": false`) {
		t.Fatalf("validate_flow mismatch: valid=%s invalid=%s", lines[5], lines[6])
	}
	if !strings.Contains(lines[7], "API_KEY") || !strings.Contains(lines[7], "ENV_NAME") || strings.Contains(lines[7], "SHOULD_NOT_APPEAR") {
		t.Fatalf("get_context unexpected env_keys: %s", lines[7])
	}
	if !strings.Contains(lines[8], "earlier lines omitted") || !strings.Contains(lines[8], "line5") || strings.Contains(lines[8], "line1") {
		t.Fatalf("get_execution tail_lines=2 failed: %s", lines[8])
	}
}
