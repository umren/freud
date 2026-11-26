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

	"freud/internal/auth"
	"freud/internal/config"
	"freud/internal/db"
	"freud/internal/engine"
	"freud/internal/syncer"
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

func callMCPTool(t *testing.T, srv *Server, ctx context.Context, name string, args any) (string, bool) {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "tools/call",
		"params": map[string]any{
			"name":      name,
			"arguments": args,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := srv.ServeStdio(ctx, bytes.NewReader(append(payload, '\n')), &out); err != nil {
		t.Fatalf("ServeStdio(%s) failed: %v", name, err)
	}
	var resp struct {
		Result struct {
			Content []struct {
				Text string `json:"text"`
			} `json:"content"`
			IsError bool `json:"isError"`
		} `json:"result"`
		Error *struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &resp); err != nil {
		t.Fatalf("invalid JSON response for %s: %v\nraw: %s", name, err, out.String())
	}
	if resp.Error != nil {
		return resp.Error.Message, true
	}
	var txt string
	if len(resp.Result.Content) > 0 {
		txt = resp.Result.Content[0].Text
	}
	return txt, resp.Result.IsError
}

func TestMCPAgentMadnessAndTokenScopes(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	includesDir := filepath.Join(tmp, "includes")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(includesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	baseYAML := "name: mcp-base\ntasks:\n  - id: s1\n    runtime: python\n    script: print(1)\n"
	if err := os.WriteFile(filepath.Join(flowsDir, "mcp.yaml"), []byte(baseYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	store, err := db.Open(filepath.Join(tmp, "madness.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	runner := &mockMCPRunner{store: store}
	adminWriteCtx := WithAuth(context.Background(), "alice", "token=ci-write", true, auth.TokenScopeWrite, true)

	// 1. Disabled by default: context reports writable=false and all writes fail.
	srvOff := NewServer(store, Config{
		FlowsDir:    flowsDir,
		IncludesDir: includesDir,
		Runner:      runner,
	})
	txt, isErr := callMCPTool(t, srvOff, adminWriteCtx, "get_context", map[string]any{})
	if isErr || !strings.Contains(txt, `"agent_madness": false`) || !strings.Contains(txt, `"writable_flows": false`) {
		t.Fatalf("expected agent_madness=false in context, got err=%v text=%s", isErr, txt)
	}
	for _, tool := range []string{"save_flow", "delete_flow", "save_include", "delete_include"} {
		msg, failed := callMCPTool(t, srvOff, adminWriteCtx, tool, map[string]any{"file": "mcp.yaml", "yaml": baseYAML, "path": "a.py", "content": "x"})
		if !failed || !strings.Contains(msg, "agent_madness is disabled") {
			t.Fatalf("expected agent_madness disabled error for %s, got failed=%v msg=%s", tool, failed, msg)
		}
	}

	// 2. Git sync enabled: writes blocked even when agent_madness=true.
	syncCfg := config.Default()
	syncCfg.FlowsDir = flowsDir
	syncCfg.IncludesDir = includesDir
	syncCfg.Sync = config.SyncConfig{
		Enabled: true,
		Repo:    filepath.Join(tmp, "upstream.git"),
		Branch:  "main",
	}
	gitSyncer, err := syncer.New(store, syncCfg)
	if err != nil {
		t.Fatal(err)
	}
	srvSync := NewServer(store, Config{
		FlowsDir:     flowsDir,
		IncludesDir:  includesDir,
		Runner:       runner,
		Syncer:       gitSyncer,
		AgentMadness: true,
	})
	txt, isErr = callMCPTool(t, srvSync, adminWriteCtx, "get_context", map[string]any{})
	if isErr || !strings.Contains(txt, `"agent_madness": true`) || !strings.Contains(txt, `"writable_flows": false`) || !strings.Contains(txt, "git sync") {
		t.Fatalf("expected sync write block in context, got err=%v text=%s", isErr, txt)
	}
	msg, failed := callMCPTool(t, srvSync, adminWriteCtx, "save_flow", map[string]any{"file": "new.yaml", "yaml": baseYAML})
	if !failed || !strings.Contains(msg, "Git sync") {
		t.Fatalf("expected Git sync block on save_flow, got failed=%v msg=%s", failed, msg)
	}

	// 3. Enabled without Git sync: enforce token scopes + admin role + full CRUD/audit.
	srvOn := NewServer(store, Config{
		FlowsDir:     flowsDir,
		IncludesDir:  includesDir,
		Runner:       runner,
		AgentMadness: true,
	})

	readCtx := WithAuth(context.Background(), "alice", "token=ro", true, auth.TokenScopeRead, true)
	if _, failed := callMCPTool(t, srvOn, readCtx, "list_flows", map[string]any{}); failed {
		t.Fatalf("expected read token to list_flows")
	}
	if msg, failed := callMCPTool(t, srvOn, readCtx, "run_flow", map[string]any{"file": "mcp.yaml"}); !failed || !strings.Contains(msg, "scope") {
		t.Fatalf("expected read token to be blocked from run_flow, got failed=%v msg=%s", failed, msg)
	}
	if msg, failed := callMCPTool(t, srvOn, readCtx, "save_flow", map[string]any{"file": "a.yaml", "yaml": baseYAML}); !failed || !strings.Contains(msg, "write token scope") {
		t.Fatalf("expected read token blocked from save_flow, got failed=%v msg=%s", failed, msg)
	}

	runCtx := WithAuth(context.Background(), "alice", "token=ci-run", true, auth.TokenScopeRun, true)
	if txt, failed := callMCPTool(t, srvOn, runCtx, "run_flow", map[string]any{"file": "mcp.yaml", "wait": true}); failed || !strings.Contains(txt, "SUCCESS") {
		t.Fatalf("expected run token to run_flow, got failed=%v txt=%s", failed, txt)
	}
	srvOn.Wait()
	if msg, failed := callMCPTool(t, srvOn, runCtx, "save_flow", map[string]any{"file": "a.yaml", "yaml": baseYAML}); !failed || !strings.Contains(msg, "write token scope") {
		t.Fatalf("expected run token blocked from save_flow, got failed=%v msg=%s", failed, msg)
	}

	memberWriteCtx := WithAuth(context.Background(), "bob", "token=bob-write", false, auth.TokenScopeWrite, true)
	if msg, failed := callMCPTool(t, srvOn, memberWriteCtx, "save_flow", map[string]any{"file": "a.yaml", "yaml": baseYAML}); !failed || !strings.Contains(msg, "admin privileges required") {
		t.Fatalf("expected non-admin write token blocked, got failed=%v msg=%s", failed, msg)
	}

	// Admin + write token saves invalid YAML -> rejected without writing file.
	if _, failed := callMCPTool(t, srvOn, adminWriteCtx, "save_flow", map[string]any{"file": "bad.yaml", "yaml": "name: bad\ntasks: []"}); !failed {
		t.Fatalf("expected invalid YAML save_flow to fail")
	}
	if _, err := os.Stat(filepath.Join(flowsDir, "bad.yaml")); !os.IsNotExist(err) {
		t.Fatalf("bad.yaml should not exist on disk")
	}

	// Admin + write token full lifecycle: save/update/delete flow & include.
	if txt, failed := callMCPTool(t, srvOn, adminWriteCtx, "save_flow", map[string]any{"file": "agent.yaml", "yaml": baseYAML}); failed || !strings.Contains(txt, `"created": true`) {
		t.Fatalf("expected save_flow create, got failed=%v txt=%s", failed, txt)
	}
	updatedYAML := "name: mcp-updated\ntasks:\n  - id: s1\n    runtime: python\n    script: print(2)\n"
	if txt, failed := callMCPTool(t, srvOn, adminWriteCtx, "save_flow", map[string]any{"file": "agent.yaml", "yaml": updatedYAML}); failed || !strings.Contains(txt, `"created": false`) {
		t.Fatalf("expected save_flow update, got failed=%v txt=%s", failed, txt)
	}
	if txt, failed := callMCPTool(t, srvOn, adminWriteCtx, "save_include", map[string]any{"path": "sub/util.py", "content": "def f(): return 1\n"}); failed || !strings.Contains(txt, `"created": true`) {
		t.Fatalf("expected save_include create, got failed=%v txt=%s", failed, txt)
	}
	if txt, failed := callMCPTool(t, srvOn, adminWriteCtx, "delete_include", map[string]any{"path": "sub/util.py"}); failed || !strings.Contains(txt, `"deleted": true`) {
		t.Fatalf("expected delete_include, got failed=%v txt=%s", failed, txt)
	}
	if txt, failed := callMCPTool(t, srvOn, adminWriteCtx, "delete_flow", map[string]any{"file": "agent.yaml"}); failed || !strings.Contains(txt, `"deleted": true`) {
		t.Fatalf("expected delete_flow, got failed=%v txt=%s", failed, txt)
	}

	for _, action := range []string{"FLOW_SAVED", "INCLUDE_SAVED", "INCLUDE_DELETED", "FLOW_DELETED"} {
		logs, _, err := store.ListAuditLogsPage(context.Background(), db.AuditQuery{Action: action, Limit: 10})
		if err != nil || len(logs) == 0 {
			t.Fatalf("expected audit log %s, err=%v count=%d", action, err, len(logs))
		}
		if logs[0].Username != "alice" || logs[0].Source != "mcp" {
			t.Fatalf("unexpected audit actor/source for %s: %+v", action, logs[0])
		}
	}
}
