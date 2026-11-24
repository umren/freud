package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"freud/internal/db"
	"freud/internal/engine"
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
	if err := store.CreateRunWithParent(ctx, "child-001", "child-flow", "", "run-001", "step2"); err != nil {
		t.Fatalf("CreateRunWithParent: %v", err)
	}
	if err := store.FinishRun(ctx, "child-001", db.StatusSuccess); err != nil {
		t.Fatalf("FinishRun child: %v", err)
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
		{"/executions/run-001", "Triggered Subflows"},
		{"/executions/child-001", "/executions/run-001"},
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

type mockKillRunner struct {
	store    *db.Store
	killedID string
}

func (m *mockKillRunner) StartAsync(_ context.Context, _ *workflow.Workflow) (string, <-chan error, error) {
	ch := make(chan error, 1)
	close(ch)
	return "unused", ch, nil
}

func (m *mockKillRunner) Kill(runID string) error {
	if runID != "run-live-1" {
		return engine.ErrRunNotActive
	}
	m.killedID = runID
	return m.store.FinishRun(context.Background(), runID, db.StatusKilled)
}

func TestUIKillExecution(t *testing.T) {
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

	if err := store.CreateRun(context.Background(), "run-live-1", "kill-demo"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	kr := &mockKillRunner{store: store}
	srv, err := NewServer(store, Config{
		FlowsDir: flowsDir,
		Runner:   kr,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	h := srv.Handler()

	// Running execution detail page must show the Kill Run button.
	getReq := httptest.NewRequest(http.MethodGet, "/executions/run-live-1", nil)
	getRec := httptest.NewRecorder()
	h.ServeHTTP(getRec, getReq)
	if getRec.Code != http.StatusOK || !strings.Contains(getRec.Body.String(), `/executions/run-live-1/kill`) {
		t.Fatalf("expected Kill Run button on running execution, code=%d", getRec.Code)
	}

	// HTMX POST /executions/{id}/kill should invoke Kill and render updated KILLED status.
	killReq := httptest.NewRequest(http.MethodPost, "/executions/run-live-1/kill", nil)
	killReq.Header.Set("HX-Request", "true")
	killRec := httptest.NewRecorder()
	h.ServeHTTP(killRec, killReq)
	if killRec.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from HTMX kill, got %d: %s", killRec.Code, killRec.Body.String())
	}
	if kr.killedID != "run-live-1" || !strings.Contains(killRec.Body.String(), "KILLED") {
		t.Fatalf("expected killedID=run-live-1 and KILLED badge, got killedID=%q body=%s", kr.killedID, killRec.Body.String())
	}

	// Filtering /executions by status=KILLED returns the killed run.
	listReq := httptest.NewRequest(http.MethodGet, "/executions?status=KILLED", nil)
	listRec := httptest.NewRecorder()
	h.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK || !strings.Contains(listRec.Body.String(), "run-live-1") {
		t.Fatalf("expected run-live-1 in KILLED filter, code=%d", listRec.Code)
	}

	// Unknown execution ID returns 404.
	missingReq := httptest.NewRequest(http.MethodPost, "/executions/no-such-run/kill", nil)
	missingRec := httptest.NewRecorder()
	h.ServeHTTP(missingRec, missingReq)
	if missingRec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown run kill, got %d", missingRec.Code)
	}
}

func TestUIAuthSetupLoginUsersAndAudit(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	includesDir := filepath.Join(tmp, "includes")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(includesDir, 0o755); err != nil {
		t.Fatal(err)
	}

	store, err := db.Open(filepath.Join(tmp, "auth.db"))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer store.Close()

	kr := &mockKillRunner{store: store}
	srv, err := NewServer(store, Config{
		Addr:        ":4321",
		FlowsDir:    flowsDir,
		IncludesDir: includesDir,
		Runner:      kr,
		EnableAuth:  true,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	h := srv.Handler()

	// 1. Before an admin exists, GET / redirects to /setup.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/setup" {
		t.Fatalf("expected 303 -> /setup, got %d %q", rec.Code, rec.Header().Get("Location"))
	}

	// 2. Complete initial setup at POST /setup -> creates admin and sets session cookie.
	setupForm := url.Values{
		"username":         {"admin"},
		"password":         {"supersecret12"},
		"confirm_password": {"supersecret12"},
	}
	req = httptest.NewRequest(http.MethodPost, "/setup", strings.NewReader(setupForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/" {
		t.Fatalf("expected 303 -> / after setup, got %d %q", rec.Code, rec.Header().Get("Location"))
	}
	var adminCookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			adminCookie = c
			break
		}
	}
	if adminCookie == nil || adminCookie.Value == "" {
		t.Fatal("expected session cookie after setup")
	}

	// 3. Unauthenticated requests now redirect to /login.
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/login" {
		t.Fatalf("expected 303 -> /login when unauthenticated, got %d %q", rec.Code, rec.Header().Get("Location"))
	}

	// 4. Admin creates a regular user 'alice'.
	createUserForm := url.Values{
		"username": {"alice"},
		"password": {"alicepass123"},
	}
	req = httptest.NewRequest(http.MethodPost, "/users", strings.NewReader(createUserForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(adminCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || !strings.HasPrefix(rec.Header().Get("Location"), "/audit") {
		t.Fatalf("expected 303 -> /audit after user creation, got %d %q", rec.Code, rec.Header().Get("Location"))
	}

	// 5. Log in as alice via POST /login.
	loginForm := url.Values{
		"username": {"alice"},
		"password": {"alicepass123"},
	}
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(loginForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/" {
		t.Fatalf("expected 303 -> / on alice login, got %d %q", rec.Code, rec.Header().Get("Location"))
	}
	var aliceCookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			aliceCookie = c
			break
		}
	}
	if aliceCookie == nil || aliceCookie.Value == "" {
		t.Fatal("expected session cookie for alice")
	}

	// 6. Regular user cannot create users (403 Forbidden), but can view /audit and kill runs.
	forbiddenForm := url.Values{"username": {"bob"}, "password": {"bobpass123"}}
	req = httptest.NewRequest(http.MethodPost, "/users", strings.NewReader(forbiddenForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(aliceCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-admin user creation, got %d", rec.Code)
	}

	ctx := context.Background()
	if err := store.CreateRunFull(ctx, "run-live-1", "demo-flow", "", "", "", "alice"); err != nil {
		t.Fatalf("CreateRunFull: %v", err)
	}

	// Alice kills the run -> recorded as FLOW_KILL in audit log.
	req = httptest.NewRequest(http.MethodPost, "/executions/run-live-1/kill", nil)
	req.AddCookie(aliceCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected 303 after kill, got %d", rec.Code)
	}

	// Executions page shows TriggeredBy 'alice'.
	req = httptest.NewRequest(http.MethodGet, "/executions", nil)
	req.AddCookie(aliceCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "alice") {
		t.Fatalf("expected /executions to include triggered_by alice, code=%d", rec.Code)
	}

	// /audit page shows SETUP_ADMIN, USER_CREATED, LOGIN, and FLOW_KILL events.
	req = httptest.NewRequest(http.MethodGet, "/audit", nil)
	req.AddCookie(adminCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on /audit, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"SETUP_ADMIN", "USER_CREATED", "LOGIN", "FLOW_KILL", "alice", "admin"} {
		if !strings.Contains(body, want) {
			t.Fatalf("expected /audit page to contain %q", want)
		}
	}

	// Logout clears session.
	req = httptest.NewRequest(http.MethodPost, "/logout", nil)
	req.AddCookie(aliceCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/login" {
		t.Fatalf("expected 303 -> /login after logout, got %d", rec.Code)
	}

	// Old alice cookie is no longer valid.
	req = httptest.NewRequest(http.MethodGet, "/flows", nil)
	req.AddCookie(aliceCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || rec.Header().Get("Location") != "/login" {
		t.Fatalf("expected invalidated session to redirect to /login, got %d", rec.Code)
	}
}

func TestRESTAPIAuthTokensAndEndpoints(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	includesDir := filepath.Join(tmp, "includes")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(includesDir, 0o755); err != nil {
		t.Fatal(err)
	}

	flowYAML := `name: api-flow
cron: "*/10 * * * *"
tasks:
  - id: ping
    runtime: python
    script: print("ok")
`
	if err := os.WriteFile(filepath.Join(flowsDir, "api.yaml"), []byte(flowYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := db.Open(filepath.Join(tmp, "restapi.db"))
	if err != nil {
		t.Fatalf("db.Open: %v", err)
	}
	defer store.Close()

	kr := &mockKillRunner{store: store}
	srv, err := NewServer(store, Config{
		Addr:        ":4321",
		FlowsDir:    flowsDir,
		IncludesDir: includesDir,
		Runner:      kr,
		EnableAuth:  true,
	})
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	h := srv.Handler()

	// 1. /api/v1/health is public and reports setup_required=true before /setup.
	req := httptest.NewRequest(http.MethodGet, "/api/v1/health", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"setup_required": true`) {
		t.Fatalf("unexpected health before setup: code=%d body=%s", rec.Code, rec.Body.String())
	}

	// Protected API endpoints return 409 JSON (not HTML redirect) before initial admin exists.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/flows", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict || !strings.Contains(rec.Body.String(), "/setup") {
		t.Fatalf("expected 409 JSON before setup, got %d %s", rec.Code, rec.Body.String())
	}

	// 2. Run /setup as admin and verify /api/v1/flows rejects unauthenticated calls with 401 JSON.
	setupForm := url.Values{
		"username":         {"admin"},
		"password":         {"adminpass12"},
		"confirm_password": {"adminpass12"},
	}
	req = httptest.NewRequest(http.MethodPost, "/setup", strings.NewReader(setupForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("setup failed: %d", rec.Code)
	}
	var adminCookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			adminCookie = c
			break
		}
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/flows", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Header().Get("Content-Type"), "application/json") {
		t.Fatalf("expected 401 JSON without credentials, got %d ct=%q", rec.Code, rec.Header().Get("Content-Type"))
	}

	// 3. Create an admin API token via POST /api/v1/tokens (using session cookie).
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tokens", strings.NewReader(`{"name":"admin-cli"}`))
	req.AddCookie(adminCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating admin token, got %d: %s", rec.Code, rec.Body.String())
	}
	var tokResp struct {
		Token string       `json:"token"`
		Meta  apiTokenView `json:"meta"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &tokResp); err != nil || !strings.HasPrefix(tokResp.Token, "frd_") {
		t.Fatalf("unexpected token response: err=%v body=%s", err, rec.Body.String())
	}

	// 4. Use Bearer token to create member 'ci' and check /api/v1/me, /api/v1/flows, /api/v1/flows/api.yaml.
	req = httptest.NewRequest(http.MethodPost, "/api/v1/users", strings.NewReader(`{"username":"ci","password":"cipass123","is_admin":false}`))
	req.Header.Set("Authorization", "Bearer "+tokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 creating user via API, got %d: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/flows/api.yaml", nil)
	req.Header.Set("Authorization", "Bearer "+tokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"name": "api-flow"`) {
		t.Fatalf("expected 200 flow detail JSON, got %d: %s", rec.Code, rec.Body.String())
	}

	// 5. Log in as 'ci' via /login, create a UI token at POST /tokens, then trigger & kill a run via API.
	loginForm := url.Values{"username": {"ci"}, "password": {"cipass123"}}
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(loginForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var ciCookie *http.Cookie
	for _, c := range rec.Result().Cookies() {
		if c.Name == sessionCookieName {
			ciCookie = c
			break
		}
	}

	req = httptest.NewRequest(http.MethodPost, "/tokens", strings.NewReader(url.Values{"name": {"ci-runner"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(ciCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "frd_") {
		t.Fatalf("expected /tokens to render one-time raw token, got %d", rec.Code)
	}

	// Create a second token via API for 'ci' so we can capture raw value cleanly in JSON.
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tokens", strings.NewReader(`{"name":"ci-json"}`))
	req.AddCookie(ciCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var ciTokResp struct {
		Token string       `json:"token"`
		Meta  apiTokenView `json:"meta"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &ciTokResp); err != nil {
		t.Fatalf("parse ci token response: %v", err)
	}

	// Member cannot create users via API (403).
	req = httptest.NewRequest(http.MethodPost, "/api/v1/users", strings.NewReader(`{"username":"bob","password":"bobpass123"}`))
	req.Header.Set("Authorization", "Bearer "+ciTokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 creating user as non-admin via API, got %d", rec.Code)
	}

	// Trigger flow run as 'ci' via Bearer token.
	req = httptest.NewRequest(http.MethodPost, "/api/v1/flows/api.yaml/run", nil)
	req.Header.Set("Authorization", "Bearer "+ciTokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusAccepted || !strings.Contains(rec.Body.String(), `"triggered_by": "ci"`) {
		t.Fatalf("expected 202 run response triggered by ci, got %d: %s", rec.Code, rec.Body.String())
	}
	srv.Wait()

	// Seed active run 'run-live-1' and verify execution detail + kill via API.
	if err := store.CreateRunFull(context.Background(), "run-live-1", "api-flow", "", "", "", "ci"); err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/executions/run-live-1", nil)
	req.Header.Set("Authorization", "Bearer "+ciTokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"triggered_by": "ci"`) {
		t.Fatalf("expected 200 execution detail, got %d: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/executions/run-live-1/kill", nil)
	req.Header.Set("Authorization", "Bearer "+ciTokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"status": "KILLED"`) {
		t.Fatalf("expected 200 KILLED response, got %d: %s", rec.Code, rec.Body.String())
	}

	// Audit log endpoint includes source="api" and token metadata.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/audit?action=FLOW_RUN", nil)
	req.Header.Set("Authorization", "Bearer "+ciTokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"source": "api"`) || !strings.Contains(rec.Body.String(), "token=ci-json") {
		t.Fatalf("expected API audit log entry, got %d: %s", rec.Code, rec.Body.String())
	}

	// Revoking token immediately invalidates Bearer auth.
	req = httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/v1/tokens/%d", ciTokResp.Meta.ID), nil)
	req.Header.Set("Authorization", "Bearer "+ciTokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 revoking own token, got %d: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/me", nil)
	req.Header.Set("Authorization", "Bearer "+ciTokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 after token revoked, got %d", rec.Code)
	}
}
