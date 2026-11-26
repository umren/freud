package ui

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"freud/internal/auth"
	"freud/internal/config"
	"freud/internal/db"
	"freud/internal/engine"
	"freud/internal/maintenance"
	"freud/internal/syncer"
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
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tokens", strings.NewReader(`{"name":"admin-cli","scope":"write"}`))
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
	req = httptest.NewRequest(http.MethodPost, "/api/v1/tokens", strings.NewReader(`{"name":"ci-json","scope":"write"}`))
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

	// Protected HTTP MCP endpoint (/mcp) requires auth, then executes tools as the token user.
	req = httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on unauthenticated /mcp, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_flows","arguments":{}}}`))
	req.Header.Set("Authorization", "Bearer "+ciTokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "api-flow") {
		t.Fatalf("expected 200 list_flows on authenticated /mcp, got %d: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api", nil)
	req.AddCookie(ciCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "/api/v1/flows/{file}/run") || !strings.Contains(rec.Body.String(), "ci-runner") {
		t.Fatalf("expected /api page with endpoints and token list, got %d", rec.Code)
	}

	// Verify API pagination (offset & total & has_more) and Web UI pagination across >50 runs.
	for i := 0; i < 55; i++ {
		id := fmt.Sprintf("bulk-run-%03d", i)
		if err := store.CreateRunFull(context.Background(), id, "bulk-flow", "", "", "", "ci"); err != nil {
			t.Fatal(err)
		}
	}
	req = httptest.NewRequest(http.MethodGet, "/api/v1/executions?q=bulk&limit=10&offset=50", nil)
	req.Header.Set("Authorization", "Bearer "+ciTokResp.Token)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"total": 55`) || !strings.Contains(rec.Body.String(), `"has_more": false`) {
		t.Fatalf("unexpected paginated API executions response: code=%d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/executions?q=bulk&page=2", nil)
	req.AddCookie(ciCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Showing 51–55 of 55") || !strings.Contains(rec.Body.String(), "Page 2/2") {
		t.Fatalf("expected Web UI /executions page 2 summary, got code=%d body=%s", rec.Code, rec.Body.String())
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

func runGitCmdForTest(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Freud Test",
		"GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=Freud Test",
		"GIT_COMMITTER_EMAIL=test@example.com",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v failed: %v\n%s", args, err, string(out))
	}
}

func TestUIAndWebhookGitSync(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	tmp := t.TempDir()
	bareRepo := filepath.Join(tmp, "remote.git")
	workRepo := filepath.Join(tmp, "work")
	flowsDir := filepath.Join(tmp, "flows")
	includesDir := filepath.Join(tmp, "includes")

	runGitCmdForTest(t, tmp, "init", "--bare", bareRepo)
	runGitCmdForTest(t, tmp, "clone", bareRepo, workRepo)
	runGitCmdForTest(t, workRepo, "checkout", "-b", "main")
	if err := os.MkdirAll(filepath.Join(workRepo, "flows"), 0o755); err != nil {
		t.Fatal(err)
	}
	flowYAML := "name: synced-flow\ntasks:\n  - id: s1\n    runtime: python\n    script: print(1)\n"
	if err := os.WriteFile(filepath.Join(workRepo, "flows", "synced.yaml"), []byte(flowYAML), 0o644); err != nil {
		t.Fatal(err)
	}
	runGitCmdForTest(t, workRepo, "add", "-A")
	runGitCmdForTest(t, workRepo, "commit", "-m", "add synced flow")
	runGitCmdForTest(t, workRepo, "push", "-u", "origin", "main")

	store, err := db.Open(filepath.Join(tmp, "uisync.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cfg := config.Default()
	cfg.FlowsDir = flowsDir
	cfg.IncludesDir = includesDir
	cfg.Sync = config.SyncConfig{
		Enabled:       true,
		Repo:          bareRepo,
		Branch:        "main",
		Interval:      "10m",
		CacheDir:      filepath.Join(tmp, "cache"),
		FlowsPath:     config.StringList{"flows"},
		WebhookSecret: "hook-secret-123",
	}
	gitSyncer, err := syncer.New(store, cfg)
	if err != nil {
		t.Fatal(err)
	}

	srv, err := NewServer(store, Config{
		FlowsDir:    flowsDir,
		IncludesDir: includesDir,
		EnableAuth:  false,
		Syncer:      gitSyncer,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.Handler()

	// 1. /flows renders Git Sync card and Sync Now button.
	req := httptest.NewRequest(http.MethodGet, "/flows", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Git Sync") || !strings.Contains(rec.Body.String(), "/flows/sync") {
		t.Fatalf("expected Git Sync card on /flows, code=%d", rec.Code)
	}

	// 2. Reject invalid webhook signature with 401.
	req = httptest.NewRequest(http.MethodPost, "/webhooks/sync", strings.NewReader(`{"ref":"refs/heads/main"}`))
	req.Header.Set("X-Hub-Signature-256", "sha256=deadbeef")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 on bad webhook signature, got %d", rec.Code)
	}

	// 3. Non-target branch webhook is safely ignored with 200.
	otherBody := []byte(`{"ref":"refs/heads/feature"}`)
	macOther := hmac.New(sha256.New, []byte("hook-secret-123"))
	_, _ = macOther.Write(otherBody)
	req = httptest.NewRequest(http.MethodPost, "/webhooks/sync", strings.NewReader(string(otherBody)))
	req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(macOther.Sum(nil)))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ignored": true`) {
		t.Fatalf("expected ignored response for non-target branch, code=%d body=%s", rec.Code, rec.Body.String())
	}

	// 4. Valid GitHub webhook pulls flow and creates a git-sync execution + audit entry.
	mainBody := []byte(`{"ref":"refs/heads/main"}`)
	macMain := hmac.New(sha256.New, []byte("hook-secret-123"))
	_, _ = macMain.Write(mainBody)
	req = httptest.NewRequest(http.MethodPost, "/webhooks/sync", strings.NewReader(string(mainBody)))
	req.Header.Set("X-Hub-Signature-256", "sha256="+hex.EncodeToString(macMain.Sum(nil)))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"changed": true`) || !strings.Contains(rec.Body.String(), `"synced.yaml"`) {
		t.Fatalf("expected webhook sync success, code=%d body=%s", rec.Code, rec.Body.String())
	}

	// 5. GET /api/v1/flows/sync and POST /flows/sync work.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/flows/sync", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"webhook_enabled": true`) {
		t.Fatalf("unexpected /api/v1/flows/sync status: code=%d body=%s", rec.Code, rec.Body.String())
	}
	req = httptest.NewRequest(http.MethodPost, "/flows/sync", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || !strings.HasPrefix(rec.Header().Get("Location"), "/flows?sync_msg=") {
		t.Fatalf("expected redirect with sync_msg after POST /flows/sync, got %d %q", rec.Code, rec.Header().Get("Location"))
	}
}

func TestUIMaintenancePageAndAPI(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	wsDir := filepath.Join(tmp, "workspaces")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	store, err := db.Open(filepath.Join(tmp, "maint_ui.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	adminHash, _ := auth.HashPassword("adminpass12")
	memberHash, _ := auth.HashPassword("memberpass12")
	_, _ = store.CreateUser(ctx, "admin", adminHash, true)
	_, _ = store.CreateUser(ctx, "member", memberHash, false)

	maintSvc, err := maintenance.New(store, wsDir, config.RetentionConfig{
		Interval:       "6h",
		RunsDays:       30,
		WorkspacesDays: 7,
		AuditDays:      90,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	srv, err := NewServer(store, Config{
		FlowsDir:    flowsDir,
		EnableAuth:  true,
		Maintenance: maintSvc,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.Handler()

	loginAs := func(user, pass string) *http.Cookie {
		form := url.Values{"username": {user}, "password": {pass}}
		req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		for _, c := range rec.Result().Cookies() {
			if c.Name == sessionCookieName {
				return c
			}
		}
		t.Fatalf("missing session cookie for %s", user)
		return nil
	}

	adminCookie := loginAs("admin", "adminpass12")
	memberCookie := loginAs("member", "memberpass12")

	// Member can inspect storage stats on /maintenance and GET /api/v1/maintenance, but cannot mutate settings or run cleanup.
	req := httptest.NewRequest(http.MethodGet, "/maintenance", nil)
	req.AddCookie(memberCookie)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Retention Policy") {
		t.Fatalf("expected 200 on /maintenance for member, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/maintenance/settings", strings.NewReader(url.Values{"runs_days": {"10"}}.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(memberCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-admin saving retention policy, got %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/maintenance/cleanup", nil)
	req.AddCookie(memberCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for non-admin POST /api/v1/maintenance/cleanup, got %d", rec.Code)
	}

	// Admin updates retention policy, runs cleanup, inspects JSON API, and resets to config defaults.
	saveForm := url.Values{
		"runs_days":       {"15"},
		"workspaces_days": {"3"},
		"audit_days":      {"45"},
		"vacuum_sqlite":   {"1"},
	}
	req = httptest.NewRequest(http.MethodPost, "/maintenance/settings", strings.NewReader(saveForm.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(adminCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther || !strings.HasPrefix(rec.Header().Get("Location"), "/maintenance?msg=") {
		t.Fatalf("expected redirect with flash after saving retention policy, got %d %q", rec.Code, rec.Header().Get("Location"))
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/maintenance", nil)
	req.AddCookie(adminCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"runs_days": 15`) || !strings.Contains(rec.Body.String(), `"using_ui_overrides": true`) {
		t.Fatalf("unexpected GET /api/v1/maintenance: code=%d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/maintenance/cleanup", nil)
	req.AddCookie(adminCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"trigger": "api"`) {
		t.Fatalf("unexpected POST /api/v1/maintenance/cleanup: code=%d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/maintenance/reset", nil)
	req.AddCookie(adminCookie)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected 303 after reset, got %d", rec.Code)
	}
	pol, overridden, _ := maintSvc.ResolvePolicy(ctx)
	if overridden || pol.RunsDays != 30 {
		t.Fatalf("expected policy reverted to config default 30d, got pol=%+v overridden=%v", pol, overridden)
	}
}

func TestAgentContextIncludesAndSpecAPI(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	includesDir := filepath.Join(tmp, "includes")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(includesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(includesDir, "helper.py"), []byte("SECRET_KEY_USAGE = 'ok'\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := db.Open(filepath.Join(tmp, "agent_api.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	eng := engine.New(store, nil, filepath.Join(tmp, "ws"))
	eng.SetEnv(map[string]string{
		"API_KEY":  "SUPER_SECRET_ENV_VALUE",
		"ENV_NAME": "production",
	})

	srv, err := NewServer(store, Config{
		FlowsDir:    flowsDir,
		IncludesDir: includesDir,
		EnableAuth:  false,
		Runner:      eng,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := srv.Handler()

	// 1. /api/v1/spec returns workflow specification (JSON & raw markdown).
	req := httptest.NewRequest(http.MethodGet, "/api/v1/spec", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "Freud Workflow YAML Specification") {
		t.Fatalf("unexpected /api/v1/spec: code=%d body=%s", rec.Code, rec.Body.String())
	}

	// 2. /api/v1/context returns ONLY sorted env_keys names and NEVER values.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/context", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	body := rec.Body.String()
	if rec.Code != http.StatusOK || !strings.Contains(body, `"API_KEY"`) || !strings.Contains(body, `"ENV_NAME"`) ||
		strings.Contains(body, "SUPER_SECRET_ENV_VALUE") || strings.Contains(body, "production") {
		t.Fatalf("unexpected or leaky /api/v1/context: code=%d body=%s", rec.Code, body)
	}

	// 3. /api/v1/includes and /api/v1/includes/{path...} list and read shared files safely.
	req = httptest.NewRequest(http.MethodGet, "/api/v1/includes", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "helper.py") {
		t.Fatalf("unexpected /api/v1/includes: code=%d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/includes/helper.py", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "SECRET_KEY_USAGE") {
		t.Fatalf("unexpected /api/v1/includes/helper.py: code=%d body=%s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/api/v1/includes/..%2Fagent_api.db", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on include traversal, got %d", rec.Code)
	}

	// 4. POST /api/v1/flows/validate checks drafts without creating files.
	req = httptest.NewRequest(http.MethodPost, "/api/v1/flows/validate", strings.NewReader(`{"yaml":"name: check-flow\ntasks:\n  - id: s1\n    runtime: python\n    script: print(1)"}`))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"valid": true`) {
		t.Fatalf("expected 200 valid flow draft, got %d: %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/api/v1/flows/validate", strings.NewReader(`{"yaml":"name: bad\ntasks: []"}`))
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnprocessableEntity || !strings.Contains(rec.Body.String(), `"valid": false`) {
		t.Fatalf("expected 422 invalid flow draft, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestAgentMadnessAndTokenScopesAPI(t *testing.T) {
	tmp := t.TempDir()
	flowsDir := filepath.Join(tmp, "flows")
	includesDir := filepath.Join(tmp, "includes")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(includesDir, 0o755); err != nil {
		t.Fatal(err)
	}
	baseYAML := "name: base\ntasks:\n  - id: s1\n    runtime: python\n    script: print(1)\n"
	if err := os.WriteFile(filepath.Join(flowsDir, "base.yaml"), []byte(baseYAML), 0o644); err != nil {
		t.Fatal(err)
	}

	store, err := db.Open(filepath.Join(tmp, "madness_api.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	hash, err := auth.HashPassword("secret123")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateUser(context.Background(), "alice", hash, true); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateUser(context.Background(), "bob", hash, false); err != nil {
		t.Fatal(err)
	}

	issueToken := func(username, name, scope string) string {
		raw, prefix, h, err := auth.NewAPIToken()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.CreateAPITokenWithScope(context.Background(), username, name, prefix, h, scope); err != nil {
			t.Fatal(err)
		}
		return raw
	}
	aliceRead := issueToken("alice", "alice-read", auth.TokenScopeRead)
	aliceRun := issueToken("alice", "alice-run", auth.TokenScopeRun)
	aliceWrite := issueToken("alice", "alice-write", auth.TokenScopeWrite)
	bobWrite := issueToken("bob", "bob-write", auth.TokenScopeWrite)

	doReq := func(h http.Handler, method, target, token, body string) *httptest.ResponseRecorder {
		var rBody *strings.Reader
		if body != "" {
			rBody = strings.NewReader(body)
		} else {
			rBody = strings.NewReader("")
		}
		req := httptest.NewRequest(method, target, rBody)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// 1. agent_madness=false by default: writes denied with 403.
	srvOff, err := NewServer(store, Config{
		FlowsDir:    flowsDir,
		IncludesDir: includesDir,
		EnableAuth:  true,
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := doReq(srvOff.Handler(), http.MethodGet, "/api/v1/context", aliceRead, "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"agent_madness": false`) || !strings.Contains(rec.Body.String(), `"writable_flows": false`) {
		t.Fatalf("unexpected context when agent_madness=false: %d %s", rec.Code, rec.Body.String())
	}
	for _, tc := range []struct{ m, p, b string }{
		{http.MethodPut, "/api/v1/flows/new.yaml", `{"yaml":"name: x\ntasks:\n  - id: a\n    runtime: python\n    script: print(1)"}`},
		{http.MethodDelete, "/api/v1/flows/base.yaml", ""},
		{http.MethodPut, "/api/v1/includes/a.py", `{"content":"x=1"}`},
		{http.MethodDelete, "/api/v1/includes/a.py", ""},
	} {
		rec = doReq(srvOff.Handler(), tc.m, tc.p, aliceWrite, tc.b)
		if rec.Code != http.StatusForbidden {
			t.Fatalf("expected 403 for %s %s with agent_madness=false, got %d: %s", tc.m, tc.p, rec.Code, rec.Body.String())
		}
	}

	// 2. Git sync enabled: writes blocked with 409 Conflict even when agent_madness=true.
	syncCfg := config.Default()
	syncCfg.FlowsDir = flowsDir
	syncCfg.IncludesDir = includesDir
	syncCfg.Sync = config.SyncConfig{
		Enabled: true,
		Repo:    filepath.Join(tmp, "repo.git"),
		Branch:  "main",
	}
	gitSyncer, err := syncer.New(store, syncCfg)
	if err != nil {
		t.Fatal(err)
	}
	srvSync, err := NewServer(store, Config{
		FlowsDir:     flowsDir,
		IncludesDir:  includesDir,
		EnableAuth:   true,
		AgentMadness: true,
		Syncer:       gitSyncer,
	})
	if err != nil {
		t.Fatal(err)
	}
	rec = doReq(srvSync.Handler(), http.MethodPut, "/api/v1/flows/new.yaml", aliceWrite, `{"yaml":"name: x\ntasks:\n  - id: a\n    runtime: python\n    script: print(1)"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict when git sync is active, got %d: %s", rec.Code, rec.Body.String())
	}
	rec = doReq(srvSync.Handler(), http.MethodPut, "/api/v1/includes/a.py", aliceWrite, `{"content":"x=1"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("expected 409 Conflict on include when git sync is active, got %d: %s", rec.Code, rec.Body.String())
	}

	// 3. agent_madness=true without Git sync: verify scopes, admin RBAC, REST + HTTP MCP CRUD, audit.
	runner := &mockAsyncRunner{store: store}
	srvOn, err := NewServer(store, Config{
		FlowsDir:     flowsDir,
		IncludesDir:  includesDir,
		EnableAuth:   true,
		AgentMadness: true,
		Runner:       runner,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srvOn.Wait)
	h := srvOn.Handler()

	// Scopes check:
	if rec := doReq(h, http.MethodGet, "/api/v1/tokens", aliceRead, ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"scope": "read"`) {
		t.Fatalf("expected GET /api/v1/tokens with scope, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(h, http.MethodPost, "/api/v1/flows/base.yaml/run", aliceRead, ""); rec.Code != http.StatusForbidden {
		t.Fatalf("expected read token blocked from /run with 403, got %d", rec.Code)
	}
	if rec := doReq(h, http.MethodPost, "/api/v1/flows/base.yaml/run", aliceRun, ""); rec.Code != http.StatusAccepted {
		t.Fatalf("expected run token allowed on /run with 202, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(h, http.MethodPost, "/api/v1/tokens", aliceRun, `{"name":"extra","scope":"read"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("expected run token blocked from creating tokens via API with 403, got %d", rec.Code)
	}
	if rec := doReq(h, http.MethodPut, "/api/v1/flows/x.yaml", aliceRun, `{"yaml":"name: x\ntasks:\n  - id: a\n    runtime: python\n    script: print(1)"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("expected run token blocked from PUT flow with 403, got %d", rec.Code)
	}
	if rec := doReq(h, http.MethodPost, "/api/v1/tokens", aliceWrite, `{"name":"scoped-check","scope":"read"}`); rec.Code != http.StatusCreated || !strings.Contains(rec.Body.String(), `"scope": "read"`) {
		t.Fatalf("expected write token to issue read token, got %d: %s", rec.Code, rec.Body.String())
	}

	// Non-admin with write token must still get 403.
	if rec := doReq(h, http.MethodPut, "/api/v1/flows/bob.yaml", bobWrite, `{"yaml":"name: bob\ntasks:\n  - id: a\n    runtime: python\n    script: print(1)"}`); rec.Code != http.StatusForbidden {
		t.Fatalf("expected non-admin write token 403, got %d: %s", rec.Code, rec.Body.String())
	}

	// Invalid YAML rejected with 400 and does not touch disk.
	if rec := doReq(h, http.MethodPut, "/api/v1/flows/bad.yaml", aliceWrite, `{"yaml":"name: bad\ntasks: []"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 on invalid YAML save, got %d: %s", rec.Code, rec.Body.String())
	}

	// Flow create -> update -> read -> delete
	flowPayload := `{"yaml":"name: created-via-api\ntasks:\n  - id: a\n    runtime: python\n    script: print(1)\n"}`
	if rec := doReq(h, http.MethodPut, "/api/v1/flows/created.yaml", aliceWrite, flowPayload); rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 on PUT flow create, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(h, http.MethodPut, "/api/v1/flows/created.yaml", aliceWrite, flowPayload); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on PUT flow update, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(h, http.MethodGet, "/api/v1/flows/created.yaml", aliceRead, ""); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on GET created flow, got %d", rec.Code)
	}
	if rec := doReq(h, http.MethodDelete, "/api/v1/flows/created.yaml", aliceWrite, ""); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on DELETE flow, got %d: %s", rec.Code, rec.Body.String())
	}

	// Include create -> update -> read -> delete
	if rec := doReq(h, http.MethodPut, "/api/v1/includes/sub/helper.py", aliceWrite, `{"content":"VAL = 1\n"}`); rec.Code != http.StatusCreated {
		t.Fatalf("expected 201 on PUT include create, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(h, http.MethodPut, "/api/v1/includes/sub/helper.py", aliceWrite, `{"content":"VAL = 2\n"}`); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on PUT include update, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(h, http.MethodGet, "/api/v1/includes/sub/helper.py", aliceRead, ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "VAL = 2") {
		t.Fatalf("expected updated include content, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(h, http.MethodDelete, "/api/v1/includes/sub/helper.py", aliceWrite, ""); rec.Code != http.StatusOK {
		t.Fatalf("expected 200 on DELETE include, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := doReq(h, http.MethodGet, "/api/v1/includes/sub/helper.py", aliceRead, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 after DELETE include, got %d", rec.Code)
	}

	// HTTP /mcp honors scopes & agent_madness too.
	mcpSaveBody := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"save_include","arguments":{"path":"via_mcp.py","content":"X=1"}}}`
	compact := func(s string) string { return strings.ReplaceAll(s, " ", "") }
	rec = doReq(h, http.MethodPost, "/mcp", aliceRead, mcpSaveBody)
	if rec.Code != http.StatusOK || !strings.Contains(compact(rec.Body.String()), `"isError":true`) {
		t.Fatalf("expected HTTP MCP save_include with read token to return tool error, got %d: %s", rec.Code, rec.Body.String())
	}
	rec = doReq(h, http.MethodPost, "/mcp", aliceWrite, mcpSaveBody)
	if rec.Code != http.StatusOK || strings.Contains(compact(rec.Body.String()), `"isError":true`) {
		t.Fatalf("expected HTTP MCP save_include with admin write token to succeed, got %d: %s", rec.Code, rec.Body.String())
	}

	for _, action := range []string{"FLOW_SAVED", "FLOW_DELETED", "INCLUDE_SAVED", "INCLUDE_DELETED"} {
		logs, _, err := store.ListAuditLogsPage(context.Background(), db.AuditQuery{Action: action, Limit: 10})
		if err != nil || len(logs) == 0 {
			t.Fatalf("expected audit log entry for %s, err=%v len=%d", action, err, len(logs))
		}
	}
}
