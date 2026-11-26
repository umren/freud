package ui

import (
	"fmt"
	"net/http"
	"strings"
)

type apiEndpointDoc struct {
	Method      string
	MethodSlug  string
	Path        string
	Access      string
	AccessSlug  string
	Params      string
	Description string
	SearchKey   string
}

type apiMCPToolDoc struct {
	Name        string
	Args        string
	Description string
}

type apiPageViewData struct {
	basePageData
	BaseURL     string
	Tokens      []apiTokenRowView
	Endpoints   []apiEndpointDoc
	MCPTools    []apiMCPToolDoc
	FlashMsg    string
	FlashErr    string
	NewRawToken string
}

func (s *Server) handleAPIDocsPage(w http.ResponseWriter, r *http.Request) {
	s.renderAPIPageWithState(w, r, r.URL.Query().Get("msg"), r.URL.Query().Get("err"), "")
}

func (s *Server) renderAPIPageWithState(w http.ResponseWriter, r *http.Request, flashMsg, flashErr, newRawToken string) {
	stats, err := s.store.GetRunStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var tokenOwner string
	if cur := userFromRequest(r); s.cfg.EnableAuth && cur != nil && !cur.IsAdmin {
		tokenOwner = cur.Username
	}
	tokensRaw, err := s.store.ListAPITokens(r.Context(), tokenOwner)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tokens := make([]apiTokenRowView, 0, len(tokensRaw))
	for _, t := range tokensRaw {
		lastUsed := "Never"
		if t.LastUsedAt.Valid && t.LastUsedAt.String != "" {
			lastUsed = formatTimestamp(t.LastUsedAt.String)
		}
		tokens = append(tokens, apiTokenRowView{
			ID:         t.ID,
			Username:   t.Username,
			Name:       t.Name,
			Prefix:     t.Prefix,
			LastUsedAt: lastUsed,
			CreatedAt:  formatTimestamp(t.CreatedAt),
		})
	}

	baseURL := "http://localhost" + s.cfg.Addr
	if strings.TrimSpace(r.Host) != "" {
		scheme := "http"
		if r.TLS != nil || strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https") {
			scheme = "https"
		}
		baseURL = fmt.Sprintf("%s://%s", scheme, r.Host)
	}

	base := s.newBaseData(r, "API & Tokens", "api", stats.Running)
	s.renderPage(w, "api", apiPageViewData{
		basePageData: base,
		BaseURL:      baseURL,
		Tokens:       tokens,
		Endpoints:    buildAPIEndpointDocs(),
		MCPTools:     buildMCPToolDocs(),
		FlashMsg:     flashMsg,
		FlashErr:     flashErr,
		NewRawToken:  newRawToken,
	})
}

func methodSlug(method string) string {
	switch method {
	case http.MethodGet:
		return "running"
	case http.MethodPost:
		return "success"
	case http.MethodDelete:
		return "killed"
	default:
		return "neutral"
	}
}

func accessSlug(access string) string {
	switch access {
	case "Admin only":
		return "accent"
	case "Public":
		return "success"
	default:
		return "neutral"
	}
}

func buildAPIEndpointDocs() []apiEndpointDoc {
	raw := []apiEndpointDoc{
		{Method: http.MethodGet, Path: "/api/v1/health", Access: "Public", Params: "—", Description: "Server status, auth_enabled, setup_required, and scheduler_on."},
		{Method: http.MethodGet, Path: "/api/v1/me", Access: "Any user", Params: "—", Description: "Current authenticated user (username, is_admin, created_at)."},
		{Method: http.MethodGet, Path: "/api/v1/flows", Access: "Any user", Params: "—", Description: "List discovered YAML workflows, cron schedules, and latest run status."},
		{Method: http.MethodGet, Path: "/api/v1/flows/{file}", Access: "Any user", Params: "path: file (e.g. etl-example.yaml)", Description: "Flow details, parsed task graph, raw YAML, and recent runs."},
		{Method: http.MethodPost, Path: "/api/v1/flows/{file}/run", Access: "Any user", Params: "path: file", Description: "Start workflow execution asynchronously (returns 202 with run_id)."},
		{Method: http.MethodGet, Path: "/api/v1/executions", Access: "Any user", Params: "query: status (RUNNING|SUCCESS|FAILED|KILLED), limit (1..200, default 50)", Description: "Aggregate run statistics and execution history newest-first."},
		{Method: http.MethodGet, Path: "/api/v1/executions/{id}", Access: "Any user", Params: "path: id (run_id)", Description: "Run metadata, child subflow runs, and task runs including logs and exit codes."},
		{Method: http.MethodPost, Path: "/api/v1/executions/{id}/kill", Access: "Any user", Params: "path: id (run_id)", Description: "Terminate an active RUNNING execution and record FLOW_KILL in audit log."},
		{Method: http.MethodGet, Path: "/api/v1/audit", Access: "Any user", Params: "query: action, limit (1..500, default 100)", Description: "User and agent audit log entries across Dream UI, API, MCP, TUI, and CLI."},
		{Method: http.MethodGet, Path: "/api/v1/users", Access: "Any user", Params: "—", Description: "List registered user accounts (usernames and admin flags, no password hashes)."},
		{Method: http.MethodPost, Path: "/api/v1/users", Access: "Admin only", Params: `body JSON: {"username", "password", "is_admin"}`, Description: "Create a new user account."},
		{Method: http.MethodDelete, Path: "/api/v1/users/{username}", Access: "Admin only", Params: "path: username", Description: "Delete a user account and revoke all of its sessions and API tokens."},
		{Method: http.MethodGet, Path: "/api/v1/tokens", Access: "Any user", Params: "—", Description: "List API token metadata (admins see all; members see their own)."},
		{Method: http.MethodPost, Path: "/api/v1/tokens", Access: "Any user", Params: `body JSON: {"name"}`, Description: "Generate a new frd_... API token (plaintext shown once)."},
		{Method: http.MethodDelete, Path: "/api/v1/tokens/{id}", Access: "Owner or Admin", Params: "path: id (token ID)", Description: "Revoke an API token immediately."},
		{Method: http.MethodPost, Path: "/mcp", Access: "Any user", Params: `body JSON-RPC 2.0: initialize, tools/list, tools/call, resources/list, resources/read`, Description: "Streamable HTTP MCP server endpoint for remote AI agents."},
	}
	for i := range raw {
		raw[i].MethodSlug = methodSlug(raw[i].Method)
		raw[i].AccessSlug = accessSlug(raw[i].Access)
		raw[i].SearchKey = strings.ToLower(raw[i].Method + " " + raw[i].Path + " " + raw[i].Access + " " + raw[i].Params + " " + raw[i].Description)
	}
	return raw
}

func buildMCPToolDocs() []apiMCPToolDoc {
	return []apiMCPToolDoc{
		{Name: "list_flows", Args: "{}", Description: "List discovered YAML workflows with cron schedules and latest run status."},
		{Name: "get_flow", Args: `{"file": "etl-example.yaml"}`, Description: "Inspect tasks, dependencies, raw YAML, and recent runs (accepts filename or workflow name)."},
		{Name: "run_flow", Args: `{"file": "...", "wait": false, "timeout_seconds": 60}`, Description: "Start a workflow; optionally wait up to 300s to return final status and task logs."},
		{Name: "list_executions", Args: `{"status": "RUNNING|SUCCESS|FAILED|KILLED", "limit": 20}`, Description: "List executions (max 100) and aggregate status counts."},
		{Name: "get_execution", Args: `{"run_id": "..."}`, Description: "Get run details, triggered_by actor, child subflows, and task logs."},
		{Name: "kill_execution", Args: `{"run_id": "..."}`, Description: "Stop an active RUNNING execution and write FLOW_KILL to audit logs."},
		{Name: "list_audit_logs", Args: `{"action": "FLOW_RUN", "limit": 50}`, Description: "Inspect user and agent activity audit logs (max 200)."},
	}
}
