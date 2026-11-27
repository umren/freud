package ui

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"freud/internal/auth"
	"freud/internal/db"
	"freud/internal/env"
	"freud/internal/mcp"
	"freud/internal/service"
	"freud/internal/syncer"
	"freud/internal/workflow"
)

type apiErrorResponse struct {
	Error string `json:"error"`
}

type apiHealthResponse struct {
	Status        string `json:"status"`
	AuthEnabled   bool   `json:"auth_enabled"`
	SetupRequired bool   `json:"setup_required"`
	SchedulerOn   bool   `json:"scheduler_on"`
}

type apiUserView struct {
	Username  string `json:"username"`
	IsAdmin   bool   `json:"is_admin"`
	CreatedAt string `json:"created_at"`
}

type apiTokenView struct {
	ID         int64  `json:"id"`
	Username   string `json:"username"`
	Name       string `json:"name"`
	Scope      string `json:"scope"`
	Prefix     string `json:"prefix"`
	LastUsedAt string `json:"last_used_at,omitempty"`
	CreatedAt  string `json:"created_at"`
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(payload)
}

func writeAPIError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, apiErrorResponse{Error: msg})
}

func writeServiceError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, service.ErrFlowNotFound), errors.Is(err, service.ErrExecutionNotFound), errors.Is(err, service.ErrIncludeNotFound):
		writeAPIError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, service.ErrInvalidFlow), errors.Is(err, service.ErrInvalidStatus), errors.Is(err, service.ErrInvalidInclude):
		writeAPIError(w, http.StatusBadRequest, err.Error())
	default:
		writeAPIError(w, http.StatusInternalServerError, err.Error())
	}
}

func decodeJSONBody(r *http.Request, dst any) error {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	return json.Unmarshal(body, dst)
}

func parseIntQueryParam(r *http.Request, name string, def int) int {
	raw := strings.TrimSpace(r.URL.Query().Get(name))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

func parseLimitParam(r *http.Request, def, maxVal int) int {
	return service.ClampLimit(parseIntQueryParam(r, "limit", def), def, maxVal)
}

func (s *Server) handleAPIHealth(w http.ResponseWriter, r *http.Request) {
	setupRequired := false
	if s.cfg.EnableAuth {
		hasAdmin, err := s.store.HasAdmin(r.Context())
		if err != nil {
			writeAPIError(w, http.StatusInternalServerError, err.Error())
			return
		}
		setupRequired = !hasAdmin
	}
	writeJSON(w, http.StatusOK, apiHealthResponse{
		Status:        "ok",
		AuthEnabled:   s.cfg.EnableAuth,
		SetupRequired: setupRequired,
		SchedulerOn:   s.cfg.SchedulerOn,
	})
}

func (s *Server) handleAPIMe(w http.ResponseWriter, r *http.Request) {
	u := userFromRequest(r)
	if u == nil {
		writeJSON(w, http.StatusOK, apiUserView{
			Username: "local",
			IsAdmin:  !s.cfg.EnableAuth,
		})
		return
	}
	writeJSON(w, http.StatusOK, apiUserView{
		Username:  u.Username,
		IsAdmin:   u.IsAdmin,
		CreatedAt: u.CreatedAt,
	})
}

func (s *Server) handleAPIFlows(w http.ResponseWriter, r *http.Request) {
	flows, err := service.ListFlows(r.Context(), s.store, s.cfg.FlowsDir)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"flows": flows})
}

func (s *Server) requireAPIScope(w http.ResponseWriter, r *http.Request, requiredScope string) bool {
	if !s.cfg.EnableAuth {
		return true
	}
	if tok := apiTokenFromRequest(r); tok != nil {
		if !auth.ScopeAllows(tok.Scope, requiredScope) {
			writeAPIError(w, http.StatusForbidden, fmt.Sprintf("token scope %q does not allow %s operations", tok.Scope, requiredScope))
			return false
		}
	}
	return true
}

func (s *Server) writeBlockReason() string {
	if !s.cfg.AgentMadness {
		return "agent_madness is disabled in config"
	}
	if s.cfg.Syncer != nil && s.cfg.Syncer.Enabled() {
		return "git sync is enabled; modify flows and includes in the upstream repository"
	}
	return ""
}

func (s *Server) authorizeAgentMadnessWrite(w http.ResponseWriter, r *http.Request) bool {
	if !s.cfg.AgentMadness {
		writeAPIError(w, http.StatusForbidden, "agent_madness is disabled in config")
		return false
	}
	if s.cfg.Syncer != nil && s.cfg.Syncer.Enabled() {
		writeAPIError(w, http.StatusConflict, "flows and includes are managed by Git sync; modify them in the upstream repository")
		return false
	}
	if s.cfg.EnableAuth {
		cur := userFromRequest(r)
		if cur == nil || !cur.IsAdmin {
			writeAPIError(w, http.StatusForbidden, "admin privileges required")
			return false
		}
		if !s.requireAPIScope(w, r, auth.TokenScopeWrite) {
			return false
		}
	}
	return true
}

func (s *Server) recordAPIAudit(r *http.Request, action, target, extra string) {
	source, tokenNote := s.auditSource(r, "api")
	details := strings.TrimSpace(extra)
	if tokenNote != "" {
		if details != "" {
			details += ", " + tokenNote
		} else {
			details = tokenNote
		}
	}
	_ = s.store.RecordAudit(r.Context(), s.actorName(r), action, target, source, details)
}

func (s *Server) handleAPIFlowDetail(w http.ResponseWriter, r *http.Request) {
	detail, err := service.GetFlowDetail(r.Context(), s.store, s.cfg.FlowsDir, r.PathValue("file"), false, 20)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleAPISaveFlow(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAgentMadnessWrite(w, r) {
		return
	}
	var req struct {
		YAML string `json:"yaml"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	created, cleanName, wf, err := service.SaveFlow(s.cfg.FlowsDir, r.PathValue("file"), req.YAML)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	verb := "updated"
	status := http.StatusOK
	if created {
		verb = "created"
		status = http.StatusCreated
	}
	s.recordAPIAudit(r, "FLOW_SAVED", cleanName, fmt.Sprintf("action=%s, workflow=%s", verb, wf.Name))
	writeJSON(w, status, map[string]any{
		"file":        cleanName,
		"name":        wf.Name,
		"cron":        wf.Cron,
		"tasks_count": len(wf.Tasks),
		"created":     created,
	})
}

func (s *Server) handleAPIDeleteFlow(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAgentMadnessWrite(w, r) {
		return
	}
	cleanName, err := service.DeleteFlow(s.cfg.FlowsDir, r.PathValue("file"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	s.recordAPIAudit(r, "FLOW_DELETED", cleanName, "")
	writeJSON(w, http.StatusOK, map[string]any{
		"deleted": true,
		"file":    cleanName,
	})
}

func (s *Server) handleAPIRunFlow(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIScope(w, r, auth.TokenScopeRun) {
		return
	}
	matched, code, err := s.findFlowEntryResult(r.PathValue("file"))
	if err != nil {
		writeAPIError(w, code, err.Error())
		return
	}

	var req struct {
		Env map[string]string `json:"env"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	cleanEnv, err := env.ValidateEnvMap(req.Env)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	actor := s.actorName(r)
	source, tokenNote := s.auditSource(r, "api")
	runID, code, err := s.launchFlowAsync(r.Context(), actor, source, tokenNote, matched.Workflow, cleanEnv)
	if err != nil {
		writeAPIError(w, code, err.Error())
		return
	}

	resp := map[string]any{
		"run_id":        runID,
		"workflow_name": matched.Workflow.Name,
		"status":        db.StatusRunning,
		"triggered_by":  actor,
	}
	if envKeys := env.SortedKeys(cleanEnv); len(envKeys) > 0 {
		resp["env_keys"] = envKeys
	}
	writeJSON(w, http.StatusAccepted, resp)
}

func (s *Server) handleAPIExecutions(w http.ResponseWriter, r *http.Request) {
	page, err := service.ListExecutions(
		r.Context(),
		s.store,
		r.URL.Query().Get("status"),
		r.URL.Query().Get("q"),
		parseLimitParam(r, 50, 200),
		parseIntQueryParam(r, "offset", 0),
		50,
		200,
	)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleAPIExecutionDetail(w http.ResponseWriter, r *http.Request) {
	detail, err := service.GetExecutionDetail(r.Context(), s.store, r.PathValue("id"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	service.ApplyTailLines(detail, parseIntQueryParam(r, "tail_lines", 0))
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleAPIKillExecution(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIScope(w, r, auth.TokenScopeRun) {
		return
	}
	runID := r.PathValue("id")
	source, tokenNote := s.auditSource(r, "api")
	code, err := s.killRunByID(r.Context(), runID, s.actorName(r), source, tokenNote)
	if err != nil {
		writeAPIError(w, code, err.Error())
		return
	}
	runRec, err := s.store.GetRun(r.Context(), runID)
	status := db.StatusKilled
	if err == nil {
		status = runRec.Status
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"run_id": runID,
		"status": status,
	})
}

func (s *Server) handleAPIAudit(w http.ResponseWriter, r *http.Request) {
	page, err := service.ListAuditLogs(
		r.Context(),
		s.store,
		parseLimitParam(r, 100, 500),
		parseIntQueryParam(r, "offset", 0),
		100,
		500,
		r.URL.Query().Get("action"),
		r.URL.Query().Get("q"),
	)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *Server) handleAPIUsersList(w http.ResponseWriter, r *http.Request) {
	usersRaw, err := s.store.ListUsers(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]apiUserView, 0, len(usersRaw))
	for _, u := range usersRaw {
		out = append(out, apiUserView{
			Username:  u.Username,
			IsAdmin:   u.IsAdmin,
			CreatedAt: u.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": out})
}

func (s *Server) handleAPICreateUser(w http.ResponseWriter, r *http.Request) {
	cur := userFromRequest(r)
	if s.cfg.EnableAuth && (cur == nil || !cur.IsAdmin) {
		writeAPIError(w, http.StatusForbidden, "admin privileges required")
		return
	}
	if !s.requireAPIScope(w, r, auth.TokenScopeWrite) {
		return
	}

	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
		IsAdmin  bool   `json:"is_admin"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}

	username, err := auth.ValidateUsername(req.Username)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	created, err := s.store.CreateUser(r.Context(), username, hash, req.IsAdmin)
	if err != nil {
		status := http.StatusInternalServerError
		if errors.Is(err, db.ErrUserExists) {
			status = http.StatusConflict
		}
		writeAPIError(w, status, err.Error())
		return
	}

	roleLabel := "member"
	if req.IsAdmin {
		roleLabel = "admin"
	}
	source, tokenNote := s.auditSource(r, "api")
	details := "role=" + roleLabel
	if tokenNote != "" {
		details += ", " + tokenNote
	}
	_ = s.store.RecordAudit(r.Context(), s.actorName(r), "USER_CREATED", username, source, details)

	writeJSON(w, http.StatusCreated, apiUserView{
		Username:  created.Username,
		IsAdmin:   created.IsAdmin,
		CreatedAt: created.CreatedAt,
	})
}

func (s *Server) handleAPIDeleteUser(w http.ResponseWriter, r *http.Request) {
	cur := userFromRequest(r)
	if s.cfg.EnableAuth && (cur == nil || !cur.IsAdmin) {
		writeAPIError(w, http.StatusForbidden, "admin privileges required")
		return
	}
	if !s.requireAPIScope(w, r, auth.TokenScopeWrite) {
		return
	}

	target := strings.TrimSpace(r.PathValue("username"))
	if cur != nil && target == cur.Username {
		writeAPIError(w, http.StatusBadRequest, "cannot delete your own active account")
		return
	}

	if err := s.store.DeleteUser(r.Context(), target); err != nil {
		switch {
		case errors.Is(err, db.ErrLastAdmin):
			writeAPIError(w, http.StatusBadRequest, err.Error())
		case strings.Contains(err.Error(), "no rows"):
			writeAPIError(w, http.StatusNotFound, "user not found")
		default:
			writeAPIError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	source, tokenNote := s.auditSource(r, "api")
	_ = s.store.RecordAudit(r.Context(), s.actorName(r), "USER_DELETED", target, source, tokenNote)
	writeJSON(w, http.StatusOK, map[string]any{"deleted": target})
}

func (s *Server) handleAPITokensList(w http.ResponseWriter, r *http.Request) {
	cur := userFromRequest(r)
	ownerFilter := ""
	if s.cfg.EnableAuth {
		if cur == nil {
			writeAPIError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		if !cur.IsAdmin {
			ownerFilter = cur.Username
		}
	}

	rows, err := s.store.ListAPITokens(r.Context(), ownerFilter)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]apiTokenView, 0, len(rows))
	for _, tok := range rows {
		out = append(out, apiTokenView{
			ID:         tok.ID,
			Username:   tok.Username,
			Name:       tok.Name,
			Scope:      tok.Scope,
			Prefix:     tok.Prefix,
			LastUsedAt: tok.LastUsedAt.String,
			CreatedAt:  tok.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"tokens": out})
}

func (s *Server) handleAPICreateToken(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.EnableAuth {
		writeAPIError(w, http.StatusBadRequest, "API tokens require authentication to be enabled")
		return
	}
	cur := userFromRequest(r)
	if cur == nil {
		writeAPIError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if !s.requireAPIScope(w, r, auth.TokenScopeWrite) {
		return
	}

	var req struct {
		Name  string `json:"name"`
		Scope string `json:"scope"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	name, err := auth.ValidateTokenName(req.Name)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}
	scope := strings.TrimSpace(req.Scope)
	if scope == "" {
		scope = auth.TokenScopeRun
	}
	scope, err = auth.ValidateTokenScope(scope)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, err.Error())
		return
	}

	raw, prefix, hash, err := auth.NewAPIToken()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	rec, err := s.store.CreateAPITokenWithScope(r.Context(), cur.Username, name, prefix, hash, scope)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	source, tokenNote := s.auditSource(r, "api")
	details := "scope=" + scope + ", prefix=" + prefix
	if tokenNote != "" {
		details += ", " + tokenNote
	}
	_ = s.store.RecordAudit(r.Context(), cur.Username, "TOKEN_CREATED", name, source, details)

	writeJSON(w, http.StatusCreated, map[string]any{
		"token": raw,
		"meta": apiTokenView{
			ID:        rec.ID,
			Username:  rec.Username,
			Name:      rec.Name,
			Scope:     rec.Scope,
			Prefix:    rec.Prefix,
			CreatedAt: rec.CreatedAt,
		},
	})
}

func (s *Server) handleAPIDeleteToken(w http.ResponseWriter, r *http.Request) {
	if !s.cfg.EnableAuth {
		writeAPIError(w, http.StatusBadRequest, "API tokens require authentication to be enabled")
		return
	}
	cur := userFromRequest(r)
	if cur == nil {
		writeAPIError(w, http.StatusUnauthorized, "authentication required")
		return
	}
	if !s.requireAPIScope(w, r, auth.TokenScopeWrite) {
		return
	}
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil || id <= 0 {
		writeAPIError(w, http.StatusBadRequest, "invalid token id")
		return
	}

	deleted, err := s.store.DeleteAPIToken(r.Context(), id, cur.Username, cur.IsAdmin)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			writeAPIError(w, http.StatusNotFound, "token not found")
		case strings.Contains(err.Error(), "forbidden"):
			writeAPIError(w, http.StatusForbidden, err.Error())
		default:
			writeAPIError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}

	source, tokenNote := s.auditSource(r, "api")
	details := "owner=" + deleted.Username
	if tokenNote != "" {
		details += ", " + tokenNote
	}
	_ = s.store.RecordAudit(r.Context(), cur.Username, "TOKEN_REVOKED", deleted.Name, source, details)
	writeJSON(w, http.StatusOK, map[string]any{"revoked_id": deleted.ID, "name": deleted.Name})
}

func (s *Server) handleAPISyncStatus(w http.ResponseWriter, _ *http.Request) {
	if s.cfg.Syncer == nil {
		writeJSON(w, http.StatusOK, syncer.Status{Enabled: false})
		return
	}
	writeJSON(w, http.StatusOK, s.cfg.Syncer.Status())
}

func (s *Server) handleAPISyncFlows(w http.ResponseWriter, r *http.Request) {
	if !s.requireAPIScope(w, r, auth.TokenScopeRun) {
		return
	}
	if s.cfg.Syncer == nil || !s.cfg.Syncer.Enabled() {
		writeAPIError(w, http.StatusBadRequest, "git sync is not enabled")
		return
	}
	_, tokenNote := s.auditSource(r, "api")
	res, err := s.cfg.Syncer.Sync(r.Context(), syncer.Options{
		Trigger:     "api",
		Actor:       s.actorName(r),
		Details:     tokenNote,
		ForceRecord: true,
	})
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"error":  err.Error(),
			"result": res,
		})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleSyncWebhook(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Syncer == nil || !s.cfg.Syncer.Enabled() {
		writeAPIError(w, http.StatusNotFound, "git sync is not enabled")
		return
	}
	secret := s.cfg.Syncer.WebhookSecret()
	if strings.TrimSpace(secret) == "" {
		writeAPIError(w, http.StatusForbidden, "sync webhook secret is not configured")
		return
	}

	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	branch, ok := syncer.VerifyWebhook(r, body, secret)
	if !ok {
		writeAPIError(w, http.StatusUnauthorized, "invalid webhook signature or token")
		return
	}
	targetBranch := s.cfg.Syncer.Status().Branch
	if branch != "" && !strings.EqualFold(branch, targetBranch) {
		writeJSON(w, http.StatusOK, map[string]any{
			"ignored": true,
			"reason":  "branch mismatch",
			"branch":  branch,
		})
		return
	}

	res, err := s.cfg.Syncer.Sync(r.Context(), syncer.Options{
		Trigger:     "webhook",
		Actor:       "webhook",
		Details:     "branch=" + targetBranch,
		ForceRecord: true,
	})
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"error":  err.Error(),
			"result": res,
		})
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleAPIMaintenanceStatus(w http.ResponseWriter, r *http.Request) {
	if s.maint == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "maintenance service is not configured")
		return
	}
	rep, err := s.maint.InspectStorage(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, rep)
}

func (s *Server) handleAPIMaintenanceCleanup(w http.ResponseWriter, r *http.Request) {
	cur := userFromRequest(r)
	if s.cfg.EnableAuth && (cur == nil || !cur.IsAdmin) {
		writeAPIError(w, http.StatusForbidden, "admin privileges required")
		return
	}
	if !s.requireAPIScope(w, r, auth.TokenScopeWrite) {
		return
	}
	if s.maint == nil {
		writeAPIError(w, http.StatusServiceUnavailable, "maintenance service is not configured")
		return
	}
	res, err := s.maint.RunCleanup(r.Context(), "api", s.actorName(r), true)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) handleAPISpec(w http.ResponseWriter, r *http.Request) {
	if strings.EqualFold(strings.TrimSpace(r.URL.Query().Get("format")), "raw") {
		w.Header().Set("Content-Type", "text/markdown; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(workflow.SpecMarkdown))
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"markdown":           workflow.SpecMarkdown,
		"supported_runtimes": []string{workflow.RuntimePython, workflow.RuntimeJS, workflow.RuntimeShell},
	})
}

func (s *Server) handleAPIContext(w http.ResponseWriter, r *http.Request) {
	flows, err := service.ListFlows(r.Context(), s.store, s.cfg.FlowsDir)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	includes, err := service.ListIncludes(s.cfg.IncludesDir)
	if err != nil {
		writeServiceError(w, err)
		return
	}

	envKeys := []string{}
	if lister, ok := s.runner.(mcp.EnvKeyLister); ok {
		if keys := lister.EnvKeys(); len(keys) > 0 {
			envKeys = keys
		}
	}

	var syncStatus any
	if s.cfg.Syncer != nil {
		syncStatus = s.cfg.Syncer.Status()
	}

	blockReason := s.writeBlockReason()
	writable := blockReason == ""

	writeJSON(w, http.StatusOK, map[string]any{
		"env_keys":           envKeys,
		"flows_count":        len(flows),
		"includes_count":     len(includes),
		"scheduler_on":       s.cfg.SchedulerOn,
		"auth_enabled":       s.cfg.EnableAuth,
		"agent_madness":      s.cfg.AgentMadness,
		"writable_flows":     writable,
		"writable_includes":  writable,
		"write_block_reason": blockReason,
		"sync":               syncStatus,
		"supported_runtimes": []string{workflow.RuntimePython, workflow.RuntimeJS, workflow.RuntimeShell},
		"workflow_spec_url":  "/api/v1/spec",
	})
}

func (s *Server) handleAPIIncludesList(w http.ResponseWriter, _ *http.Request) {
	files, err := service.ListIncludes(s.cfg.IncludesDir)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"includes": files})
}

func (s *Server) handleAPIIncludeDetail(w http.ResponseWriter, r *http.Request) {
	inc, err := service.ReadInclude(s.cfg.IncludesDir, r.PathValue("path"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, inc)
}

func (s *Server) handleAPISaveInclude(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAgentMadnessWrite(w, r) {
		return
	}
	var req struct {
		Content string `json:"content"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	created, cleanPath, size, err := service.SaveInclude(s.cfg.IncludesDir, r.PathValue("path"), req.Content)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	verb := "updated"
	status := http.StatusOK
	if created {
		verb = "created"
		status = http.StatusCreated
	}
	s.recordAPIAudit(r, "INCLUDE_SAVED", cleanPath, fmt.Sprintf("action=%s, bytes=%d", verb, size))
	writeJSON(w, status, map[string]any{
		"path":    cleanPath,
		"size":    size,
		"created": created,
	})
}

func (s *Server) handleAPIDeleteInclude(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAgentMadnessWrite(w, r) {
		return
	}
	cleanPath, err := service.DeleteInclude(s.cfg.IncludesDir, r.PathValue("path"))
	if err != nil {
		writeServiceError(w, err)
		return
	}
	s.recordAPIAudit(r, "INCLUDE_DELETED", cleanPath, "")
	writeJSON(w, http.StatusOK, map[string]any{
		"deleted": true,
		"path":    cleanPath,
	})
}

func (s *Server) handleAPIValidateFlow(w http.ResponseWriter, r *http.Request) {
	var req struct {
		YAML string `json:"yaml"`
	}
	if err := decodeJSONBody(r, &req); err != nil {
		writeAPIError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}
	if strings.TrimSpace(req.YAML) == "" {
		writeAPIError(w, http.StatusBadRequest, "yaml field is required")
		return
	}
	wf, err := workflow.Parse([]byte(req.YAML))
	if err != nil {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{
			"valid": false,
			"error": err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"valid":       true,
		"name":        wf.Name,
		"cron":        wf.Cron,
		"tasks_count": len(wf.Tasks),
	})
}
