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
	"freud/internal/service"
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
	case errors.Is(err, service.ErrFlowNotFound), errors.Is(err, service.ErrExecutionNotFound):
		writeAPIError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, service.ErrInvalidFlow), errors.Is(err, service.ErrInvalidStatus):
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

func (s *Server) handleAPIFlowDetail(w http.ResponseWriter, r *http.Request) {
	detail, err := service.GetFlowDetail(r.Context(), s.store, s.cfg.FlowsDir, r.PathValue("file"), false, 20)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleAPIRunFlow(w http.ResponseWriter, r *http.Request) {
	matched, code, err := s.findFlowEntryResult(r.PathValue("file"))
	if err != nil {
		writeAPIError(w, code, err.Error())
		return
	}

	actor := s.actorName(r)
	source, tokenNote := s.auditSource(r, "api")
	runID, code, err := s.launchFlowAsync(r.Context(), actor, source, tokenNote, matched.Workflow)
	if err != nil {
		writeAPIError(w, code, err.Error())
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"run_id":        runID,
		"workflow_name": matched.Workflow.Name,
		"status":        db.StatusRunning,
		"triggered_by":  actor,
	})
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
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleAPIKillExecution(w http.ResponseWriter, r *http.Request) {
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

	var req struct {
		Name string `json:"name"`
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

	raw, prefix, hash, err := auth.NewAPIToken()
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	rec, err := s.store.CreateAPIToken(r.Context(), cur.Username, name, prefix, hash)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	source, _ := s.auditSource(r, "api")
	_ = s.store.RecordAudit(r.Context(), cur.Username, "TOKEN_CREATED", name, source, "prefix="+prefix)

	writeJSON(w, http.StatusCreated, map[string]any{
		"token": raw,
		"meta": apiTokenView{
			ID:        rec.ID,
			Username:  rec.Username,
			Name:      rec.Name,
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
