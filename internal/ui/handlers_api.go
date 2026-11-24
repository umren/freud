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
	"time"

	"freud/internal/auth"
	"freud/internal/cron"
	"freud/internal/db"
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
	Prefix     string `json:"prefix"`
	LastUsedAt string `json:"last_used_at,omitempty"`
	CreatedAt  string `json:"created_at"`
}

type apiFlowSummary struct {
	File       string            `json:"file"`
	Name       string            `json:"name,omitempty"`
	Cron       string            `json:"cron,omitempty"`
	NextRun    string            `json:"next_run,omitempty"`
	TasksCount int               `json:"tasks_count"`
	ParseError string            `json:"parse_error,omitempty"`
	LastRun    *apiExecutionView `json:"last_run,omitempty"`
}

type apiFlowTaskView struct {
	ID           string   `json:"id"`
	Runtime      string   `json:"runtime,omitempty"`
	Image        string   `json:"image,omitempty"`
	Flow         string   `json:"flow,omitempty"`
	Wait         *bool    `json:"wait,omitempty"`
	DependsOn    []string `json:"depends_on,omitempty"`
	Dependencies []string `json:"dependencies,omitempty"`
}

type apiFlowDetail struct {
	File       string             `json:"file"`
	Name       string             `json:"name"`
	Cron       string             `json:"cron,omitempty"`
	NextRun    string             `json:"next_run,omitempty"`
	Tasks      []apiFlowTaskView  `json:"tasks"`
	RawYAML    string             `json:"raw_yaml"`
	RecentRuns []apiExecutionView `json:"recent_runs"`
}

type apiExecutionView struct {
	ID           string `json:"id"`
	WorkflowName string `json:"workflow_name"`
	Status       string `json:"status"`
	TriggeredBy  string `json:"triggered_by,omitempty"`
	ParentRunID  string `json:"parent_run_id,omitempty"`
	ParentTaskID string `json:"parent_task_id,omitempty"`
	CreatedAt    string `json:"created_at"`
	FinishedAt   string `json:"finished_at,omitempty"`
}

type apiTaskRunView struct {
	TaskID     string `json:"task_id"`
	Status     string `json:"status"`
	Runtime    string `json:"runtime,omitempty"`
	Image      string `json:"image,omitempty"`
	ExitCode   *int64 `json:"exit_code,omitempty"`
	Logs       string `json:"logs,omitempty"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at,omitempty"`
}

type apiAuditView struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Action    string `json:"action"`
	Target    string `json:"target,omitempty"`
	Source    string `json:"source"`
	Details   string `json:"details,omitempty"`
	CreatedAt string `json:"created_at"`
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

func toAPIExecutionView(r db.RunRecord) apiExecutionView {
	return apiExecutionView{
		ID:           r.ID,
		WorkflowName: r.WorkflowName,
		Status:       r.Status,
		TriggeredBy:  r.TriggeredBy.String,
		ParentRunID:  r.ParentRunID.String,
		ParentTaskID: r.ParentTaskID.String,
		CreatedAt:    r.CreatedAt,
		FinishedAt:   r.FinishedAt.String,
	}
}

func parseLimitParam(r *http.Request, def, maxVal int) int {
	raw := strings.TrimSpace(r.URL.Query().Get("limit"))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	if n > maxVal {
		return maxVal
	}
	return n
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
	entries, err := workflow.LoadDir(s.cfg.FlowsDir)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	latestRuns, err := s.store.GetLatestRunByWorkflow(r.Context())
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	now := time.Now()
	flows := make([]apiFlowSummary, 0, len(entries))
	for _, e := range entries {
		item := apiFlowSummary{File: e.FileName}
		if e.ParseErr != nil {
			item.ParseError = e.ParseErr.Error()
			flows = append(flows, item)
			continue
		}
		wf := e.Workflow
		item.Name = wf.Name
		item.Cron = wf.Cron
		item.TasksCount = len(wf.Tasks)
		if wf.Cron != "" {
			if sched, err := cron.Parse(wf.Cron); err == nil {
				if next := sched.Next(now); !next.IsZero() {
					item.NextRun = next.UTC().Format(time.RFC3339)
				}
			}
		}
		if last, ok := latestRuns[wf.Name]; ok {
			v := toAPIExecutionView(last)
			item.LastRun = &v
		}
		flows = append(flows, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"flows": flows})
}

func (s *Server) handleAPIFlowDetail(w http.ResponseWriter, r *http.Request) {
	matched, code, err := s.findFlowEntryResult(r.PathValue("file"))
	if err != nil {
		writeAPIError(w, code, err.Error())
		return
	}

	recentRaw, err := s.store.ListRunsByWorkflow(r.Context(), matched.Workflow.Name, 20)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	nextRun := ""
	if matched.Workflow.Cron != "" {
		if sched, err := cron.Parse(matched.Workflow.Cron); err == nil {
			if next := sched.Next(time.Now()); !next.IsZero() {
				nextRun = next.UTC().Format(time.RFC3339)
			}
		}
	}

	tasks := make([]apiFlowTaskView, 0, len(matched.Workflow.Tasks))
	for _, t := range matched.Workflow.Tasks {
		tasks = append(tasks, apiFlowTaskView{
			ID:           t.ID,
			Runtime:      t.Runtime,
			Image:        t.Image,
			Flow:         t.Flow,
			Wait:         t.Wait,
			DependsOn:    t.DependsOn,
			Dependencies: t.Dependencies,
		})
	}
	recent := make([]apiExecutionView, 0, len(recentRaw))
	for _, rec := range recentRaw {
		recent = append(recent, toAPIExecutionView(rec))
	}

	writeJSON(w, http.StatusOK, apiFlowDetail{
		File:       matched.FileName,
		Name:       matched.Workflow.Name,
		Cron:       matched.Workflow.Cron,
		NextRun:    nextRun,
		Tasks:      tasks,
		RawYAML:    matched.RawYAML,
		RecentRuns: recent,
	})
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
	ctx := r.Context()
	statusFilter := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("status")))
	switch statusFilter {
	case "", db.StatusSuccess, db.StatusFailed, db.StatusRunning, db.StatusKilled:
	default:
		writeAPIError(w, http.StatusBadRequest, "invalid status filter")
		return
	}

	limit := parseLimitParam(r, 50, 200)
	stats, err := s.store.GetRunStats(ctx)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	runs, err := s.store.ListRuns(ctx, limit, statusFilter)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := make([]apiExecutionView, 0, len(runs))
	for _, rec := range runs {
		out = append(out, toAPIExecutionView(rec))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"stats": stats,
		"runs":  out,
	})
}

func (s *Server) handleAPIExecutionDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	runID := r.PathValue("id")
	runRec, err := s.store.GetRun(ctx, runID)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			writeAPIError(w, http.StatusNotFound, "execution not found")
			return
		}
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	taskRecs, err := s.store.ListTaskRuns(ctx, runID)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}
	childRecs, err := s.store.ListChildRuns(ctx, runID)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	tasks := make([]apiTaskRunView, 0, len(taskRecs))
	for _, tr := range taskRecs {
		var exitCode *int64
		if tr.ExitCode.Valid {
			v := tr.ExitCode.Int64
			exitCode = &v
		}
		tasks = append(tasks, apiTaskRunView{
			TaskID:     tr.TaskID,
			Status:     tr.Status,
			Runtime:    tr.Runtime.String,
			Image:      tr.Image.String,
			ExitCode:   exitCode,
			Logs:       tr.Logs.String,
			StartedAt:  tr.StartedAt,
			FinishedAt: tr.FinishedAt.String,
		})
	}
	children := make([]apiExecutionView, 0, len(childRecs))
	for _, cr := range childRecs {
		children = append(children, toAPIExecutionView(cr))
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"run":        toAPIExecutionView(*runRec),
		"tasks":      tasks,
		"child_runs": children,
	})
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
	actionFilter := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("action")))
	limit := parseLimitParam(r, 100, 500)
	logsRaw, err := s.store.ListAuditLogs(r.Context(), limit, actionFilter)
	if err != nil {
		writeAPIError(w, http.StatusInternalServerError, err.Error())
		return
	}

	out := make([]apiAuditView, 0, len(logsRaw))
	for _, l := range logsRaw {
		out = append(out, apiAuditView{
			ID:        l.ID,
			Username:  l.Username,
			Action:    l.Action,
			Target:    l.Target.String,
			Source:    l.Source,
			Details:   l.Details.String,
			CreatedAt: l.CreatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"logs": out})
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
