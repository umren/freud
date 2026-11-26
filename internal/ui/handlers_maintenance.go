package ui

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"freud/internal/format"
	"freud/internal/maintenance"
)

type maintenanceViewData struct {
	basePageData
	Report         *maintenance.StorageReport
	WorkspacesSize string
	DBSize         string
	CacheSize      string
	LastRunAt      string
	LastReclaimed  string
	FlashMsg       string
	FlashErr       string
}

func (s *Server) requireAdmin(w http.ResponseWriter, r *http.Request) bool {
	if !s.cfg.EnableAuth {
		return true
	}
	cur := userFromRequest(r)
	if cur == nil || !cur.IsAdmin {
		http.Error(w, "admin privileges required", http.StatusForbidden)
		return false
	}
	return true
}

func (s *Server) handleMaintenancePage(w http.ResponseWriter, r *http.Request) {
	if s.maint == nil {
		http.Error(w, "maintenance service is not configured", http.StatusServiceUnavailable)
		return
	}
	stats, err := s.store.GetRunStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	rep, err := s.maint.InspectStorage(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	dbSize := ""
	if rep.DB.DBFileBytes > 0 {
		dbSize = format.Bytes(rep.DB.DBFileBytes)
	}
	lastAt := ""
	lastReclaimed := "0 B"
	if rep.LastResult != nil {
		lastAt = formatTimestamp(rep.LastResult.RanAt)
		lastReclaimed = format.Bytes(rep.LastResult.ReclaimedDiskBytes)
	}

	s.renderPage(w, "maintenance", maintenanceViewData{
		basePageData:   s.newBaseData(r, "Maintenance", "maintenance", stats.Running),
		Report:         rep,
		WorkspacesSize: format.Bytes(rep.WorkspacesBytes),
		DBSize:         dbSize,
		CacheSize:      format.Bytes(rep.CacheImagesBytes),
		LastRunAt:      lastAt,
		LastReclaimed:  lastReclaimed,
		FlashMsg:       r.URL.Query().Get("msg"),
		FlashErr:       r.URL.Query().Get("err"),
	})
}

func parseNonNegativeInt(raw string, label string) (int, error) {
	v, err := strconv.Atoi(raw)
	if err != nil || v < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", label)
	}
	return v, nil
}

func (s *Server) handleSaveMaintenanceSettings(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	runsDays, err := parseNonNegativeInt(r.FormValue("runs_days"), "Runs retention days")
	if err != nil {
		http.Redirect(w, r, "/maintenance?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	wsDays, err := parseNonNegativeInt(r.FormValue("workspaces_days"), "Workspaces retention days")
	if err != nil {
		http.Redirect(w, r, "/maintenance?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	auditDays, err := parseNonNegativeInt(r.FormValue("audit_days"), "Audit retention days")
	if err != nil {
		http.Redirect(w, r, "/maintenance?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	vacuum := r.FormValue("vacuum_sqlite") == "1"

	err = s.maint.SavePolicy(r.Context(), s.actorName(r), "dream", maintenance.Policy{
		RunsDays:       runsDays,
		WorkspacesDays: wsDays,
		AuditDays:      auditDays,
		VacuumSQLite:   vacuum,
	})
	if err != nil {
		http.Redirect(w, r, "/maintenance?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/maintenance?msg="+url.QueryEscape("Retention policy saved"), http.StatusSeeOther)
}

func (s *Server) handleResetMaintenanceSettings(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	if err := s.maint.ResetPolicy(r.Context(), s.actorName(r), "dream"); err != nil {
		http.Redirect(w, r, "/maintenance?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/maintenance?msg="+url.QueryEscape("Reset retention policy to config defaults"), http.StatusSeeOther)
}

func (s *Server) handleRunMaintenanceCleanup(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	res, err := s.maint.RunCleanup(r.Context(), "dream", s.actorName(r), true)
	if err != nil {
		http.Redirect(w, r, "/maintenance?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	msg := fmt.Sprintf("Cleanup finished: runs=%d, workspaces=%d (%s), audit=%d, sessions=%d",
		res.DeletedRuns, res.RemovedWorkspaces, format.Bytes(res.ReclaimedDiskBytes), res.DeletedAuditLogs, res.DeletedSessions)
	http.Redirect(w, r, "/maintenance?msg="+url.QueryEscape(msg), http.StatusSeeOther)
}

func (s *Server) handleClearMaintenanceCache(w http.ResponseWriter, r *http.Request) {
	if !s.requireAdmin(w, r) {
		return
	}
	removed, reclaimed, err := s.maint.ClearDockerCache(r.Context(), s.actorName(r), "dream")
	if err != nil {
		http.Redirect(w, r, "/maintenance?err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	msg := fmt.Sprintf("Cleared %d cached Docker image(s), reclaimed %s", removed, format.Bytes(reclaimed))
	http.Redirect(w, r, "/maintenance?msg="+url.QueryEscape(msg), http.StatusSeeOther)
}
