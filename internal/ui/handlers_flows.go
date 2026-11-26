package ui

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"freud/internal/cron"
	"freud/internal/engine"
	"freud/internal/service"
	"freud/internal/syncer"
	"freud/internal/workflow"
)

type flowRowView struct {
	FileName      string
	Name          string
	Cron          string
	NextRun       string
	StartsIn      string
	TaskCount     int
	ParseErr      string
	HasLastRun    bool
	LastRunID     string
	LastRunStatus string
	LastRunSlug   string
	LastRunAt     string
	SearchKey     string
}

type flowsViewData struct {
	basePageData
	FlowsDir string
	Flows    []flowRowView
	Sync     syncer.Status
	SyncMsg  string
	SyncErr  string
}

type flowDetailItem struct {
	Name      string
	FileName  string
	FilePath  string
	Cron      string
	TaskCount int
	RawYAML   string
}

type flowDetailViewData struct {
	basePageData
	Flow       flowDetailItem
	CanRun     bool
	NextRun    string
	StartsIn   string
	YAMLLines  []codeLine
	DAG        DAGGraph
	RecentRuns []runRowView
}

func (s *Server) handleFlows(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.GetRunStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	entries, err := workflow.LoadDir(s.cfg.FlowsDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	latestRuns, err := s.store.GetLatestRunByWorkflow(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	now := time.Now()
	rows := make([]flowRowView, 0, len(entries))
	for _, e := range entries {
		row := flowRowView{
			FileName:  e.FileName,
			SearchKey: strings.ToLower(e.FileName),
		}
		if e.ParseErr != nil {
			row.ParseErr = e.ParseErr.Error()
			rows = append(rows, row)
			continue
		}

		wf := e.Workflow
		row.Name = wf.Name
		row.TaskCount = len(wf.Tasks)
		row.Cron = wf.Cron
		row.SearchKey = strings.ToLower(wf.Name + " " + e.FileName + " " + wf.Cron)

		if wf.Cron != "" {
			if sched, err := cron.Parse(wf.Cron); err == nil {
				if next := sched.Next(now); !next.IsZero() {
					row.NextRun = next.Format("2006-01-02 15:04")
					row.StartsIn = formatRelativeDuration(next.Sub(now))
				}
			}
		}

		if last, ok := latestRuns[wf.Name]; ok {
			row.HasLastRun = true
			row.LastRunID = last.ID
			row.LastRunStatus = last.Status
			row.LastRunSlug = statusSlug(last.Status)
			row.LastRunAt = formatTimestamp(last.CreatedAt)
		}

		rows = append(rows, row)
	}

	var syncStatus syncer.Status
	if s.cfg.Syncer != nil {
		syncStatus = s.cfg.Syncer.Status()
	}

	s.renderPage(w, "flows", flowsViewData{
		basePageData: s.newBaseData(r, "Flows", "flows", stats.Running),
		FlowsDir:     s.cfg.FlowsDir,
		Flows:        rows,
		Sync:         syncStatus,
		SyncMsg:      r.URL.Query().Get("sync_msg"),
		SyncErr:      r.URL.Query().Get("sync_err"),
	})
}

func (s *Server) handleSyncFlows(w http.ResponseWriter, r *http.Request) {
	if s.cfg.Syncer == nil || !s.cfg.Syncer.Enabled() {
		http.Redirect(w, r, "/flows?sync_err="+url.QueryEscape("Git sync is not enabled"), http.StatusSeeOther)
		return
	}
	res, err := s.cfg.Syncer.Sync(r.Context(), syncer.Options{
		Trigger:     "dream",
		Actor:       s.actorName(r),
		ForceRecord: true,
	})
	if err != nil {
		http.Redirect(w, r, "/flows?sync_err="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	http.Redirect(w, r, "/flows?sync_msg="+url.QueryEscape(res.Message), http.StatusSeeOther)
}

func (s *Server) findFlowEntryResult(fileName string) (*workflow.WorkflowEntry, int, error) {
	matched, err := service.FindFlow(s.cfg.FlowsDir, fileName, false)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrFlowNotFound):
			return nil, http.StatusNotFound, err
		case errors.Is(err, service.ErrInvalidFlow):
			return nil, http.StatusBadRequest, err
		default:
			return nil, http.StatusInternalServerError, err
		}
	}
	return matched, http.StatusOK, nil
}

func (s *Server) findFlowEntry(w http.ResponseWriter, fileName string) (*workflow.WorkflowEntry, bool) {
	matched, status, err := s.findFlowEntryResult(fileName)
	if err != nil {
		if status == http.StatusNotFound {
			http.NotFound(w, nil)
		} else {
			http.Error(w, err.Error(), status)
		}
		return nil, false
	}
	return matched, true
}

func (s *Server) launchFlowAsync(auditCtx context.Context, actor, source, extraDetails string, wf *workflow.Workflow) (string, int, error) {
	if s.runner == nil {
		return "", http.StatusServiceUnavailable, fmt.Errorf("execution runner is not configured")
	}

	runCtx := engine.WithTriggeredBy(s.runCtx, actor)
	runID, doneCh, err := s.runner.StartAsync(runCtx, wf)
	if err != nil {
		return "", http.StatusInternalServerError, fmt.Errorf("start workflow %q: %w", wf.Name, err)
	}

	details := "run_id=" + runID
	if extraDetails != "" {
		details += ", " + extraDetails
	}
	_ = s.store.RecordAudit(auditCtx, actor, "FLOW_RUN", wf.Name, source, details)

	s.wg.Add(1)
	go func(name, id string, ch <-chan error) {
		defer s.wg.Done()
		if runErr := <-ch; runErr != nil {
			fmt.Fprintf(os.Stderr, "[ui] Manual run %s (%s) failed: %v\n", id, name, runErr)
		}
	}(wf.Name, runID, doneCh)

	return runID, http.StatusAccepted, nil
}

func (s *Server) handleFlowDetail(w http.ResponseWriter, r *http.Request) {
	matched, ok := s.findFlowEntry(w, r.PathValue("file"))
	if !ok {
		return
	}

	stats, err := s.store.GetRunStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	recentRaw, err := s.store.ListRunsByWorkflow(r.Context(), matched.Workflow.Name, 20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	now := time.Now()
	nextRun, startsIn := "", ""
	if matched.Workflow.Cron != "" {
		if sched, err := cron.Parse(matched.Workflow.Cron); err == nil {
			if next := sched.Next(now); !next.IsZero() {
				nextRun = next.Format("2006-01-02 15:04")
				startsIn = formatRelativeDuration(next.Sub(now))
			}
		}
	}

	dagInput := make([]DAGTaskInput, 0, len(matched.Workflow.Tasks))
	for _, t := range matched.Workflow.Tasks {
		rt := t.Runtime
		if t.Flow != "" {
			rt = "subflow → " + t.Flow
		}
		dagInput = append(dagInput, DAGTaskInput{
			ID:        t.ID,
			Runtime:   rt,
			Image:     t.Image,
			DependsOn: t.DependsOn,
		})
	}

	s.renderPage(w, "flow_detail", flowDetailViewData{
		basePageData: s.newBaseData(r, matched.Workflow.Name, "flows", stats.Running),
		Flow: flowDetailItem{
			Name:      matched.Workflow.Name,
			FileName:  matched.FileName,
			FilePath:  matched.FilePath,
			Cron:      matched.Workflow.Cron,
			TaskCount: len(matched.Workflow.Tasks),
			RawYAML:   matched.RawYAML,
		},
		CanRun:     s.runner != nil,
		NextRun:    nextRun,
		StartsIn:   startsIn,
		YAMLLines:  HighlightWorkflowYAML(matched.RawYAML),
		DAG:        BuildDAGLayout(dagInput),
		RecentRuns: toRunRowViews(recentRaw, now),
	})
}

func (s *Server) handleRunFlow(w http.ResponseWriter, r *http.Request) {
	matched, ok := s.findFlowEntry(w, r.PathValue("file"))
	if !ok {
		return
	}

	runID, status, err := s.launchFlowAsync(r.Context(), s.actorName(r), "dream", "", matched.Workflow)
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}

	target := "/executions/" + runID
	if r.Header.Get("HX-Request") == "true" {
		w.Header().Set("HX-Redirect", target)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}
