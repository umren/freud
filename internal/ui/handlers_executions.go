package ui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"freud/internal/db"
	"freud/internal/engine"
)

const defaultWebPageSize = 50

type paginationView struct {
	Page           int
	TotalPages     int
	Total          int
	StartItem      int
	EndItem        int
	HasPrev        bool
	HasNext        bool
	PrevURL        string
	NextURL        string
	PrevPartialURL string
	NextPartialURL string
}

func parsePageParam(r *http.Request) int {
	raw := strings.TrimSpace(r.URL.Query().Get("page"))
	if raw == "" {
		return 1
	}
	p, err := strconv.Atoi(raw)
	if err != nil || p < 1 {
		return 1
	}
	return p
}

func buildPaginationView(basePath, partialPath string, baseParams url.Values, page, pageSize, total int) paginationView {
	if pageSize <= 0 {
		pageSize = defaultWebPageSize
	}
	totalPages := 1
	if total > 0 {
		totalPages = (total + pageSize - 1) / pageSize
	}
	if page < 1 {
		page = 1
	}
	if page > totalPages {
		page = totalPages
	}

	startItem, endItem := 0, 0
	if total > 0 {
		startItem = (page-1)*pageSize + 1
		endItem = min(total, page*pageSize)
	}

	buildURL := func(prefix string, p int) string {
		if prefix == "" {
			return ""
		}
		vals := url.Values{}
		for k, vv := range baseParams {
			for _, v := range vv {
				if v != "" {
					vals.Add(k, v)
				}
			}
		}
		if p > 1 {
			vals.Set("page", strconv.Itoa(p))
		}
		enc := vals.Encode()
		if enc == "" {
			return prefix
		}
		return prefix + "?" + enc
	}

	pv := paginationView{
		Page:       page,
		TotalPages: totalPages,
		Total:      total,
		StartItem:  startItem,
		EndItem:    endItem,
		HasPrev:    page > 1,
		HasNext:    page < totalPages,
	}
	if pv.HasPrev {
		pv.PrevURL = buildURL(basePath, page-1)
		pv.PrevPartialURL = buildURL(partialPath, page-1)
	}
	if pv.HasNext {
		pv.NextURL = buildURL(basePath, page+1)
		pv.NextPartialURL = buildURL(partialPath, page+1)
	}
	return pv
}

type runRowView struct {
	ID           string
	WorkflowName string
	Status       string
	StatusSlug   string
	StartedAt    string
	FinishedAt   string
	Duration     string
	ParentRunID  string
	ParentTaskID string
	TriggeredBy  string
	SearchKey    string
}

type executionsViewData struct {
	basePageData
	StatusFilter string
	Query        string
	Runs         []runRowView
	Pagination   paginationView
}

type taskRunView struct {
	TaskID      string
	Status      string
	StatusSlug  string
	Runtime     string
	Image       string
	HasExitCode bool
	ExitCode    int64
	Logs        string
	Duration    string
	OffsetPct   string
	WidthPct    string
	SearchKey   string
}

type executionDetailViewData struct {
	basePageData
	Run       runRowView
	Tasks     []taskRunView
	ChildRuns []runRowView
	DAG       DAGGraph
	CanKill   bool
}

type runSnapshot struct {
	Tasks []struct {
		ID        string   `json:"id"`
		Runtime   string   `json:"runtime"`
		Image     string   `json:"image"`
		Flow      string   `json:"flow"`
		DependsOn []string `json:"depends_on"`
	} `json:"tasks"`
}

func (s *Server) buildExecutionsData(r *http.Request, statusFilter, searchQuery string, page int) (executionsViewData, error) {
	ctx := r.Context()
	statusFilter = strings.ToUpper(strings.TrimSpace(statusFilter))
	switch statusFilter {
	case "", db.StatusSuccess, db.StatusFailed, db.StatusRunning, db.StatusKilled:
	default:
		statusFilter = ""
	}
	searchQuery = strings.TrimSpace(searchQuery)

	stats, err := s.store.GetRunStats(ctx)
	if err != nil {
		return executionsViewData{}, err
	}
	runs, total, err := s.store.ListRunsPage(ctx, db.RunQuery{
		Status: statusFilter,
		Search: searchQuery,
		Limit:  defaultWebPageSize,
		Offset: (page - 1) * defaultWebPageSize,
	})
	if err != nil {
		return executionsViewData{}, err
	}

	params := url.Values{}
	if statusFilter != "" {
		params.Set("status", statusFilter)
	}
	if searchQuery != "" {
		params.Set("q", searchQuery)
	}
	pg := buildPaginationView("/executions", "/partials/executions", params, page, defaultWebPageSize, total)
	if pg.Page != page && total > 0 {
		runs, total, err = s.store.ListRunsPage(ctx, db.RunQuery{
			Status: statusFilter,
			Search: searchQuery,
			Limit:  defaultWebPageSize,
			Offset: (pg.Page - 1) * defaultWebPageSize,
		})
		if err != nil {
			return executionsViewData{}, err
		}
		pg = buildPaginationView("/executions", "/partials/executions", params, pg.Page, defaultWebPageSize, total)
	}

	return executionsViewData{
		basePageData: s.newBaseData(r, "Executions", "executions", stats.Running),
		StatusFilter: statusFilter,
		Query:        searchQuery,
		Runs:         toRunRowViews(runs, time.Now()),
		Pagination:   pg,
	}, nil
}

func (s *Server) handleExecutions(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	data, err := s.buildExecutionsData(r, q.Get("status"), q.Get("q"), parsePageParam(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderPage(w, "executions", data)
}

func (s *Server) handleExecutionsPartial(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	data, err := s.buildExecutionsData(r, q.Get("status"), q.Get("q"), parsePageParam(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderPartial(w, "executions_table", data)
}

func (s *Server) buildExecutionDetailData(r *http.Request, runID string) (executionDetailViewData, bool, error) {
	ctx := r.Context()
	runRec, err := s.store.GetRun(ctx, runID)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return executionDetailViewData{}, false, nil
		}
		return executionDetailViewData{}, false, err
	}
	taskRecs, err := s.store.ListTaskRuns(ctx, runID)
	if err != nil {
		return executionDetailViewData{}, false, err
	}
	childRecs, err := s.store.ListChildRuns(ctx, runID)
	if err != nil {
		return executionDetailViewData{}, false, err
	}
	stats, err := s.store.GetRunStats(ctx)
	if err != nil {
		return executionDetailViewData{}, false, err
	}

	now := time.Now()
	runView := toRunRowViews([]db.RunRecord{*runRec}, now)[0]

	runStart, _ := time.Parse(time.RFC3339Nano, runRec.CreatedAt)
	runEnd := now
	if runRec.FinishedAt.Valid {
		if parsed, err := time.Parse(time.RFC3339Nano, runRec.FinishedAt.String); err == nil {
			runEnd = parsed
		}
	}
	totalSpan := runEnd.Sub(runStart)
	if totalSpan <= 0 {
		totalSpan = time.Millisecond
	}

	taskStatusByID := make(map[string]string, len(taskRecs))
	taskViews := make([]taskRunView, 0, len(taskRecs))

	for _, tr := range taskRecs {
		taskStatusByID[tr.TaskID] = tr.Status
		tStart, _ := time.Parse(time.RFC3339Nano, tr.StartedAt)
		tEnd := now
		if tr.FinishedAt.Valid {
			if parsed, err := time.Parse(time.RFC3339Nano, tr.FinishedAt.String); err == nil {
				tEnd = parsed
			}
		}

		offset := float64(tStart.Sub(runStart)) / float64(totalSpan) * 100.0
		if offset < 0 {
			offset = 0
		}
		if offset > 98 {
			offset = 98
		}
		width := float64(tEnd.Sub(tStart)) / float64(totalSpan) * 100.0
		if width < 1.5 {
			width = 1.5
		}
		if offset+width > 100 {
			width = 100 - offset
		}

		dur := tEnd.Sub(tStart)
		if !tr.FinishedAt.Valid && tr.Status != db.StatusRunning {
			dur = 0
		}

		taskViews = append(taskViews, taskRunView{
			TaskID:      tr.TaskID,
			Status:      tr.Status,
			StatusSlug:  statusSlug(tr.Status),
			Runtime:     tr.Runtime.String,
			Image:       tr.Image.String,
			HasExitCode: tr.ExitCode.Valid,
			ExitCode:    tr.ExitCode.Int64,
			Logs:        tr.Logs.String,
			Duration:    formatDuration(dur),
			OffsetPct:   fmt.Sprintf("%.2f", offset),
			WidthPct:    fmt.Sprintf("%.2f", width),
			SearchKey:   strings.ToLower(tr.TaskID + " " + tr.Status),
		})
	}

	var dag DAGGraph
	if runRec.DefinitionJSON.Valid && runRec.DefinitionJSON.String != "" {
		var snap runSnapshot
		if err := json.Unmarshal([]byte(runRec.DefinitionJSON.String), &snap); err == nil && len(snap.Tasks) > 0 {
			dagInput := make([]DAGTaskInput, 0, len(snap.Tasks))
			for _, t := range snap.Tasks {
				st := taskStatusByID[t.ID]
				if st == "" {
					st = "PENDING"
				}
				rt := t.Runtime
				if t.Flow != "" {
					rt = "subflow → " + t.Flow
				}
				dagInput = append(dagInput, DAGTaskInput{
					ID:        t.ID,
					Runtime:   rt,
					Image:     t.Image,
					DependsOn: t.DependsOn,
					Status:    st,
				})
			}
			dag = BuildDAGLayout(dagInput)
		}
	}

	_, canKill := s.runner.(KillableFlowRunner)
	return executionDetailViewData{
		basePageData: s.newBaseData(r, runRec.WorkflowName+" · "+runRec.ID, "executions", stats.Running),
		Run:          runView,
		Tasks:        taskViews,
		ChildRuns:    toRunRowViews(childRecs, now),
		DAG:          dag,
		CanKill:      canKill,
	}, true, nil
}

func (s *Server) handleExecutionDetail(w http.ResponseWriter, r *http.Request) {
	data, found, err := s.buildExecutionDetailData(r, r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	s.renderPage(w, "execution_detail", data)
}

func (s *Server) handleExecutionDetailPartial(w http.ResponseWriter, r *http.Request) {
	data, found, err := s.buildExecutionDetailData(r, r.PathValue("id"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !found {
		http.NotFound(w, r)
		return
	}
	s.renderPartial(w, "execution_detail_content", data)
}

func (s *Server) killRunByID(ctx context.Context, runID, actor, source, details string) (int, error) {
	kr, ok := s.runner.(KillableFlowRunner)
	if !ok {
		return http.StatusServiceUnavailable, fmt.Errorf("execution killer is not configured")
	}

	if _, err := s.store.GetRun(ctx, runID); err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return http.StatusNotFound, fmt.Errorf("execution %q not found", runID)
		}
		return http.StatusInternalServerError, err
	}

	err := kr.Kill(runID)
	if err != nil && !errors.Is(err, engine.ErrRunNotActive) {
		return http.StatusInternalServerError, fmt.Errorf("kill run %q: %v", runID, err)
	}
	if err == nil {
		_ = s.store.RecordAudit(ctx, actor, "FLOW_KILL", runID, source, details)
	}
	return http.StatusOK, nil
}

func (s *Server) handleKillExecution(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	status, err := s.killRunByID(r.Context(), runID, s.actorName(r), "dream", "")
	if err != nil {
		http.Error(w, err.Error(), status)
		return
	}

	if r.Header.Get("HX-Request") == "true" {
		data, found, err := s.buildExecutionDetailData(r, runID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !found {
			http.NotFound(w, r)
			return
		}
		s.renderPartial(w, "execution_detail_content", data)
		return
	}
	http.Redirect(w, r, "/executions/"+runID, http.StatusSeeOther)
}

func toRunRowViews(records []db.RunRecord, now time.Time) []runRowView {
	views := make([]runRowView, 0, len(records))
	for _, r := range records {
		started := formatTimestamp(r.CreatedAt)
		finished := "—"
		durStr := "—"

		startT, startErr := time.Parse(time.RFC3339Nano, r.CreatedAt)
		if r.FinishedAt.Valid {
			finished = formatTimestamp(r.FinishedAt.String)
			if endT, endErr := time.Parse(time.RFC3339Nano, r.FinishedAt.String); startErr == nil && endErr == nil {
				durStr = formatDuration(endT.Sub(startT))
			}
		} else if r.Status == db.StatusRunning && startErr == nil {
			durStr = formatDuration(now.Sub(startT))
		}

		views = append(views, runRowView{
			ID:           r.ID,
			WorkflowName: r.WorkflowName,
			Status:       r.Status,
			StatusSlug:   statusSlug(r.Status),
			StartedAt:    started,
			FinishedAt:   finished,
			Duration:     durStr,
			ParentRunID:  r.ParentRunID.String,
			ParentTaskID: r.ParentTaskID.String,
			TriggeredBy:  r.TriggeredBy.String,
			SearchKey:    strings.ToLower(r.ID + " " + r.WorkflowName + " " + r.Status + " " + r.TriggeredBy.String),
		})
	}
	return views
}
