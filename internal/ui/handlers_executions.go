package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"freud/internal/db"
)

type runRowView struct {
	ID           string
	WorkflowName string
	Status       string
	StatusSlug   string
	StartedAt    string
	FinishedAt   string
	Duration     string
	SearchKey    string
}

type executionsViewData struct {
	basePageData
	StatusFilter string
	Runs         []runRowView
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
	Run   runRowView
	Tasks []taskRunView
	DAG   DAGGraph
}

type runSnapshot struct {
	Tasks []struct {
		ID        string   `json:"id"`
		Runtime   string   `json:"runtime"`
		Image     string   `json:"image"`
		DependsOn []string `json:"depends_on"`
	} `json:"tasks"`
}

func (s *Server) buildExecutionsData(ctx context.Context, statusFilter string) (executionsViewData, error) {
	statusFilter = strings.ToUpper(strings.TrimSpace(statusFilter))
	switch statusFilter {
	case "", db.StatusSuccess, db.StatusFailed, db.StatusRunning:
	default:
		statusFilter = ""
	}

	stats, err := s.store.GetRunStats(ctx)
	if err != nil {
		return executionsViewData{}, err
	}
	runs, err := s.store.ListRuns(ctx, 200, statusFilter)
	if err != nil {
		return executionsViewData{}, err
	}

	return executionsViewData{
		basePageData: s.newBaseData("Executions", "executions", stats.Running),
		StatusFilter: statusFilter,
		Runs:         toRunRowViews(runs, time.Now()),
	}, nil
}

func (s *Server) handleExecutions(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildExecutionsData(r.Context(), r.URL.Query().Get("status"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderPage(w, "executions", data)
}

func (s *Server) handleExecutionsPartial(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildExecutionsData(r.Context(), r.URL.Query().Get("status"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderPartial(w, "executions_table", data)
}

func (s *Server) buildExecutionDetailData(ctx context.Context, runID string) (executionDetailViewData, bool, error) {
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
			SearchKey:   strings.ToLower(tr.TaskID + " " + tr.Status + " " + tr.Logs.String),
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
				dagInput = append(dagInput, DAGTaskInput{
					ID:        t.ID,
					Runtime:   t.Runtime,
					Image:     t.Image,
					DependsOn: t.DependsOn,
					Status:    st,
				})
			}
			dag = BuildDAGLayout(dagInput)
		}
	}

	return executionDetailViewData{
		basePageData: s.newBaseData(runRec.WorkflowName+" · "+runRec.ID, "executions", stats.Running),
		Run:          runView,
		Tasks:        taskViews,
		DAG:          dag,
	}, true, nil
}

func (s *Server) handleExecutionDetail(w http.ResponseWriter, r *http.Request) {
	data, found, err := s.buildExecutionDetailData(r.Context(), r.PathValue("id"))
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
	data, found, err := s.buildExecutionDetailData(r.Context(), r.PathValue("id"))
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
			SearchKey:    strings.ToLower(r.ID + " " + r.WorkflowName + " " + r.Status),
		})
	}
	return views
}
