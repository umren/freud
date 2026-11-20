package ui

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"freud/internal/cron"
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

	s.renderPage(w, "flows", flowsViewData{
		basePageData: s.newBaseData("Flows", "flows", stats.Running),
		FlowsDir:     s.cfg.FlowsDir,
		Flows:        rows,
	})
}

func (s *Server) handleFlowDetail(w http.ResponseWriter, r *http.Request) {
	fileName := r.PathValue("file")
	if fileName == "" || strings.ContainsAny(fileName, "/\\") || fileName == "." || fileName == ".." {
		http.NotFound(w, r)
		return
	}

	entries, err := workflow.LoadDir(s.cfg.FlowsDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var matched *workflow.WorkflowEntry
	for i := range entries {
		if entries[i].FileName == fileName {
			matched = &entries[i]
			break
		}
	}
	if matched == nil {
		http.NotFound(w, r)
		return
	}
	if matched.ParseErr != nil {
		http.Error(w, fmt.Sprintf("invalid workflow %q: %v", fileName, matched.ParseErr), http.StatusBadRequest)
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
		dagInput = append(dagInput, DAGTaskInput{
			ID:        t.ID,
			Runtime:   t.Runtime,
			Image:     t.Image,
			DependsOn: t.DependsOn,
		})
	}

	s.renderPage(w, "flow_detail", flowDetailViewData{
		basePageData: s.newBaseData(matched.Workflow.Name, "flows", stats.Running),
		Flow: flowDetailItem{
			Name:      matched.Workflow.Name,
			FileName:  matched.FileName,
			FilePath:  matched.FilePath,
			Cron:      matched.Workflow.Cron,
			TaskCount: len(matched.Workflow.Tasks),
			RawYAML:   matched.RawYAML,
		},
		NextRun:    nextRun,
		StartsIn:   startsIn,
		YAMLLines:  HighlightWorkflowYAML(matched.RawYAML),
		DAG:        BuildDAGLayout(dagInput),
		RecentRuns: toRunRowViews(recentRaw, now),
	})
}
