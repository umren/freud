package ui

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"freud/internal/cron"
	"freud/internal/db"
	"freud/internal/engine"
	"freud/internal/workflow"
)

//go:embed static templates
var embeddedFS embed.FS

const maxPreviewBytes = 512 * 1024 // 512 KiB preview limit in web UI

// Config configures the embedded web UI server.
type Config struct {
	Addr        string
	FlowsDir    string
	IncludesDir string
	SchedulerOn bool
}

// Server serves the embedded Kestra-inspired web UI over HTTP.
type Server struct {
	store     *db.Store
	cfg       Config
	pages     map[string]*template.Template
	partials  *template.Template
	staticHnd http.Handler
}

// NewServer initializes embedded templates, static assets, and routes.
func NewServer(store *db.Store, cfg Config) (*Server, error) {
	if cfg.Addr == "" {
		cfg.Addr = ":4321"
	}
	if cfg.FlowsDir == "" {
		cfg.FlowsDir = "flows"
	}
	if cfg.IncludesDir == "" {
		cfg.IncludesDir = "includes"
	}

	pageFiles := map[string][]string{
		"dashboard":        {"templates/layout.html", "templates/dashboard.html", "templates/dashboard_partial.html"},
		"flows":            {"templates/layout.html", "templates/flows.html"},
		"flow_detail":      {"templates/layout.html", "templates/flow_detail.html", "templates/dag_partial.html"},
		"executions":       {"templates/layout.html", "templates/executions.html", "templates/executions_partial.html"},
		"execution_detail": {"templates/layout.html", "templates/execution_detail.html", "templates/execution_detail_partial.html", "templates/dag_partial.html"},
		"includes":         {"templates/layout.html", "templates/includes.html"},
	}

	pages := make(map[string]*template.Template, len(pageFiles))
	for name, files := range pageFiles {
		tmpl, err := template.ParseFS(embeddedFS, files...)
		if err != nil {
			return nil, fmt.Errorf("parse page template %q: %w", name, err)
		}
		pages[name] = tmpl
	}

	partials, err := template.ParseFS(
		embeddedFS,
		"templates/dashboard_partial.html",
		"templates/executions_partial.html",
		"templates/execution_detail_partial.html",
		"templates/dag_partial.html",
	)
	if err != nil {
		return nil, fmt.Errorf("parse partial templates: %w", err)
	}

	staticSub, err := fs.Sub(embeddedFS, "static")
	if err != nil {
		return nil, fmt.Errorf("open embedded static fs: %w", err)
	}

	return &Server{
		store:     store,
		cfg:       cfg,
		pages:     pages,
		partials:  partials,
		staticHnd: http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))),
	}, nil
}

// Handler returns the HTTP handler for the UI server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /static/", s.staticHnd)
	mux.HandleFunc("GET /{$}", s.handleDashboard)
	mux.HandleFunc("GET /partials/dashboard", s.handleDashboardPartial)
	mux.HandleFunc("GET /flows", s.handleFlows)
	mux.HandleFunc("GET /flows/{file}", s.handleFlowDetail)
	mux.HandleFunc("GET /executions", s.handleExecutions)
	mux.HandleFunc("GET /partials/executions", s.handleExecutionsPartial)
	mux.HandleFunc("GET /executions/{id}", s.handleExecutionDetail)
	mux.HandleFunc("GET /partials/executions/{id}", s.handleExecutionDetailPartial)
	mux.HandleFunc("GET /includes", s.handleIncludes)
	return mux
}

// StartScheduler runs a background cron loop that executes flows in flowsDir on schedule.
func StartScheduler(ctx context.Context, eng *engine.Engine, flowsDir string) {
	lastFired := make(map[string]string)
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	checkAndRun := func(now time.Time) {
		entries, err := workflow.LoadDir(flowsDir)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[scheduler] Failed to scan %q: %v\n", flowsDir, err)
			return
		}
		currentMinute := now.Truncate(time.Minute)
		minuteKey := currentMinute.Format("2006-01-02T15:04")

		for _, entry := range entries {
			if entry.Workflow == nil || entry.Workflow.Cron == "" {
				continue
			}
			if lastFired[entry.FileName] == minuteKey {
				continue
			}
			sched, err := cron.Parse(entry.Workflow.Cron)
			if err != nil {
				continue
			}
			if sched.Next(currentMinute.Add(-time.Second)).Equal(currentMinute) {
				lastFired[entry.FileName] = minuteKey
				fmt.Printf("[scheduler] Triggering scheduled flow %q (cron=%q)\n", entry.Workflow.Name, entry.Workflow.Cron)
				if _, runErr := eng.Run(ctx, entry.Workflow); runErr != nil {
					fmt.Fprintf(os.Stderr, "[scheduler] Flow %q failed: %v\n", entry.Workflow.Name, runErr)
				}
			}
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			checkAndRun(now)
		}
	}
}

type basePageData struct {
	Title        string
	Nav          string
	RunningCount int
	Addr         string
	SchedulerOn  bool
}

type upcomingScheduleView struct {
	Name             string
	FileName         string
	Cron             string
	NextRunAt        time.Time
	NextRunFormatted string
	StartsIn         string
}

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

type dashboardViewData struct {
	basePageData
	Stats       db.RunStats
	SuccessRate string
	FlowCount   int
	Upcoming    []upcomingScheduleView
	RecentRuns  []runRowView
}

func (s *Server) buildDashboardData(ctx context.Context) (dashboardViewData, error) {
	stats, err := s.store.GetRunStats(ctx)
	if err != nil {
		return dashboardViewData{}, err
	}

	entries, err := workflow.LoadDir(s.cfg.FlowsDir)
	if err != nil {
		return dashboardViewData{}, err
	}

	recentRaw, err := s.store.ListRuns(ctx, 8, "")
	if err != nil {
		return dashboardViewData{}, err
	}

	now := time.Now()
	var upcoming []upcomingScheduleView
	validFlows := 0
	for _, e := range entries {
		if e.Workflow == nil {
			continue
		}
		validFlows++
		if e.Workflow.Cron == "" {
			continue
		}
		sched, err := cron.Parse(e.Workflow.Cron)
		if err != nil {
			continue
		}
		next := sched.Next(now)
		if next.IsZero() {
			continue
		}
		upcoming = append(upcoming, upcomingScheduleView{
			Name:             e.Workflow.Name,
			FileName:         e.FileName,
			Cron:             e.Workflow.Cron,
			NextRunAt:        next,
			NextRunFormatted: next.Format("2006-01-02 15:04"),
			StartsIn:         formatRelativeDuration(next.Sub(now)),
		})
	}

	sort.Slice(upcoming, func(i, j int) bool {
		return upcoming[i].NextRunAt.Before(upcoming[j].NextRunAt)
	})

	successRate := "—"
	completed := stats.Success + stats.Failed
	if completed > 0 {
		pct := float64(stats.Success) * 100.0 / float64(completed)
		successRate = fmt.Sprintf("%.0f%%", pct)
	}

	return dashboardViewData{
		basePageData: basePageData{
			Title:        "Dashboard",
			Nav:          "dashboard",
			RunningCount: stats.Running,
			Addr:         s.cfg.Addr,
			SchedulerOn:  s.cfg.SchedulerOn,
		},
		Stats:       stats,
		SuccessRate: successRate,
		FlowCount:   validFlows,
		Upcoming:    upcoming,
		RecentRuns:  toRunRowViews(recentRaw, now),
	}, nil
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildDashboardData(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderPage(w, "dashboard", data)
}

func (s *Server) handleDashboardPartial(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildDashboardData(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderPartial(w, "dashboard_content", data)
}

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
		basePageData: basePageData{
			Title:        "Flows",
			Nav:          "flows",
			RunningCount: stats.Running,
			Addr:         s.cfg.Addr,
			SchedulerOn:  s.cfg.SchedulerOn,
		},
		FlowsDir: s.cfg.FlowsDir,
		Flows:    rows,
	})
}

type codeLine struct {
	Number int
	HTML   template.HTML
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
		basePageData: basePageData{
			Title:        matched.Workflow.Name,
			Nav:          "flows",
			RunningCount: stats.Running,
			Addr:         s.cfg.Addr,
			SchedulerOn:  s.cfg.SchedulerOn,
		},
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

type executionsViewData struct {
	basePageData
	StatusFilter string
	Runs         []runRowView
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
		basePageData: basePageData{
			Title:        "Executions",
			Nav:          "executions",
			RunningCount: stats.Running,
			Addr:         s.cfg.Addr,
			SchedulerOn:  s.cfg.SchedulerOn,
		},
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
		basePageData: basePageData{
			Title:        runRec.WorkflowName + " · " + runRec.ID,
			Nav:          "executions",
			RunningCount: stats.Running,
			Addr:         s.cfg.Addr,
			SchedulerOn:  s.cfg.SchedulerOn,
		},
		Run:   runView,
		Tasks: taskViews,
		DAG:   dag,
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

type includeFileEntry struct {
	RelPath       string
	Size          int64
	SizeFormatted string
	SearchKey     string
}

type includesViewData struct {
	basePageData
	IncludesDir  string
	Files        []includeFileEntry
	SelectedRel  string
	SelectedSize string
	RawContent   string
	Lines        []codeLine
	ReadErr      string
}

func (s *Server) handleIncludes(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.GetRunStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	files, err := listIncludeFiles(s.cfg.IncludesDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	selected := strings.TrimSpace(r.URL.Query().Get("file"))
	if selected == "" && len(files) > 0 {
		selected = files[0].RelPath
	}

	data := includesViewData{
		basePageData: basePageData{
			Title:        "Includes",
			Nav:          "includes",
			RunningCount: stats.Running,
			Addr:         s.cfg.Addr,
			SchedulerOn:  s.cfg.SchedulerOn,
		},
		IncludesDir: s.cfg.IncludesDir,
		Files:       files,
		SelectedRel: selected,
	}

	if selected != "" {
		content, size, readErr := readIncludeFileSafe(s.cfg.IncludesDir, selected)
		if readErr != nil {
			data.ReadErr = readErr.Error()
		} else {
			data.SelectedSize = formatHumanBytes(size)
			data.RawContent = content
			data.Lines = HighlightByFilename(selected, content)
		}
	}

	s.renderPage(w, "includes", data)
}

func listIncludeFiles(dir string) ([]includeFileEntry, error) {
	absRoot, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(absRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if !info.IsDir() {
		return nil, nil
	}

	var files []includeFileEntry
	err = filepath.WalkDir(absRoot, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(absRoot, p)
		if err != nil {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)
		files = append(files, includeFileEntry{
			RelPath:       relSlash,
			Size:          fi.Size(),
			SizeFormatted: formatHumanBytes(fi.Size()),
			SearchKey:     strings.ToLower(relSlash),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].RelPath < files[j].RelPath
	})
	return files, nil
}

func readIncludeFileSafe(dir, rel string) (string, int64, error) {
	absRoot, err := filepath.Abs(dir)
	if err != nil {
		return "", 0, err
	}
	cleanRel := filepath.Clean(filepath.FromSlash(rel))
	if filepath.IsAbs(cleanRel) || cleanRel == "." || cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(filepath.Separator)) {
		return "", 0, fmt.Errorf("invalid file path %q", rel)
	}
	absTarget := filepath.Join(absRoot, cleanRel)
	checkRel, err := filepath.Rel(absRoot, absTarget)
	if err != nil || checkRel == ".." || strings.HasPrefix(checkRel, ".."+string(filepath.Separator)) {
		return "", 0, fmt.Errorf("file path %q escapes includes directory", rel)
	}

	info, err := os.Stat(absTarget)
	if err != nil {
		return "", 0, fmt.Errorf("file %q not found", rel)
	}
	if info.IsDir() {
		return "", 0, fmt.Errorf("%q is a directory", rel)
	}
	if info.Size() > maxPreviewBytes {
		return "", info.Size(), fmt.Errorf("file %q (%s) exceeds %s preview limit", rel, formatHumanBytes(info.Size()), formatHumanBytes(maxPreviewBytes))
	}

	raw, err := os.ReadFile(absTarget)
	if err != nil {
		return "", info.Size(), err
	}
	return string(raw), info.Size(), nil
}

func (s *Server) renderPage(w http.ResponseWriter, name string, data any) {
	tmpl, ok := s.pages[name]
	if !ok {
		http.Error(w, "template not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := tmpl.ExecuteTemplate(w, "layout", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (s *Server) renderPartial(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := s.partials.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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

func formatTimestamp(raw string) string {
	t, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil {
		return raw
	}
	return t.Local().Format("2006-01-02 15:04:05")
}

func formatDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	mins := int(d.Minutes())
	secs := int(d.Seconds()) % 60
	return fmt.Sprintf("%dm%02ds", mins, secs)
}

func formatRelativeDuration(d time.Duration) string {
	if d <= 0 {
		return "now"
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm", int(d.Minutes()))
	}
	hours := int(d.Hours())
	mins := int(d.Minutes()) % 60
	if hours >= 24 {
		days := hours / 24
		remHours := hours % 24
		return fmt.Sprintf("%dd %dh", days, remHours)
	}
	return fmt.Sprintf("%dh %dm", hours, mins)
}

func formatHumanBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
