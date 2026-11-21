package ui

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"sync"

	"freud/internal/db"
	"freud/internal/workflow"
)

//go:embed static templates
var embeddedFS embed.FS

// AsyncFlowRunner starts workflows asynchronously after synchronously creating the run record.
type AsyncFlowRunner interface {
	StartAsync(ctx context.Context, wf *workflow.Workflow) (string, <-chan error, error)
}

// Config configures the embedded web UI server.
type Config struct {
	Addr        string
	FlowsDir    string
	IncludesDir string
	SchedulerOn bool
	Runner      AsyncFlowRunner
	RunCtx      context.Context
}

// Server serves the embedded Freud web UI over HTTP.
type Server struct {
	store     *db.Store
	cfg       Config
	runner    AsyncFlowRunner
	runCtx    context.Context
	wg        sync.WaitGroup
	pages     map[string]*template.Template
	partials  *template.Template
	staticHnd http.Handler
}

type basePageData struct {
	Title        string
	Nav          string
	RunningCount int
	Addr         string
	SchedulerOn  bool
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
	runCtx := cfg.RunCtx
	if runCtx == nil {
		runCtx = context.Background()
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
		runner:    cfg.Runner,
		runCtx:    runCtx,
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
	mux.HandleFunc("POST /flows/{file}/run", s.handleRunFlow)
	mux.HandleFunc("GET /executions", s.handleExecutions)
	mux.HandleFunc("GET /partials/executions", s.handleExecutionsPartial)
	mux.HandleFunc("GET /executions/{id}", s.handleExecutionDetail)
	mux.HandleFunc("GET /partials/executions/{id}", s.handleExecutionDetailPartial)
	mux.HandleFunc("GET /includes", s.handleIncludes)
	return mux
}

// Wait blocks until all manually triggered UI runs finish.
func (s *Server) Wait() {
	s.wg.Wait()
}

func (s *Server) newBaseData(title, nav string, runningCount int) basePageData {
	return basePageData{
		Title:        title,
		Nav:          nav,
		RunningCount: runningCount,
		Addr:         s.cfg.Addr,
		SchedulerOn:  s.cfg.SchedulerOn,
	}
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
