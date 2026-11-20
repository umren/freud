package ui

import (
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"

	"freud/internal/db"
)

//go:embed static templates
var embeddedFS embed.FS

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
