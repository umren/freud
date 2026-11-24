package ui

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"io/fs"
	"net/http"
	"sync"
	"time"

	"freud/internal/auth"
	"freud/internal/db"
	"freud/internal/workflow"
)

//go:embed static templates
var embeddedFS embed.FS

// AsyncFlowRunner starts workflows asynchronously after synchronously creating the run record.
type AsyncFlowRunner interface {
	StartAsync(ctx context.Context, wf *workflow.Workflow) (string, <-chan error, error)
}

// KillableFlowRunner is an optional capability for stopping currently running workflow executions.
type KillableFlowRunner interface {
	Kill(runID string) error
}

// Config configures the embedded web UI server.
type Config struct {
	Addr        string
	FlowsDir    string
	IncludesDir string
	SchedulerOn bool
	EnableAuth  bool
	Runner      AsyncFlowRunner
	RunCtx      context.Context
}

// Server serves the embedded Freud web UI over HTTP.
type Server struct {
	store     *db.Store
	cfg       Config
	runner    AsyncFlowRunner
	runCtx    context.Context
	limiter   *auth.LoginLimiter
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
	AuthEnabled  bool
	CurrentUser  *db.UserRecord
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
		"audit":            {"templates/layout.html", "templates/audit.html"},
		"setup":            {"templates/auth_layout.html", "templates/setup.html"},
		"login":            {"templates/auth_layout.html", "templates/login.html"},
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
		limiter:   auth.NewLoginLimiter(10, 5*time.Minute),
		pages:     pages,
		partials:  partials,
		staticHnd: http.StripPrefix("/static/", http.FileServer(http.FS(staticSub))),
	}, nil
}

// Handler returns the HTTP handler for the UI server.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /static/", s.staticHnd)
	mux.HandleFunc("GET /setup", s.handleSetupGet)
	mux.HandleFunc("POST /setup", s.handleSetupPost)
	mux.HandleFunc("GET /login", s.handleLoginGet)
	mux.HandleFunc("POST /login", s.handleLoginPost)
	mux.HandleFunc("POST /logout", s.handleLogout)
	mux.HandleFunc("GET /{$}", s.handleDashboard)
	mux.HandleFunc("GET /partials/dashboard", s.handleDashboardPartial)
	mux.HandleFunc("GET /flows", s.handleFlows)
	mux.HandleFunc("GET /flows/{file}", s.handleFlowDetail)
	mux.HandleFunc("POST /flows/{file}/run", s.handleRunFlow)
	mux.HandleFunc("GET /executions", s.handleExecutions)
	mux.HandleFunc("GET /partials/executions", s.handleExecutionsPartial)
	mux.HandleFunc("GET /executions/{id}", s.handleExecutionDetail)
	mux.HandleFunc("POST /executions/{id}/kill", s.handleKillExecution)
	mux.HandleFunc("GET /partials/executions/{id}", s.handleExecutionDetailPartial)
	mux.HandleFunc("GET /includes", s.handleIncludes)
	mux.HandleFunc("GET /audit", s.handleAuditPage)
	mux.HandleFunc("POST /users", s.handleCreateUser)
	mux.HandleFunc("POST /users/{username}/delete", s.handleDeleteUser)
	mux.HandleFunc("POST /account/password", s.handleChangePassword)
	mux.HandleFunc("POST /tokens", s.handleCreateAPIToken)
	mux.HandleFunc("POST /tokens/{id}/delete", s.handleDeleteAPIToken)

	// REST API v1 (JSON, Bearer token or session cookie auth)
	mux.HandleFunc("GET /api/v1/health", s.handleAPIHealth)
	mux.HandleFunc("GET /api/v1/me", s.handleAPIMe)
	mux.HandleFunc("GET /api/v1/flows", s.handleAPIFlows)
	mux.HandleFunc("GET /api/v1/flows/{file}", s.handleAPIFlowDetail)
	mux.HandleFunc("POST /api/v1/flows/{file}/run", s.handleAPIRunFlow)
	mux.HandleFunc("GET /api/v1/executions", s.handleAPIExecutions)
	mux.HandleFunc("GET /api/v1/executions/{id}", s.handleAPIExecutionDetail)
	mux.HandleFunc("POST /api/v1/executions/{id}/kill", s.handleAPIKillExecution)
	mux.HandleFunc("GET /api/v1/audit", s.handleAPIAudit)
	mux.HandleFunc("GET /api/v1/users", s.handleAPIUsersList)
	mux.HandleFunc("POST /api/v1/users", s.handleAPICreateUser)
	mux.HandleFunc("DELETE /api/v1/users/{username}", s.handleAPIDeleteUser)
	mux.HandleFunc("GET /api/v1/tokens", s.handleAPITokensList)
	mux.HandleFunc("POST /api/v1/tokens", s.handleAPICreateToken)
	mux.HandleFunc("DELETE /api/v1/tokens/{id}", s.handleAPIDeleteToken)
	return s.authMiddleware(mux)
}

// Wait blocks until all manually triggered UI runs finish.
func (s *Server) Wait() {
	s.wg.Wait()
}

func (s *Server) newBaseData(r *http.Request, title, nav string, runningCount int) basePageData {
	return basePageData{
		Title:        title,
		Nav:          nav,
		RunningCount: runningCount,
		Addr:         s.cfg.Addr,
		SchedulerOn:  s.cfg.SchedulerOn,
		AuthEnabled:  s.cfg.EnableAuth,
		CurrentUser:  userFromRequest(r),
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
