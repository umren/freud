package mcp

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"

	"freud/internal/auth"
	"freud/internal/db"
	"freud/internal/syncer"
	"freud/internal/workflow"
)

const defaultProtocolVersion = "2025-03-26"

// FlowRunner starts workflows asynchronously (implemented by *engine.Engine).
type FlowRunner interface {
	StartAsync(ctx context.Context, wf *workflow.Workflow) (string, <-chan error, error)
}

// KillableFlowRunner optionally terminates currently running executions.
type KillableFlowRunner interface {
	Kill(runID string) error
}

// EnvKeyLister optionally exposes configured .env variable names (never values).
type EnvKeyLister interface {
	EnvKeys() []string
}

// Config configures the Freud MCP server.
type Config struct {
	FlowsDir     string
	IncludesDir  string
	Runner       FlowRunner
	Syncer       *syncer.Syncer
	AgentMadness bool
	EnvKeys      []string
	RunCtx       context.Context
	OnLog        func(format string, args ...any)
}

type actorCtxKey string

const mcpActorCtxKey actorCtxKey = "freud_mcp_actor"

// Actor identifies who invoked an MCP operation for triggered_by and audit logging.
type Actor struct {
	Username string
	Details  string
	IsAdmin  bool
	Scope    string
	HasToken bool
}

// WithActor binds an authenticated MCP actor and optional details (e.g. "token=ci") to ctx.
// Assumes local/full privileges; use WithAuth when enforcing RBAC and token scopes.
func WithActor(ctx context.Context, username, details string) context.Context {
	return WithAuth(ctx, username, details, true, auth.TokenScopeWrite, false)
}

// WithAuth binds an authenticated MCP actor together with RBAC and API token scope metadata.
func WithAuth(ctx context.Context, username, details string, isAdmin bool, scope string, hasToken bool) context.Context {
	return context.WithValue(ctx, mcpActorCtxKey, Actor{
		Username: strings.TrimSpace(username),
		Details:  strings.TrimSpace(details),
		IsAdmin:  isAdmin,
		Scope:    strings.TrimSpace(scope),
		HasToken: hasToken,
	})
}

func actorFromContext(ctx context.Context) Actor {
	if ctx != nil {
		if a, ok := ctx.Value(mcpActorCtxKey).(Actor); ok && a.Username != "" {
			return a
		}
	}
	return Actor{
		Username: auth.LocalActor(),
		Details:  "stdio",
		IsAdmin:  true,
		Scope:    auth.TokenScopeWrite,
		HasToken: false,
	}
}

// Server implements Model Context Protocol (MCP) JSON-RPC 2.0 over stdio and Streamable HTTP.
type Server struct {
	store  *db.Store
	cfg    Config
	runCtx context.Context
	wg     sync.WaitGroup
	tools  map[string]toolEntry
	order  []string
}

type toolEntry struct {
	def     ToolDefinition
	handler func(ctx context.Context, args json.RawMessage) (any, error)
}

// ToolDefinition describes an MCP tool exposed in tools/list.
type ToolDefinition struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	InputSchema map[string]any `json:"inputSchema"`
}

type jsonRPCRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type jsonRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Result  any             `json:"result,omitempty"`
	Error   *jsonRPCError   `json:"error,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

type toolCallResult struct {
	Content []textContent `json:"content"`
	IsError bool          `json:"isError,omitempty"`
}

type textContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// NewServer constructs an MCP server backed by Freud's state store and execution engine.
func NewServer(store *db.Store, cfg Config) *Server {
	if strings.TrimSpace(cfg.FlowsDir) == "" {
		cfg.FlowsDir = "flows"
	}
	if strings.TrimSpace(cfg.IncludesDir) == "" {
		cfg.IncludesDir = "includes"
	}
	runCtx := cfg.RunCtx
	if runCtx == nil {
		runCtx = context.Background()
	}
	s := &Server{
		store:  store,
		cfg:    cfg,
		runCtx: runCtx,
		tools:  make(map[string]toolEntry),
	}
	s.registerTools()
	return s
}

// Wait blocks until background runs launched via run_flow finish or runCtx is cancelled.
func (s *Server) Wait() {
	s.wg.Wait()
}

func (s *Server) registerTool(def ToolDefinition, handler func(ctx context.Context, args json.RawMessage) (any, error)) {
	s.tools[def.Name] = toolEntry{def: def, handler: handler}
	s.order = append(s.order, def.Name)
}

// ServeStdio reads newline-delimited JSON-RPC 2.0 requests from r and writes responses to w.
func (s *Server) ServeStdio(ctx context.Context, r io.Reader, w io.Writer) error {
	reader := bufio.NewReader(r)
	var writeMu sync.Mutex

	writeResp := func(resp *jsonRPCResponse) error {
		if resp == nil {
			return nil
		}
		data, err := json.Marshal(resp)
		if err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		_, err = w.Write(append(data, '\n'))
		return err
	}

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		line, err := reader.ReadBytes('\n')
		trimmed := bytes.TrimSpace(line)
		if len(trimmed) > 0 {
			resp := s.handleRawPayload(ctx, trimmed)
			for _, item := range resp {
				if writeErr := writeResp(item); writeErr != nil {
					return writeErr
				}
			}
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

// ServeHTTP handles MCP Streamable HTTP requests at /mcp.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		w.WriteHeader(http.StatusNoContent)
		return
	case http.MethodGet:
		w.Header().Set("Allow", "POST, DELETE")
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "SSE stream not offered; send JSON-RPC requests via POST /mcp"})
		return
	case http.MethodPost:
	default:
		w.Header().Set("Allow", "POST, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 4<<20))
	if err != nil {
		writeHTTPJSON(w, http.StatusBadRequest, s.errResp(nil, -32700, "failed to read request body"))
		return
	}
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) == 0 {
		writeHTTPJSON(w, http.StatusBadRequest, s.errResp(nil, -32600, "empty request body"))
		return
	}

	isBatch := trimmed[0] == '['
	responses := s.handleRawPayload(r.Context(), trimmed)
	if len(responses) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	if isBatch {
		writeHTTPJSON(w, http.StatusOK, responses)
		return
	}
	writeHTTPJSON(w, http.StatusOK, responses[0])
}

func writeHTTPJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func (s *Server) handleRawPayload(ctx context.Context, raw []byte) []*jsonRPCResponse {
	if raw[0] == '[' {
		var batch []json.RawMessage
		if err := json.Unmarshal(raw, &batch); err != nil || len(batch) == 0 {
			return []*jsonRPCResponse{s.errResp(nil, -32600, "invalid JSON-RPC batch")}
		}
		out := make([]*jsonRPCResponse, 0, len(batch))
		for _, item := range batch {
			if resp := s.handleSingleJSON(ctx, item); resp != nil {
				out = append(out, resp)
			}
		}
		return out
	}

	if resp := s.handleSingleJSON(ctx, raw); resp != nil {
		return []*jsonRPCResponse{resp}
	}
	return nil
}

func (s *Server) handleSingleJSON(ctx context.Context, raw []byte) *jsonRPCResponse {
	var req jsonRPCRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		return s.errResp(nil, -32700, fmt.Sprintf("parse error: %v", err))
	}
	if req.JSONRPC != "2.0" || strings.TrimSpace(req.Method) == "" {
		return s.errResp(req.ID, -32600, "invalid JSON-RPC 2.0 request")
	}

	isNotification := len(bytes.TrimSpace(req.ID)) == 0 || string(bytes.TrimSpace(req.ID)) == "null"

	// Notifications do not expect a JSON-RPC response.
	if strings.HasPrefix(req.Method, "notifications/") && isNotification {
		return nil
	}

	result, rpcErr := s.dispatch(ctx, req.Method, req.Params)
	if isNotification {
		return nil
	}
	if rpcErr != nil {
		return &jsonRPCResponse{
			JSONRPC: "2.0",
			ID:      req.ID,
			Error:   rpcErr,
		}
	}
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      req.ID,
		Result:  result,
	}
}

func (s *Server) errResp(id json.RawMessage, code int, msg string) *jsonRPCResponse {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	return &jsonRPCResponse{
		JSONRPC: "2.0",
		ID:      id,
		Error:   &jsonRPCError{Code: code, Message: msg},
	}
}

func (s *Server) dispatch(ctx context.Context, method string, params json.RawMessage) (any, *jsonRPCError) {
	switch method {
	case "initialize":
		var p struct {
			ProtocolVersion string `json:"protocolVersion"`
		}
		if len(params) > 0 {
			_ = json.Unmarshal(params, &p)
		}
		proto := defaultProtocolVersion
		switch p.ProtocolVersion {
		case "2024-11-05", "2025-03-26", "2025-06-18":
			proto = p.ProtocolVersion
		}
		return map[string]any{
			"protocolVersion": proto,
			"capabilities": map[string]any{
				"tools":     map[string]any{"listChanged": false},
				"resources": map[string]any{"subscribe": false, "listChanged": false},
			},
			"serverInfo": map[string]any{
				"name":    "freud",
				"version": "0.1.0",
			},
			"instructions": "Freud container workflow orchestrator. Read freud://docs/workflow-spec for YAML schema, call get_context for .env keys and installation status, inspect flows (list_flows, get_flow) and includes (list_includes, get_include), validate drafts (validate_flow), run/kill pipelines (run_flow, get_execution with tail_lines, kill_execution), sync Git repos (sync_flows), and check audit logs (list_audit_logs).",
		}, nil

	case "notifications/initialized":
		return map[string]any{}, nil

	case "ping":
		return map[string]any{}, nil

	case "tools/list":
		defs := make([]ToolDefinition, 0, len(s.order))
		for _, name := range s.order {
			defs = append(defs, s.tools[name].def)
		}
		return map[string]any{"tools": defs}, nil

	case "tools/call":
		var p toolCallParams
		if len(params) == 0 || json.Unmarshal(params, &p) != nil || strings.TrimSpace(p.Name) == "" {
			return nil, &jsonRPCError{Code: -32602, Message: "invalid tools/call params: name required"}
		}
		entry, ok := s.tools[p.Name]
		if !ok {
			return nil, &jsonRPCError{Code: -32601, Message: fmt.Sprintf("unknown tool %q", p.Name)}
		}
		out, err := entry.handler(ctx, p.Arguments)
		if err != nil {
			return toolCallResult{
				Content: []textContent{{Type: "text", Text: err.Error()}},
				IsError: true,
			}, nil
		}
		encoded, err := json.MarshalIndent(out, "", "  ")
		if err != nil {
			return nil, &jsonRPCError{Code: -32603, Message: err.Error()}
		}
		return toolCallResult{
			Content: []textContent{{Type: "text", Text: string(encoded)}},
		}, nil

	case "resources/list":
		resources, err := s.listResources()
		if err != nil {
			return nil, &jsonRPCError{Code: -32603, Message: err.Error()}
		}
		return map[string]any{"resources": resources}, nil

	case "resources/read":
		var p struct {
			URI string `json:"uri"`
		}
		if len(params) == 0 || json.Unmarshal(params, &p) != nil || strings.TrimSpace(p.URI) == "" {
			return nil, &jsonRPCError{Code: -32602, Message: "invalid resources/read params: uri required"}
		}
		contents, err := s.readResource(p.URI)
		if err != nil {
			return nil, &jsonRPCError{Code: -32602, Message: err.Error()}
		}
		return map[string]any{"contents": contents}, nil

	default:
		return nil, &jsonRPCError{Code: -32601, Message: fmt.Sprintf("method %q not found", method)}
	}
}
