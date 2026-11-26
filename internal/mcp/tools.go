package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"freud/internal/db"
	"freud/internal/engine"
	"freud/internal/service"
	"freud/internal/workflow"
)

const (
	flowResourcePrefix = "freud://flows/"
	maxWaitSeconds     = 300
	defaultWaitSeconds = 60
)

func (s *Server) registerTools() {
	s.registerTool(ToolDefinition{
		Name:        "list_flows",
		Description: "List discovered YAML workflows with their cron schedule, task count, and latest execution status.",
		InputSchema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		},
	}, s.toolListFlows)

	s.registerTool(ToolDefinition{
		Name:        "get_flow",
		Description: "Inspect a workflow's tasks, dependencies, raw YAML definition, and recent execution history.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"file": map[string]any{
					"type":        "string",
					"description": "Workflow filename (e.g. etl-example.yaml) or workflow name.",
				},
			},
			"required": []string{"file"},
		},
	}, s.toolGetFlow)

	s.registerTool(ToolDefinition{
		Name:        "run_flow",
		Description: "Start a workflow execution. By default returns immediately with run_id; set wait=true to wait up to timeout_seconds and receive final status and task logs.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"file": map[string]any{
					"type":        "string",
					"description": "Workflow filename (e.g. etl-example.yaml) or workflow name.",
				},
				"wait": map[string]any{
					"type":        "boolean",
					"description": "Wait for workflow completion before returning (default false).",
				},
				"timeout_seconds": map[string]any{
					"type":        "integer",
					"description": "Maximum seconds to wait when wait=true (default 60, max 300).",
				},
			},
			"required": []string{"file"},
		},
	}, s.toolRunFlow)

	s.registerTool(ToolDefinition{
		Name:        "list_executions",
		Description: "List paginated workflow executions ordered newest-first with aggregate status statistics.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"status": map[string]any{
					"type":        "string",
					"description": "Optional status filter: RUNNING, SUCCESS, FAILED, or KILLED.",
					"enum":        []string{"RUNNING", "SUCCESS", "FAILED", "KILLED"},
				},
				"q": map[string]any{
					"type":        "string",
					"description": "Optional substring search across run_id, workflow_name, and triggered_by.",
				},
				"limit": map[string]any{
					"type":        "integer",
					"description": "Maximum runs to return (default 20, max 100).",
				},
				"offset": map[string]any{
					"type":        "integer",
					"description": "Number of matching runs to skip for pagination (default 0).",
				},
			},
		},
	}, s.toolListExecutions)

	s.registerTool(ToolDefinition{
		Name:        "get_execution",
		Description: "Get full execution status, triggered_by user, task runs with exit codes/logs, and child subflow runs.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"run_id": map[string]any{
					"type":        "string",
					"description": "Execution ID (e.g. 20261124-120000-ab12cd34).",
				},
			},
			"required": []string{"run_id"},
		},
	}, s.toolGetExecution)

	s.registerTool(ToolDefinition{
		Name:        "kill_execution",
		Description: "Terminate an active RUNNING workflow execution and record KILLED status in the audit log.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"run_id": map[string]any{
					"type":        "string",
					"description": "Execution ID to stop.",
				},
			},
			"required": []string{"run_id"},
		},
	}, s.toolKillExecution)

	s.registerTool(ToolDefinition{
		Name:        "list_audit_logs",
		Description: "Inspect paginated user and agent actions (FLOW_RUN, FLOW_KILL, LOGIN, TOKEN_CREATED, etc.).",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"action": map[string]any{
					"type":        "string",
					"description": "Optional exact action filter (e.g. FLOW_RUN, FLOW_KILL, LOGIN).",
				},
				"q": map[string]any{
					"type":        "string",
					"description": "Optional substring search across username, target, source, and details.",
				},
				"limit": map[string]any{
					"type":        "integer",
					"description": "Maximum audit entries to return (default 50, max 200).",
				},
				"offset": map[string]any{
					"type":        "integer",
					"description": "Number of matching entries to skip for pagination (default 0).",
				},
			},
		},
	}, s.toolListAuditLogs)
}

func decodeArgs(raw json.RawMessage, dst any) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return json.Unmarshal(raw, dst)
}

func (s *Server) toolListFlows(ctx context.Context, _ json.RawMessage) (any, error) {
	flows, err := service.ListFlows(ctx, s.store, s.cfg.FlowsDir)
	if err != nil {
		return nil, err
	}
	return map[string]any{"flows": flows}, nil
}

func (s *Server) toolGetFlow(ctx context.Context, raw json.RawMessage) (any, error) {
	var args struct {
		File string `json:"file"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	return service.GetFlowDetail(ctx, s.store, s.cfg.FlowsDir, args.File, true, 10)
}

func (s *Server) toolRunFlow(ctx context.Context, raw json.RawMessage) (any, error) {
	if s.cfg.Runner == nil {
		return nil, fmt.Errorf("execution runner is not configured")
	}
	var args struct {
		File           string `json:"file"`
		Wait           bool   `json:"wait"`
		TimeoutSeconds int    `json:"timeout_seconds"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	matched, err := service.FindFlow(s.cfg.FlowsDir, args.File, true)
	if err != nil {
		return nil, err
	}

	actor := actorFromContext(ctx)
	runCtx := engine.WithTriggeredBy(s.runCtx, actor.Username)
	runID, doneCh, err := s.cfg.Runner.StartAsync(runCtx, matched.Workflow)
	if err != nil {
		return nil, fmt.Errorf("start workflow %q: %w", matched.Workflow.Name, err)
	}

	details := "run_id=" + runID
	if actor.Details != "" {
		details += ", " + actor.Details
	}
	_ = s.store.RecordAudit(ctx, actor.Username, "FLOW_RUN", matched.Workflow.Name, "mcp", details)

	completed := make(chan error, 1)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		runErr := <-doneCh
		if runErr != nil && s.cfg.OnLog != nil {
			s.cfg.OnLog("MCP run %s (%s) failed: %v", runID, matched.Workflow.Name, runErr)
		}
		completed <- runErr
		close(completed)
	}()

	if !args.Wait {
		return map[string]any{
			"run_id":        runID,
			"workflow_name": matched.Workflow.Name,
			"status":        db.StatusRunning,
			"triggered_by":  actor.Username,
		}, nil
	}

	timeoutSec := service.ClampLimit(args.TimeoutSeconds, defaultWaitSeconds, maxWaitSeconds)
	timer := time.NewTimer(time.Duration(timeoutSec) * time.Second)
	defer timer.Stop()

	timedOut := false
	select {
	case <-completed:
	case <-timer.C:
		timedOut = true
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	detail, err := service.GetExecutionDetail(ctx, s.store, runID)
	if err != nil {
		return nil, err
	}
	return struct {
		*service.ExecutionDetail
		TimedOut bool `json:"timed_out"`
	}{
		ExecutionDetail: detail,
		TimedOut:        timedOut,
	}, nil
}

func (s *Server) toolListExecutions(ctx context.Context, raw json.RawMessage) (any, error) {
	var args struct {
		Status string `json:"status"`
		Query  string `json:"q"`
		Limit  int    `json:"limit"`
		Offset int    `json:"offset"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	return service.ListExecutions(ctx, s.store, args.Status, args.Query, args.Limit, args.Offset, 20, 100)
}

func (s *Server) toolGetExecution(ctx context.Context, raw json.RawMessage) (any, error) {
	var args struct {
		RunID string `json:"run_id"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	return service.GetExecutionDetail(ctx, s.store, args.RunID)
}

func (s *Server) toolKillExecution(ctx context.Context, raw json.RawMessage) (any, error) {
	kr, ok := s.cfg.Runner.(KillableFlowRunner)
	if !ok {
		return nil, fmt.Errorf("execution killer is not configured")
	}
	var args struct {
		RunID string `json:"run_id"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	runID := strings.TrimSpace(args.RunID)
	if runID == "" {
		return nil, fmt.Errorf("run_id is required")
	}
	if _, err := s.store.GetRun(ctx, runID); err != nil {
		return nil, fmt.Errorf("execution %q not found: %w", runID, err)
	}

	err := kr.Kill(runID)
	if err != nil && !errors.Is(err, engine.ErrRunNotActive) {
		return nil, fmt.Errorf("kill run %q: %w", runID, err)
	}
	actor := actorFromContext(ctx)
	if err == nil {
		_ = s.store.RecordAudit(ctx, actor.Username, "FLOW_KILL", runID, "mcp", actor.Details)
	}
	runRec, getErr := s.store.GetRun(ctx, runID)
	status := db.StatusKilled
	if getErr == nil {
		status = runRec.Status
	}
	return map[string]any{
		"run_id": runID,
		"status": status,
	}, nil
}

func (s *Server) toolListAuditLogs(ctx context.Context, raw json.RawMessage) (any, error) {
	var args struct {
		Action string `json:"action"`
		Query  string `json:"q"`
		Limit  int    `json:"limit"`
		Offset int    `json:"offset"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	return service.ListAuditLogs(ctx, s.store, args.Limit, args.Offset, 50, 200, args.Action, args.Query)
}

func (s *Server) listFlowResources() ([]map[string]any, error) {
	entries, err := workflow.LoadDir(s.cfg.FlowsDir)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(entries))
	for _, e := range entries {
		if e.ParseErr != nil || e.Workflow == nil {
			continue
		}
		out = append(out, map[string]any{
			"uri":         flowResourcePrefix + e.FileName,
			"name":        e.Workflow.Name,
			"description": fmt.Sprintf("Workflow YAML (%s, %d task(s))", e.FileName, len(e.Workflow.Tasks)),
			"mimeType":    "text/yaml",
		})
	}
	return out, nil
}

func (s *Server) readFlowResource(uri string) ([]map[string]any, error) {
	if !strings.HasPrefix(uri, flowResourcePrefix) {
		return nil, fmt.Errorf("unsupported resource URI %q", uri)
	}
	fileName := strings.TrimPrefix(uri, flowResourcePrefix)
	matched, err := service.FindFlow(s.cfg.FlowsDir, fileName, false)
	if err != nil {
		return nil, err
	}
	return []map[string]any{
		{
			"uri":      flowResourcePrefix + matched.FileName,
			"mimeType": "text/yaml",
			"text":     matched.RawYAML,
		},
	}, nil
}
