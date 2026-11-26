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
	"freud/internal/syncer"
	"freud/internal/workflow"
)

const (
	flowResourcePrefix    = "freud://flows/"
	includeResourcePrefix = "freud://includes/"
	workflowSpecURI       = "freud://docs/workflow-spec"
	maxWaitSeconds        = 300
	defaultWaitSeconds    = 60
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
				"tail_lines": map[string]any{
					"type":        "integer",
					"description": "Optional limit to return only the last N log lines per task when wait=true.",
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
				"tail_lines": map[string]any{
					"type":        "integer",
					"description": "Optional: return only the last N log lines per task to keep context compact.",
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

	s.registerTool(ToolDefinition{
		Name:        "sync_flows",
		Description: "Trigger Git repository sync now to pull updated workflows and includes.",
		InputSchema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		},
	}, s.toolSyncFlows)

	s.registerTool(ToolDefinition{
		Name:        "list_includes",
		Description: "List shared helper files inside the includes directory (mounted read-only at /includes in containers).",
		InputSchema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		},
	}, s.toolListIncludes)

	s.registerTool(ToolDefinition{
		Name:        "get_include",
		Description: "Read the contents of a shared file from the includes directory (max 512 KiB).",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"path": map[string]any{
					"type":        "string",
					"description": "Relative path inside includes/ (e.g. payload_utils.py or includes/payload_utils.py).",
				},
			},
			"required": []string{"path"},
		},
	}, s.toolGetInclude)

	s.registerTool(ToolDefinition{
		Name:        "validate_flow",
		Description: "Parse and validate a workflow YAML draft (fields, cron expression, include paths, DAG cycles) without modifying files.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"yaml": map[string]any{
					"type":        "string",
					"description": "Raw workflow YAML text to validate.",
				},
			},
			"required": []string{"yaml"},
		},
	}, s.toolValidateFlow)

	s.registerTool(ToolDefinition{
		Name:        "get_context",
		Description: "Return installation context for agents: available .env variable NAMES only (never values), flows/includes counts, Git sync status, and workflow YAML spec resource URI.",
		InputSchema: map[string]any{
			"type":                 "object",
			"properties":           map[string]any{},
			"additionalProperties": false,
		},
	}, s.toolGetContext)
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
		TailLines      int    `json:"tail_lines"`
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
	service.ApplyTailLines(detail, args.TailLines)
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
		RunID     string `json:"run_id"`
		TailLines int    `json:"tail_lines"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	detail, err := service.GetExecutionDetail(ctx, s.store, args.RunID)
	if err != nil {
		return nil, err
	}
	service.ApplyTailLines(detail, args.TailLines)
	return detail, nil
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

func (s *Server) toolSyncFlows(ctx context.Context, _ json.RawMessage) (any, error) {
	if s.cfg.Syncer == nil || !s.cfg.Syncer.Enabled() {
		return nil, fmt.Errorf("git sync is not enabled")
	}
	actor := actorFromContext(ctx)
	res, err := s.cfg.Syncer.Sync(ctx, syncer.Options{
		Trigger:     "mcp",
		Actor:       actor.Username,
		Details:     actor.Details,
		ForceRecord: true,
	})
	if err != nil {
		if res != nil && res.RunID != "" {
			return nil, fmt.Errorf("%w (run_id=%s)", err, res.RunID)
		}
		return nil, err
	}
	return res, nil
}

func (s *Server) toolListIncludes(_ context.Context, _ json.RawMessage) (any, error) {
	files, err := service.ListIncludes(s.cfg.IncludesDir)
	if err != nil {
		return nil, err
	}
	return map[string]any{"includes": files}, nil
}

func (s *Server) toolGetInclude(_ context.Context, raw json.RawMessage) (any, error) {
	var args struct {
		Path string `json:"path"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	return service.ReadInclude(s.cfg.IncludesDir, args.Path)
}

func (s *Server) toolValidateFlow(_ context.Context, raw json.RawMessage) (any, error) {
	var args struct {
		YAML string `json:"yaml"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	if strings.TrimSpace(args.YAML) == "" {
		return nil, fmt.Errorf("yaml field is required")
	}
	wf, err := workflow.Parse([]byte(args.YAML))
	if err != nil {
		return map[string]any{
			"valid": false,
			"error": err.Error(),
		}, nil
	}
	return map[string]any{
		"valid":       true,
		"name":        wf.Name,
		"cron":        wf.Cron,
		"tasks_count": len(wf.Tasks),
	}, nil
}

func (s *Server) envKeys() []string {
	if lister, ok := s.cfg.Runner.(EnvKeyLister); ok {
		if keys := lister.EnvKeys(); len(keys) > 0 {
			return keys
		}
	}
	return s.cfg.EnvKeys
}

func (s *Server) toolGetContext(ctx context.Context, _ json.RawMessage) (any, error) {
	flows, err := service.ListFlows(ctx, s.store, s.cfg.FlowsDir)
	if err != nil {
		return nil, err
	}
	includes, err := service.ListIncludes(s.cfg.IncludesDir)
	if err != nil {
		return nil, err
	}
	var syncStatus any
	if s.cfg.Syncer != nil {
		syncStatus = s.cfg.Syncer.Status()
	}
	envKeys := s.envKeys()
	if envKeys == nil {
		envKeys = []string{}
	}
	return map[string]any{
		"env_keys":           envKeys,
		"flows_count":        len(flows),
		"includes_count":     len(includes),
		"sync":               syncStatus,
		"workflow_spec_uri":  workflowSpecURI,
		"supported_runtimes": []string{workflow.RuntimePython, workflow.RuntimeJS, workflow.RuntimeShell},
	}, nil
}

func (s *Server) listResources() ([]map[string]any, error) {
	out := []map[string]any{
		{
			"uri":         workflowSpecURI,
			"name":        "workflow-spec",
			"description": "Freud Workflow YAML Specification & container runtime conventions",
			"mimeType":    "text/markdown",
		},
	}

	entries, err := workflow.LoadDir(s.cfg.FlowsDir)
	if err != nil {
		return nil, err
	}
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

	includes, err := service.ListIncludes(s.cfg.IncludesDir)
	if err != nil {
		return nil, err
	}
	for _, inc := range includes {
		out = append(out, map[string]any{
			"uri":         includeResourcePrefix + inc.Path,
			"name":        "include:" + inc.Path,
			"description": fmt.Sprintf("Shared include file (%d bytes)", inc.Size),
			"mimeType":    "text/plain",
		})
	}
	return out, nil
}

func (s *Server) readResource(uri string) ([]map[string]any, error) {
	if uri == workflowSpecURI {
		return []map[string]any{
			{
				"uri":      workflowSpecURI,
				"mimeType": "text/markdown",
				"text":     workflow.SpecMarkdown,
			},
		}, nil
	}
	if strings.HasPrefix(uri, includeResourcePrefix) {
		rel := strings.TrimPrefix(uri, includeResourcePrefix)
		inc, err := service.ReadInclude(s.cfg.IncludesDir, rel)
		if err != nil {
			return nil, err
		}
		return []map[string]any{
			{
				"uri":      includeResourcePrefix + inc.Path,
				"mimeType": "text/plain",
				"text":     inc.Content,
			},
		}, nil
	}
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
