package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"freud/internal/cron"
	"freud/internal/db"
	"freud/internal/engine"
	"freud/internal/workflow"
)

const (
	flowResourcePrefix = "freud://flows/"
	maxWaitSeconds     = 300
	defaultWaitSeconds = 60
)

type flowSummary struct {
	File       string     `json:"file"`
	Name       string     `json:"name,omitempty"`
	Cron       string     `json:"cron,omitempty"`
	NextRun    string     `json:"next_run,omitempty"`
	TasksCount int        `json:"tasks_count"`
	ParseError string     `json:"parse_error,omitempty"`
	LastRun    *runOutput `json:"last_run,omitempty"`
}

type runOutput struct {
	ID           string `json:"id"`
	WorkflowName string `json:"workflow_name"`
	Status       string `json:"status"`
	TriggeredBy  string `json:"triggered_by,omitempty"`
	ParentRunID  string `json:"parent_run_id,omitempty"`
	ParentTaskID string `json:"parent_task_id,omitempty"`
	CreatedAt    string `json:"created_at"`
	FinishedAt   string `json:"finished_at,omitempty"`
}

type taskOutput struct {
	TaskID     string `json:"task_id"`
	Status     string `json:"status"`
	Runtime    string `json:"runtime,omitempty"`
	Image      string `json:"image,omitempty"`
	ExitCode   *int64 `json:"exit_code,omitempty"`
	Logs       string `json:"logs,omitempty"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at,omitempty"`
}

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
		Description: "List workflow executions ordered newest-first with aggregate status statistics.",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"status": map[string]any{
					"type":        "string",
					"description": "Optional status filter: RUNNING, SUCCESS, FAILED, or KILLED.",
					"enum":        []string{"RUNNING", "SUCCESS", "FAILED", "KILLED"},
				},
				"limit": map[string]any{
					"type":        "integer",
					"description": "Maximum runs to return (default 20, max 100).",
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
		Description: "Inspect recent user and agent actions (FLOW_RUN, FLOW_KILL, LOGIN, TOKEN_CREATED, etc.).",
		InputSchema: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"action": map[string]any{
					"type":        "string",
					"description": "Optional exact action filter (e.g. FLOW_RUN, FLOW_KILL, LOGIN).",
				},
				"limit": map[string]any{
					"type":        "integer",
					"description": "Maximum audit entries to return (default 50, max 200).",
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

func toRunOutput(r db.RunRecord) runOutput {
	return runOutput{
		ID:           r.ID,
		WorkflowName: r.WorkflowName,
		Status:       r.Status,
		TriggeredBy:  r.TriggeredBy.String,
		ParentRunID:  r.ParentRunID.String,
		ParentTaskID: r.ParentTaskID.String,
		CreatedAt:    r.CreatedAt,
		FinishedAt:   r.FinishedAt.String,
	}
}

func (s *Server) findFlow(target string) (*workflow.WorkflowEntry, error) {
	target = strings.TrimSpace(target)
	if target == "" || strings.ContainsAny(target, "/\\") || target == "." || target == ".." {
		return nil, fmt.Errorf("invalid workflow file or name %q", target)
	}
	entries, err := workflow.LoadDir(s.cfg.FlowsDir)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		if entries[i].FileName == target {
			if entries[i].ParseErr != nil {
				return nil, fmt.Errorf("invalid workflow %q: %w", target, entries[i].ParseErr)
			}
			return &entries[i], nil
		}
	}
	for i := range entries {
		if entries[i].ParseErr == nil && entries[i].Workflow != nil && entries[i].Workflow.Name == target {
			return &entries[i], nil
		}
	}
	return nil, fmt.Errorf("workflow %q not found in %s", target, s.cfg.FlowsDir)
}

func (s *Server) toolListFlows(ctx context.Context, _ json.RawMessage) (any, error) {
	entries, err := workflow.LoadDir(s.cfg.FlowsDir)
	if err != nil {
		return nil, err
	}
	latestRuns, err := s.store.GetLatestRunByWorkflow(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	flows := make([]flowSummary, 0, len(entries))
	for _, e := range entries {
		item := flowSummary{File: e.FileName}
		if e.ParseErr != nil {
			item.ParseError = e.ParseErr.Error()
			flows = append(flows, item)
			continue
		}
		wf := e.Workflow
		item.Name = wf.Name
		item.Cron = wf.Cron
		item.TasksCount = len(wf.Tasks)
		if wf.Cron != "" {
			if sched, err := cron.Parse(wf.Cron); err == nil {
				if next := sched.Next(now); !next.IsZero() {
					item.NextRun = next.UTC().Format(time.RFC3339)
				}
			}
		}
		if last, ok := latestRuns[wf.Name]; ok {
			v := toRunOutput(last)
			item.LastRun = &v
		}
		flows = append(flows, item)
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
	matched, err := s.findFlow(args.File)
	if err != nil {
		return nil, err
	}
	recentRaw, err := s.store.ListRunsByWorkflow(ctx, matched.Workflow.Name, 10)
	if err != nil {
		return nil, err
	}
	recent := make([]runOutput, 0, len(recentRaw))
	for _, r := range recentRaw {
		recent = append(recent, toRunOutput(r))
	}
	return map[string]any{
		"file":        matched.FileName,
		"name":        matched.Workflow.Name,
		"cron":        matched.Workflow.Cron,
		"tasks":       matched.Workflow.Tasks,
		"raw_yaml":    matched.RawYAML,
		"recent_runs": recent,
	}, nil
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
	matched, err := s.findFlow(args.File)
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

	// Bridge doneCh so both Wait() and optional synchronous wait=true can observe completion.
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

	timeoutSec := args.TimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = defaultWaitSeconds
	}
	if timeoutSec > maxWaitSeconds {
		timeoutSec = maxWaitSeconds
	}
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

	detail, err := s.loadExecutionDetail(ctx, runID)
	if err != nil {
		return nil, err
	}
	detail["timed_out"] = timedOut
	return detail, nil
}

func (s *Server) toolListExecutions(ctx context.Context, raw json.RawMessage) (any, error) {
	var args struct {
		Status string `json:"status"`
		Limit  int    `json:"limit"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	status := strings.ToUpper(strings.TrimSpace(args.Status))
	switch status {
	case "", db.StatusRunning, db.StatusSuccess, db.StatusFailed, db.StatusKilled:
	default:
		return nil, fmt.Errorf("invalid status %q", args.Status)
	}
	limit := args.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	stats, err := s.store.GetRunStats(ctx)
	if err != nil {
		return nil, err
	}
	runsRaw, err := s.store.ListRuns(ctx, limit, status)
	if err != nil {
		return nil, err
	}
	runs := make([]runOutput, 0, len(runsRaw))
	for _, r := range runsRaw {
		runs = append(runs, toRunOutput(r))
	}
	return map[string]any{
		"stats": stats,
		"runs":  runs,
	}, nil
}

func (s *Server) loadExecutionDetail(ctx context.Context, runID string) (map[string]any, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("run_id is required")
	}
	runRec, err := s.store.GetRun(ctx, runID)
	if err != nil {
		return nil, fmt.Errorf("execution %q not found: %w", runID, err)
	}
	taskRecs, err := s.store.ListTaskRuns(ctx, runID)
	if err != nil {
		return nil, err
	}
	childRecs, err := s.store.ListChildRuns(ctx, runID)
	if err != nil {
		return nil, err
	}

	tasks := make([]taskOutput, 0, len(taskRecs))
	for _, tr := range taskRecs {
		var exitCode *int64
		if tr.ExitCode.Valid {
			v := tr.ExitCode.Int64
			exitCode = &v
		}
		tasks = append(tasks, taskOutput{
			TaskID:     tr.TaskID,
			Status:     tr.Status,
			Runtime:    tr.Runtime.String,
			Image:      tr.Image.String,
			ExitCode:   exitCode,
			Logs:       tr.Logs.String,
			StartedAt:  tr.StartedAt,
			FinishedAt: tr.FinishedAt.String,
		})
	}
	children := make([]runOutput, 0, len(childRecs))
	for _, cr := range childRecs {
		children = append(children, toRunOutput(cr))
	}
	return map[string]any{
		"run":        toRunOutput(*runRec),
		"tasks":      tasks,
		"child_runs": children,
	}, nil
}

func (s *Server) toolGetExecution(ctx context.Context, raw json.RawMessage) (any, error) {
	var args struct {
		RunID string `json:"run_id"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	return s.loadExecutionDetail(ctx, args.RunID)
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
		Limit  int    `json:"limit"`
	}
	if err := decodeArgs(raw, &args); err != nil {
		return nil, err
	}
	limit := args.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	logsRaw, err := s.store.ListAuditLogs(ctx, limit, args.Action)
	if err != nil {
		return nil, err
	}
	type auditOut struct {
		ID        int64  `json:"id"`
		Username  string `json:"username"`
		Action    string `json:"action"`
		Target    string `json:"target,omitempty"`
		Source    string `json:"source"`
		Details   string `json:"details,omitempty"`
		CreatedAt string `json:"created_at"`
	}
	out := make([]auditOut, 0, len(logsRaw))
	for _, l := range logsRaw {
		out = append(out, auditOut{
			ID:        l.ID,
			Username:  l.Username,
			Action:    l.Action,
			Target:    l.Target.String,
			Source:    l.Source,
			Details:   l.Details.String,
			CreatedAt: l.CreatedAt,
		})
	}
	return map[string]any{"logs": out}, nil
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
	matched, err := s.findFlow(fileName)
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
