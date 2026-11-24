package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"freud/internal/cron"
	"freud/internal/db"
	"freud/internal/workflow"
)

var (
	// ErrFlowNotFound indicates the requested workflow file or name does not exist in flowsDir.
	ErrFlowNotFound = errors.New("workflow not found")
	// ErrInvalidFlow indicates an invalid flow identifier or workflow YAML parse/validation failure.
	ErrInvalidFlow = errors.New("invalid workflow")
	// ErrExecutionNotFound indicates no run exists for the given run ID.
	ErrExecutionNotFound = errors.New("execution not found")
	// ErrInvalidStatus indicates an unsupported execution status filter value.
	ErrInvalidStatus = errors.New("invalid status filter")
)

// FlowSummary is the shared DTO for flow listings in REST API and MCP.
type FlowSummary struct {
	File       string     `json:"file"`
	Name       string     `json:"name,omitempty"`
	Cron       string     `json:"cron,omitempty"`
	NextRun    string     `json:"next_run,omitempty"`
	TasksCount int        `json:"tasks_count"`
	ParseError string     `json:"parse_error,omitempty"`
	LastRun    *Execution `json:"last_run,omitempty"`
}

// FlowTask describes a parsed task node inside a workflow definition.
type FlowTask struct {
	ID           string   `json:"id"`
	Runtime      string   `json:"runtime,omitempty"`
	Image        string   `json:"image,omitempty"`
	Flow         string   `json:"flow,omitempty"`
	Wait         *bool    `json:"wait,omitempty"`
	DependsOn    []string `json:"depends_on,omitempty"`
	Dependencies []string `json:"dependencies,omitempty"`
}

// FlowDetail is the shared DTO for single-flow inspection in REST API and MCP.
type FlowDetail struct {
	File       string      `json:"file"`
	Name       string      `json:"name"`
	Cron       string      `json:"cron,omitempty"`
	NextRun    string      `json:"next_run,omitempty"`
	Tasks      []FlowTask  `json:"tasks"`
	RawYAML    string      `json:"raw_yaml"`
	RecentRuns []Execution `json:"recent_runs"`
}

// Execution is the shared DTO for workflow run metadata across REST API and MCP.
type Execution struct {
	ID           string `json:"id"`
	WorkflowName string `json:"workflow_name"`
	Status       string `json:"status"`
	TriggeredBy  string `json:"triggered_by,omitempty"`
	ParentRunID  string `json:"parent_run_id,omitempty"`
	ParentTaskID string `json:"parent_task_id,omitempty"`
	CreatedAt    string `json:"created_at"`
	FinishedAt   string `json:"finished_at,omitempty"`
}

// TaskRun is the shared DTO for single-task execution status and logs.
type TaskRun struct {
	TaskID     string `json:"task_id"`
	Status     string `json:"status"`
	Runtime    string `json:"runtime,omitempty"`
	Image      string `json:"image,omitempty"`
	ExitCode   *int64 `json:"exit_code,omitempty"`
	Logs       string `json:"logs,omitempty"`
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at,omitempty"`
}

// ExecutionDetail combines a run, its task runs, and any child subflow runs.
type ExecutionDetail struct {
	Run       Execution   `json:"run"`
	Tasks     []TaskRun   `json:"tasks"`
	ChildRuns []Execution `json:"child_runs"`
}

// AuditLog is the shared DTO for audit_logs entries across REST API and MCP.
type AuditLog struct {
	ID        int64  `json:"id"`
	Username  string `json:"username"`
	Action    string `json:"action"`
	Target    string `json:"target,omitempty"`
	Source    string `json:"source"`
	Details   string `json:"details,omitempty"`
	CreatedAt string `json:"created_at"`
}

// ToExecution converts a db.RunRecord into its public Execution DTO.
func ToExecution(r db.RunRecord) Execution {
	return Execution{
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

// ClampLimit bounds list limits to [1, maxVal] using def when non-positive.
func ClampLimit(val, def, maxVal int) int {
	if val <= 0 {
		return def
	}
	if val > maxVal {
		return maxVal
	}
	return val
}

// FindFlow loads and validates a workflow entry from flowsDir without permitting path traversal.
func FindFlow(flowsDir, target string, allowByName bool) (*workflow.WorkflowEntry, error) {
	target = strings.TrimSpace(target)
	if target == "" || strings.ContainsAny(target, "/\\") || target == "." || target == ".." {
		return nil, fmt.Errorf("%w: %q", ErrFlowNotFound, target)
	}
	entries, err := workflow.LoadDir(flowsDir)
	if err != nil {
		return nil, err
	}
	for i := range entries {
		if entries[i].FileName == target {
			if entries[i].ParseErr != nil {
				return nil, fmt.Errorf("%w %q: %v", ErrInvalidFlow, target, entries[i].ParseErr)
			}
			return &entries[i], nil
		}
	}
	if allowByName {
		for i := range entries {
			if entries[i].ParseErr == nil && entries[i].Workflow != nil && entries[i].Workflow.Name == target {
				return &entries[i], nil
			}
		}
	}
	return nil, fmt.Errorf("%w: %q", ErrFlowNotFound, target)
}

func nextCronUTC(expr string, now time.Time) string {
	if expr == "" {
		return ""
	}
	sched, err := cron.Parse(expr)
	if err != nil {
		return ""
	}
	next := sched.Next(now)
	if next.IsZero() {
		return ""
	}
	return next.UTC().Format(time.RFC3339)
}

// ListFlows returns all discovered workflows with cron schedules and latest run status.
func ListFlows(ctx context.Context, store *db.Store, flowsDir string) ([]FlowSummary, error) {
	entries, err := workflow.LoadDir(flowsDir)
	if err != nil {
		return nil, err
	}
	latestRuns, err := store.GetLatestRunByWorkflow(ctx)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	flows := make([]FlowSummary, 0, len(entries))
	for _, e := range entries {
		item := FlowSummary{File: e.FileName}
		if e.ParseErr != nil {
			item.ParseError = e.ParseErr.Error()
			flows = append(flows, item)
			continue
		}
		wf := e.Workflow
		item.Name = wf.Name
		item.Cron = wf.Cron
		item.TasksCount = len(wf.Tasks)
		item.NextRun = nextCronUTC(wf.Cron, now)
		if last, ok := latestRuns[wf.Name]; ok {
			v := ToExecution(last)
			item.LastRun = &v
		}
		flows = append(flows, item)
	}
	return flows, nil
}

// GetFlowDetail returns full workflow metadata, task graph definitions, raw YAML, and recent runs.
func GetFlowDetail(ctx context.Context, store *db.Store, flowsDir, target string, allowByName bool, recentLimit int) (*FlowDetail, error) {
	matched, err := FindFlow(flowsDir, target, allowByName)
	if err != nil {
		return nil, err
	}
	recentRaw, err := store.ListRunsByWorkflow(ctx, matched.Workflow.Name, ClampLimit(recentLimit, 20, 100))
	if err != nil {
		return nil, err
	}

	tasks := make([]FlowTask, 0, len(matched.Workflow.Tasks))
	for _, t := range matched.Workflow.Tasks {
		tasks = append(tasks, FlowTask{
			ID:           t.ID,
			Runtime:      t.Runtime,
			Image:        t.Image,
			Flow:         t.Flow,
			Wait:         t.Wait,
			DependsOn:    t.DependsOn,
			Dependencies: t.Dependencies,
		})
	}
	recent := make([]Execution, 0, len(recentRaw))
	for _, rec := range recentRaw {
		recent = append(recent, ToExecution(rec))
	}

	return &FlowDetail{
		File:       matched.FileName,
		Name:       matched.Workflow.Name,
		Cron:       matched.Workflow.Cron,
		NextRun:    nextCronUTC(matched.Workflow.Cron, time.Now()),
		Tasks:      tasks,
		RawYAML:    matched.RawYAML,
		RecentRuns: recent,
	}, nil
}

// ListExecutions validates statusFilter and returns aggregate stats alongside bounded execution rows.
func ListExecutions(ctx context.Context, store *db.Store, statusFilter string, limit, defLimit, maxLimit int) (db.RunStats, []Execution, error) {
	status := strings.ToUpper(strings.TrimSpace(statusFilter))
	switch status {
	case "", db.StatusRunning, db.StatusSuccess, db.StatusFailed, db.StatusKilled:
	default:
		return db.RunStats{}, nil, fmt.Errorf("%w: %q", ErrInvalidStatus, statusFilter)
	}

	stats, err := store.GetRunStats(ctx)
	if err != nil {
		return db.RunStats{}, nil, err
	}
	runsRaw, err := store.ListRuns(ctx, ClampLimit(limit, defLimit, maxLimit), status)
	if err != nil {
		return db.RunStats{}, nil, err
	}

	runs := make([]Execution, 0, len(runsRaw))
	for _, rec := range runsRaw {
		runs = append(runs, ToExecution(rec))
	}
	return stats, runs, nil
}

// GetExecutionDetail loads a run along with its task runs and child subflow runs.
func GetExecutionDetail(ctx context.Context, store *db.Store, runID string) (*ExecutionDetail, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return nil, fmt.Errorf("%w: empty run id", ErrExecutionNotFound)
	}
	runRec, err := store.GetRun(ctx, runID)
	if err != nil {
		if strings.Contains(err.Error(), "no rows") {
			return nil, fmt.Errorf("%w: %q", ErrExecutionNotFound, runID)
		}
		return nil, err
	}
	taskRecs, err := store.ListTaskRuns(ctx, runID)
	if err != nil {
		return nil, err
	}
	childRecs, err := store.ListChildRuns(ctx, runID)
	if err != nil {
		return nil, err
	}

	tasks := make([]TaskRun, 0, len(taskRecs))
	for _, tr := range taskRecs {
		var exitCode *int64
		if tr.ExitCode.Valid {
			v := tr.ExitCode.Int64
			exitCode = &v
		}
		tasks = append(tasks, TaskRun{
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
	children := make([]Execution, 0, len(childRecs))
	for _, cr := range childRecs {
		children = append(children, ToExecution(cr))
	}

	return &ExecutionDetail{
		Run:       ToExecution(*runRec),
		Tasks:     tasks,
		ChildRuns: children,
	}, nil
}

// ListAuditLogs returns bounded audit log DTOs filtered optionally by action.
func ListAuditLogs(ctx context.Context, store *db.Store, limit, defLimit, maxLimit int, actionFilter string) ([]AuditLog, error) {
	logsRaw, err := store.ListAuditLogs(ctx, ClampLimit(limit, defLimit, maxLimit), actionFilter)
	if err != nil {
		return nil, err
	}
	out := make([]AuditLog, 0, len(logsRaw))
	for _, l := range logsRaw {
		out = append(out, AuditLog{
			ID:        l.ID,
			Username:  l.Username,
			Action:    l.Action,
			Target:    l.Target.String,
			Source:    l.Source,
			Details:   l.Details.String,
			CreatedAt: l.CreatedAt,
		})
	}
	return out, nil
}
