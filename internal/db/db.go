package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const (
	StatusRunning = "RUNNING"
	StatusSuccess = "SUCCESS"
	StatusFailed  = "FAILED"
	StatusKilled  = "KILLED"
)

// RunRecord represents a row in the runs table.
type RunRecord struct {
	ID             string
	WorkflowName   string
	Status         string
	CreatedAt      string
	FinishedAt     sql.NullString
	DefinitionJSON sql.NullString
	ParentRunID    sql.NullString
	ParentTaskID   sql.NullString
}

const runSelectCols = `id, workflow_name, status, created_at, finished_at, definition_json, parent_run_id, parent_task_id`

// TaskRunRecord represents a row in the task_runs table.
type TaskRunRecord struct {
	ID         int64
	RunID      string
	TaskID     string
	Status     string
	ExitCode   sql.NullInt64
	Logs       sql.NullString
	StartedAt  string
	FinishedAt sql.NullString
	Runtime    sql.NullString
	Image      sql.NullString
}

// RunStats holds aggregate execution counts across all workflow runs.
type RunStats struct {
	Total   int
	Success int
	Failed  int
	Running int
	Killed  int
}

// Store wraps SQLite persistence for workflow runs and task runs.
type Store struct {
	db *sql.DB
}

// Open initializes the SQLite database, configures WAL mode and busy timeout, and runs migrations.
func Open(path string) (*Store, error) {
	sqlDB, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database %q: %w", path, err)
	}

	// Keep a single open connection so WAL and busy_timeout apply consistently.
	sqlDB.SetMaxOpenConns(1)

	if err := sqlDB.Ping(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("ping sqlite database %q: %w", path, err)
	}

	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA foreign_keys=ON;",
	}
	for _, p := range pragmas {
		if _, err := sqlDB.Exec(p); err != nil {
			_ = sqlDB.Close()
			return nil, fmt.Errorf("execute %q: %w", p, err)
		}
	}

	store := &Store{db: sqlDB}
	if err := store.migrate(); err != nil {
		_ = sqlDB.Close()
		return nil, fmt.Errorf("migrate database: %w", err)
	}

	return store, nil
}

// Close closes the underlying SQLite database connection.
func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) migrate() error {
	schema := `
CREATE TABLE IF NOT EXISTS runs (
	id TEXT PRIMARY KEY,
	workflow_name TEXT NOT NULL,
	status TEXT NOT NULL,
	created_at TEXT NOT NULL,
	finished_at TEXT,
	definition_json TEXT,
	parent_run_id TEXT,
	parent_task_id TEXT
);

CREATE TABLE IF NOT EXISTS task_runs (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	run_id TEXT NOT NULL,
	task_id TEXT NOT NULL,
	status TEXT NOT NULL,
	exit_code INTEGER,
	logs TEXT,
	started_at TEXT NOT NULL,
	finished_at TEXT,
	runtime TEXT,
	image TEXT,
	FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS idx_task_runs_run_id ON task_runs(run_id);
CREATE INDEX IF NOT EXISTS idx_runs_created_at ON runs(created_at DESC);
`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}

	alterations := []struct {
		table  string
		column string
		def    string
	}{
		{"runs", "definition_json", "TEXT"},
		{"runs", "parent_run_id", "TEXT"},
		{"runs", "parent_task_id", "TEXT"},
		{"task_runs", "runtime", "TEXT"},
		{"task_runs", "image", "TEXT"},
	}
	for _, alt := range alterations {
		if err := s.ensureColumn(alt.table, alt.column, alt.def); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) ensureColumn(table, column, colType string) error {
	rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%s);", table))
	if err != nil {
		return fmt.Errorf("inspect table %q: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name, ctype string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return fmt.Errorf("scan table info for %q: %w", table, err)
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate table info for %q: %w", table, err)
	}

	if _, err := s.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", table, column, colType)); err != nil {
		return fmt.Errorf("add column %s.%s: %w", table, column, err)
	}
	return nil
}

// CreateRun inserts a new workflow run in RUNNING state.
func (s *Store) CreateRun(ctx context.Context, runID, workflowName string) error {
	return s.CreateRunWithParent(ctx, runID, workflowName, "", "", "")
}

// CreateRunWithDefinition inserts a new workflow run in RUNNING state with a JSON workflow snapshot.
func (s *Store) CreateRunWithDefinition(ctx context.Context, runID, workflowName, definitionJSON string) error {
	return s.CreateRunWithParent(ctx, runID, workflowName, definitionJSON, "", "")
}

// CreateRunWithParent inserts a new workflow run with optional definition snapshot and parent link.
func (s *Store) CreateRunWithParent(ctx context.Context, runID, workflowName, definitionJSON, parentRunID, parentTaskID string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO runs (id, workflow_name, status, created_at, definition_json, parent_run_id, parent_task_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		runID, workflowName, StatusRunning, now, nullString(definitionJSON), nullString(parentRunID), nullString(parentTaskID),
	)
	if err != nil {
		return fmt.Errorf("insert run %q: %w", runID, err)
	}
	return nil
}

func nullString(v string) sql.NullString {
	if v == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: v, Valid: true}
}

// FinishRun updates the final status and completion timestamp of a workflow run.
func (s *Store) FinishRun(ctx context.Context, runID, status string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE runs SET status = ?, finished_at = ? WHERE id = ?`,
		status, now, runID,
	)
	if err != nil {
		return fmt.Errorf("update run %q: %w", runID, err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return fmt.Errorf("run %q not found", runID)
	}
	return nil
}

// CreateTaskRun inserts a new task execution record in RUNNING state and returns its ID.
func (s *Store) CreateTaskRun(ctx context.Context, runID, taskID string) (int64, error) {
	return s.CreateTaskRunWithMeta(ctx, runID, taskID, "", "")
}

// CreateTaskRunWithMeta inserts a new task execution record with runtime and image metadata.
func (s *Store) CreateTaskRunWithMeta(ctx context.Context, runID, taskID, runtimeName, imageName string) (int64, error) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var rt, img sql.NullString
	if runtimeName != "" {
		rt = sql.NullString{String: runtimeName, Valid: true}
	}
	if imageName != "" {
		img = sql.NullString{String: imageName, Valid: true}
	}
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO task_runs (run_id, task_id, status, started_at, runtime, image) VALUES (?, ?, ?, ?, ?, ?)`,
		runID, taskID, StatusRunning, now, rt, img,
	)
	if err != nil {
		return 0, fmt.Errorf("insert task run for task %q: %w", taskID, err)
	}
	id, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("retrieve task run id: %w", err)
	}
	return id, nil
}

// UpdateTaskRunLogs persists incremental log output while a task run is in progress.
func (s *Store) UpdateTaskRunLogs(ctx context.Context, taskRunID int64, logs string) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE task_runs SET logs = ? WHERE id = ?`,
		logs, taskRunID,
	)
	if err != nil {
		return fmt.Errorf("update task run %d logs: %w", taskRunID, err)
	}
	return nil
}

// FinishTaskRun records the final status, exit code, logs, and completion time of a task run.
func (s *Store) FinishTaskRun(ctx context.Context, taskRunID int64, status string, exitCode int64, logs string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(
		ctx,
		`UPDATE task_runs SET status = ?, exit_code = ?, logs = ?, finished_at = ? WHERE id = ?`,
		status, exitCode, logs, now, taskRunID,
	)
	if err != nil {
		return fmt.Errorf("update task run %d: %w", taskRunID, err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return fmt.Errorf("task run %d not found", taskRunID)
	}
	return nil
}

// GetRun retrieves a run record by ID.
func (s *Store) GetRun(ctx context.Context, runID string) (*RunRecord, error) {
	row := s.db.QueryRowContext(
		ctx,
		`SELECT `+runSelectCols+` FROM runs WHERE id = ?`,
		runID,
	)
	var rec RunRecord
	if err := row.Scan(
		&rec.ID,
		&rec.WorkflowName,
		&rec.Status,
		&rec.CreatedAt,
		&rec.FinishedAt,
		&rec.DefinitionJSON,
		&rec.ParentRunID,
		&rec.ParentTaskID,
	); err != nil {
		return nil, fmt.Errorf("get run %q: %w", runID, err)
	}
	return &rec, nil
}

// GetRunStats returns aggregate counts of runs by status.
func (s *Store) GetRunStats(ctx context.Context) (RunStats, error) {
	var stats RunStats
	row := s.db.QueryRowContext(ctx, `
		SELECT
			COUNT(*) AS total,
			COALESCE(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END), 0) AS success_cnt,
			COALESCE(SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END), 0) AS failed_cnt,
			COALESCE(SUM(CASE WHEN status = 'RUNNING' THEN 1 ELSE 0 END), 0) AS running_cnt,
			COALESCE(SUM(CASE WHEN status = 'KILLED' THEN 1 ELSE 0 END), 0) AS killed_cnt
		FROM runs
	`)
	if err := row.Scan(&stats.Total, &stats.Success, &stats.Failed, &stats.Running, &stats.Killed); err != nil {
		return stats, fmt.Errorf("query run stats: %w", err)
	}
	return stats, nil
}

// ListRuns returns runs ordered from newest to oldest, optionally filtered by status.
func (s *Store) ListRuns(ctx context.Context, limit int, statusFilter string) ([]RunRecord, error) {
	query := `SELECT ` + runSelectCols + ` FROM runs`
	var args []any
	if statusFilter != "" {
		query += ` WHERE status = ?`
		args = append(args, statusFilter)
	}
	query += ` ORDER BY created_at DESC`
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}

	return s.queryRuns(ctx, query, args...)
}

// ListRunsByWorkflow returns runs for a specific workflow ordered from newest to oldest.
func (s *Store) ListRunsByWorkflow(ctx context.Context, workflowName string, limit int) ([]RunRecord, error) {
	query := `SELECT ` + runSelectCols + ` FROM runs WHERE workflow_name = ? ORDER BY created_at DESC`
	args := []any{workflowName}
	if limit > 0 {
		query += ` LIMIT ?`
		args = append(args, limit)
	}
	return s.queryRuns(ctx, query, args...)
}

// ListChildRuns returns subflow runs triggered by parentRunID ordered chronologically.
func (s *Store) ListChildRuns(ctx context.Context, parentRunID string) ([]RunRecord, error) {
	query := `SELECT ` + runSelectCols + ` FROM runs WHERE parent_run_id = ? ORDER BY created_at ASC`
	return s.queryRuns(ctx, query, parentRunID)
}

// GetLatestRunByWorkflow returns the most recent RunRecord keyed by workflow_name.
func (s *Store) GetLatestRunByWorkflow(ctx context.Context) (map[string]RunRecord, error) {
	runs, err := s.queryRuns(ctx, `
		SELECT r.id, r.workflow_name, r.status, r.created_at, r.finished_at, r.definition_json, r.parent_run_id, r.parent_task_id
		FROM runs r
		INNER JOIN (
			SELECT workflow_name, MAX(created_at) AS max_created
			FROM runs
			GROUP BY workflow_name
		) latest ON r.workflow_name = latest.workflow_name AND r.created_at = latest.max_created
	`)
	if err != nil {
		return nil, err
	}
	res := make(map[string]RunRecord, len(runs))
	for _, r := range runs {
		res[r.WorkflowName] = r
	}
	return res, nil
}

func (s *Store) queryRuns(ctx context.Context, query string, args ...any) ([]RunRecord, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query runs: %w", err)
	}
	defer rows.Close()

	var records []RunRecord
	for rows.Next() {
		var rec RunRecord
		if err := rows.Scan(
			&rec.ID,
			&rec.WorkflowName,
			&rec.Status,
			&rec.CreatedAt,
			&rec.FinishedAt,
			&rec.DefinitionJSON,
			&rec.ParentRunID,
			&rec.ParentTaskID,
		); err != nil {
			return nil, fmt.Errorf("scan run: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate runs: %w", err)
	}
	return records, nil
}

// ListTaskRuns retrieves all task runs for a given run ID ordered by creation ID.
func (s *Store) ListTaskRuns(ctx context.Context, runID string) ([]TaskRunRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, run_id, task_id, status, exit_code, logs, started_at, finished_at, runtime, image FROM task_runs WHERE run_id = ? ORDER BY id ASC`,
		runID,
	)
	if err != nil {
		return nil, fmt.Errorf("list task runs for %q: %w", runID, err)
	}
	defer rows.Close()

	var records []TaskRunRecord
	for rows.Next() {
		var rec TaskRunRecord
		if err := rows.Scan(
			&rec.ID,
			&rec.RunID,
			&rec.TaskID,
			&rec.Status,
			&rec.ExitCode,
			&rec.Logs,
			&rec.StartedAt,
			&rec.FinishedAt,
			&rec.Runtime,
			&rec.Image,
		); err != nil {
			return nil, fmt.Errorf("scan task run: %w", err)
		}
		records = append(records, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate task runs: %w", err)
	}
	return records, nil
}
