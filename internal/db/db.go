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
)

// RunRecord represents a row in the runs table.
type RunRecord struct {
	ID           string
	WorkflowName string
	Status       string
	CreatedAt    string
	FinishedAt   sql.NullString
}

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
	finished_at TEXT
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
	FOREIGN KEY (run_id) REFERENCES runs(id)
);

CREATE INDEX IF NOT EXISTS idx_task_runs_run_id ON task_runs(run_id);
`
	if _, err := s.db.Exec(schema); err != nil {
		return fmt.Errorf("apply schema: %w", err)
	}
	return nil
}

// CreateRun inserts a new workflow run in RUNNING state.
func (s *Store) CreateRun(ctx context.Context, runID, workflowName string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(
		ctx,
		`INSERT INTO runs (id, workflow_name, status, created_at) VALUES (?, ?, ?, ?)`,
		runID, workflowName, StatusRunning, now,
	)
	if err != nil {
		return fmt.Errorf("insert run %q: %w", runID, err)
	}
	return nil
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
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(
		ctx,
		`INSERT INTO task_runs (run_id, task_id, status, started_at) VALUES (?, ?, ?, ?)`,
		runID, taskID, StatusRunning, now,
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
		`SELECT id, workflow_name, status, created_at, finished_at FROM runs WHERE id = ?`,
		runID,
	)
	var rec RunRecord
	if err := row.Scan(&rec.ID, &rec.WorkflowName, &rec.Status, &rec.CreatedAt, &rec.FinishedAt); err != nil {
		return nil, fmt.Errorf("get run %q: %w", runID, err)
	}
	return &rec, nil
}

// ListTaskRuns retrieves all task runs for a given run ID ordered by creation ID.
func (s *Store) ListTaskRuns(ctx context.Context, runID string) ([]TaskRunRecord, error) {
	rows, err := s.db.QueryContext(
		ctx,
		`SELECT id, run_id, task_id, status, exit_code, logs, started_at, finished_at FROM task_runs WHERE run_id = ? ORDER BY id ASC`,
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
