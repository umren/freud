package db

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

// StorageCounts captures row counts and SQLite file footprint for the maintenance overview.
type StorageCounts struct {
	Dialect     Dialect `json:"dialect"`
	Runs        int     `json:"runs"`
	TaskRuns    int     `json:"task_runs"`
	AuditLogs   int     `json:"audit_logs"`
	Sessions    int     `json:"sessions"`
	DBFileBytes int64   `json:"db_file_bytes"`
}

// RunRetentionMeta carries lightweight run metadata needed for safe age-based retention checks.
type RunRetentionMeta struct {
	ID         string
	Status     string
	CreatedAt  string
	FinishedAt string
}

// GetSettingsByPrefix returns all persisted key-value pairs whose key starts with prefix.
func (s *Store) GetSettingsByPrefix(ctx context.Context, prefix string) (map[string]string, error) {
	rows, err := s.db.QueryContext(
		ctx,
		s.rebind(`SELECT key_name, val FROM settings WHERE key_name LIKE ? ESCAPE '!'`),
		strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(prefix)+"%",
	)
	if err != nil {
		return nil, fmt.Errorf("query settings: %w", err)
	}
	defer rows.Close()

	out := make(map[string]string)
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, fmt.Errorf("scan setting: %w", err)
		}
		out[k] = v
	}
	return out, rows.Err()
}

// UpsertSetting writes or updates a setting row across SQLite, PostgreSQL, and MySQL/MariaDB.
func (s *Store) UpsertSetting(ctx context.Context, key, val string) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	query := `INSERT INTO settings (key_name, val, updated_at) VALUES (?, ?, ?)
		ON CONFLICT(key_name) DO UPDATE SET val = excluded.val, updated_at = excluded.updated_at`
	if s.dialect == DialectMySQL {
		query = `INSERT INTO settings (key_name, val, updated_at) VALUES (?, ?, ?)
			ON DUPLICATE KEY UPDATE val = VALUES(val), updated_at = VALUES(updated_at)`
	}
	if _, err := s.db.ExecContext(ctx, s.rebind(query), key, val, now); err != nil {
		return fmt.Errorf("upsert setting %q: %w", key, err)
	}
	return nil
}

// DeleteSettingsByPrefix removes persisted settings matching prefix (used to revert to config defaults).
func (s *Store) DeleteSettingsByPrefix(ctx context.Context, prefix string) error {
	_, err := s.db.ExecContext(
		ctx,
		s.rebind(`DELETE FROM settings WHERE key_name LIKE ? ESCAPE '!'`),
		strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(prefix)+"%",
	)
	if err != nil {
		return fmt.Errorf("delete settings %q: %w", prefix, err)
	}
	return nil
}

// GetStorageCounts returns table row counts and (for file-backed SQLite) total DB/WAL/SHM bytes on disk.
func (s *Store) GetStorageCounts(ctx context.Context) (StorageCounts, error) {
	sc := StorageCounts{Dialect: s.dialect}
	tables := []struct {
		query string
		dst   *int
	}{
		{`SELECT COUNT(*) FROM runs`, &sc.Runs},
		{`SELECT COUNT(*) FROM task_runs`, &sc.TaskRuns},
		{`SELECT COUNT(*) FROM audit_logs`, &sc.AuditLogs},
		{`SELECT COUNT(*) FROM sessions`, &sc.Sessions},
	}
	for _, t := range tables {
		if err := s.db.QueryRowContext(ctx, t.query).Scan(t.dst); err != nil {
			return sc, fmt.Errorf("count %s: %w", t.query, err)
		}
	}

	if s.sqlitePath != "" {
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if info, err := os.Stat(s.sqlitePath + suffix); err == nil {
				sc.DBFileBytes += info.Size()
			}
		}
	}
	return sc, nil
}

// ListRunRetentionMeta returns lightweight run metadata ordered oldest-first for cleanup evaluation.
func (s *Store) ListRunRetentionMeta(ctx context.Context) ([]RunRetentionMeta, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, status, created_at, COALESCE(finished_at, '') FROM runs ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("list run retention metadata: %w", err)
	}
	defer rows.Close()

	var out []RunRetentionMeta
	for rows.Next() {
		var m RunRetentionMeta
		if err := rows.Scan(&m.ID, &m.Status, &m.CreatedAt, &m.FinishedAt); err != nil {
			return nil, fmt.Errorf("scan run retention metadata: %w", err)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// DeleteRunsByIDs deletes the specified runs along with their child task_runs inside a transaction.
func (s *Store) DeleteRunsByIDs(ctx context.Context, runIDs []string) (deletedRuns int64, deletedTasks int64, err error) {
	if len(runIDs) == 0 {
		return 0, 0, nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, 0, fmt.Errorf("begin delete runs tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	delTaskStmt, err := tx.PrepareContext(ctx, s.rebind(`DELETE FROM task_runs WHERE run_id = ?`))
	if err != nil {
		return 0, 0, err
	}
	defer delTaskStmt.Close()

	delRunStmt, err := tx.PrepareContext(ctx, s.rebind(`DELETE FROM runs WHERE id = ?`))
	if err != nil {
		return 0, 0, err
	}
	defer delRunStmt.Close()

	for _, id := range runIDs {
		resT, err := delTaskStmt.ExecContext(ctx, id)
		if err != nil {
			return 0, 0, fmt.Errorf("delete task_runs for %q: %w", id, err)
		}
		if n, _ := resT.RowsAffected(); n > 0 {
			deletedTasks += n
		}

		resR, err := delRunStmt.ExecContext(ctx, id)
		if err != nil {
			return 0, 0, fmt.Errorf("delete run %q: %w", id, err)
		}
		if n, _ := resR.RowsAffected(); n > 0 {
			deletedRuns += n
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, 0, fmt.Errorf("commit delete runs tx: %w", err)
	}
	return deletedRuns, deletedTasks, nil
}

// DeleteExpiredSessions deletes session rows whose expires_at timestamp is before now.
func (s *Store) DeleteExpiredSessions(ctx context.Context, now time.Time) (int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT token, expires_at FROM sessions`)
	if err != nil {
		return 0, fmt.Errorf("list sessions for cleanup: %w", err)
	}
	var expired []string
	for rows.Next() {
		var tok, expRaw string
		if err := rows.Scan(&tok, &expRaw); err != nil {
			_ = rows.Close()
			return 0, err
		}
		exp, parseErr := time.Parse(time.RFC3339Nano, expRaw)
		if parseErr != nil || now.After(exp) {
			expired = append(expired, tok)
		}
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(expired) == 0 {
		return 0, nil
	}

	var deleted int64
	for _, tok := range expired {
		res, err := s.db.ExecContext(ctx, s.rebind(`DELETE FROM sessions WHERE token = ?`), tok)
		if err != nil {
			return deleted, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			deleted += n
		}
	}
	return deleted, nil
}

// PruneAuditLogsBefore removes audit_logs entries strictly older than cutoff.
func (s *Store) PruneAuditLogsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, created_at FROM audit_logs ORDER BY id ASC`)
	if err != nil {
		return 0, fmt.Errorf("query audit_logs for cleanup: %w", err)
	}
	var ids []int64
	for rows.Next() {
		var id int64
		var createdRaw string
		if err := rows.Scan(&id, &createdRaw); err != nil {
			_ = rows.Close()
			return 0, err
		}
		ts, parseErr := time.Parse(time.RFC3339Nano, createdRaw)
		if parseErr != nil {
			continue
		}
		if !ts.Before(cutoff) {
			// id increases monotonically with created_at, so we can stop scanning once caught up.
			break
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(ids) == 0 {
		return 0, nil
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.PrepareContext(ctx, s.rebind(`DELETE FROM audit_logs WHERE id = ?`))
	if err != nil {
		return 0, err
	}
	defer stmt.Close()

	var deleted int64
	for _, id := range ids {
		res, err := stmt.ExecContext(ctx, id)
		if err != nil {
			return 0, err
		}
		if n, _ := res.RowsAffected(); n > 0 {
			deleted += n
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return deleted, nil
}

// Vacuum compacts the database file when running on SQLite.
func (s *Store) Vacuum(ctx context.Context) error {
	if s.dialect != DialectSQLite {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `VACUUM`); err != nil {
		return fmt.Errorf("sqlite vacuum: %w", err)
	}
	return nil
}
