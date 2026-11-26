package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	// ErrUserExists is returned when attempting to create a duplicate username.
	ErrUserExists = errors.New("user already exists")
	// ErrLastAdmin is returned when attempting to delete the sole administrator account.
	ErrLastAdmin = errors.New("cannot delete the last admin account")
)

// UserRecord represents a row in the users table.
type UserRecord struct {
	ID           int64
	Username     string
	PasswordHash string
	IsAdmin      bool
	CreatedAt    string
}

// AuditLogRecord represents a row in the audit_logs table.
type AuditLogRecord struct {
	ID        int64
	Username  string
	Action    string
	Target    sql.NullString
	Source    string
	Details   sql.NullString
	CreatedAt string
}

// APITokenRecord represents a row in the api_tokens table (never storing plaintext tokens).
type APITokenRecord struct {
	ID         int64
	Username   string
	Name       string
	Prefix     string
	TokenHash  string
	Scope      string
	LastUsedAt sql.NullString
	CreatedAt  string
}

// HasAdmin reports whether at least one admin account exists.
func (s *Store) HasAdmin(ctx context.Context) (bool, error) {
	var cnt int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE is_admin = 1`).Scan(&cnt); err != nil {
		return false, fmt.Errorf("count admin users: %w", err)
	}
	return cnt > 0, nil
}

// CountUsers returns the total number of registered users.
func (s *Store) CountUsers(ctx context.Context) (int, error) {
	var cnt int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users`).Scan(&cnt); err != nil {
		return 0, fmt.Errorf("count users: %w", err)
	}
	return cnt, nil
}

// CreateUser inserts a new user account.
func (s *Store) CreateUser(ctx context.Context, username, passwordHash string, isAdmin bool) (*UserRecord, error) {
	username = strings.TrimSpace(username)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	adminFlag := 0
	if isAdmin {
		adminFlag = 1
	}

	_, err := s.db.ExecContext(
		ctx,
		s.rebind(`INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, ?, ?)`),
		username, passwordHash, adminFlag, now,
	)
	if err != nil {
		lower := strings.ToLower(err.Error())
		if strings.Contains(lower, "unique") || strings.Contains(lower, "duplicate") {
			return nil, fmt.Errorf("%w: %s", ErrUserExists, username)
		}
		return nil, fmt.Errorf("insert user %q: %w", username, err)
	}
	return s.GetUserByUsername(ctx, username)
}

// GetUserByUsername fetches a single user record by username.
func (s *Store) GetUserByUsername(ctx context.Context, username string) (*UserRecord, error) {
	var rec UserRecord
	var isAdmin int
	err := s.db.QueryRowContext(
		ctx,
		s.rebind(`SELECT id, username, password_hash, is_admin, created_at FROM users WHERE username = ?`),
		strings.TrimSpace(username),
	).Scan(&rec.ID, &rec.Username, &rec.PasswordHash, &isAdmin, &rec.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("get user %q: %w", username, err)
	}
	rec.IsAdmin = isAdmin != 0
	return &rec, nil
}

// ListUsers returns all users ordered alphabetically by username (admins first).
func (s *Store) ListUsers(ctx context.Context) ([]UserRecord, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, username, password_hash, is_admin, created_at FROM users ORDER BY is_admin DESC, username ASC`)
	if err != nil {
		return nil, fmt.Errorf("list users: %w", err)
	}
	defer rows.Close()

	var users []UserRecord
	for rows.Next() {
		var rec UserRecord
		var isAdmin int
		if err := rows.Scan(&rec.ID, &rec.Username, &rec.PasswordHash, &isAdmin, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan user: %w", err)
		}
		rec.IsAdmin = isAdmin != 0
		users = append(users, rec)
	}
	return users, rows.Err()
}

// UpdateUserPassword replaces the password hash for the named user.
func (s *Store) UpdateUserPassword(ctx context.Context, username, passwordHash string) error {
	res, err := s.db.ExecContext(
		ctx,
		s.rebind(`UPDATE users SET password_hash = ? WHERE username = ?`),
		passwordHash, strings.TrimSpace(username),
	)
	if err != nil {
		return fmt.Errorf("update password for %q: %w", username, err)
	}
	rows, err := res.RowsAffected()
	if err == nil && rows == 0 {
		return fmt.Errorf("user %q not found", username)
	}
	return nil
}

// DeleteUser removes a user account and clears its sessions, refusing to delete the last admin.
func (s *Store) DeleteUser(ctx context.Context, username string) error {
	username = strings.TrimSpace(username)
	u, err := s.GetUserByUsername(ctx, username)
	if err != nil {
		return err
	}
	if u.IsAdmin {
		var adminCount int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE is_admin = 1`).Scan(&adminCount); err != nil {
			return fmt.Errorf("count admins: %w", err)
		}
		if adminCount <= 1 {
			return ErrLastAdmin
		}
	}
	if _, err := s.db.ExecContext(ctx, s.rebind(`DELETE FROM api_tokens WHERE username = ?`), username); err != nil {
		return fmt.Errorf("delete user api tokens %q: %w", username, err)
	}
	if _, err := s.db.ExecContext(ctx, s.rebind(`DELETE FROM sessions WHERE username = ?`), username); err != nil {
		return fmt.Errorf("delete user sessions %q: %w", username, err)
	}
	if _, err := s.db.ExecContext(ctx, s.rebind(`DELETE FROM users WHERE username = ?`), username); err != nil {
		return fmt.Errorf("delete user %q: %w", username, err)
	}
	return nil
}

// CreateSession stores a persistent HTTP session token for username.
func (s *Store) CreateSession(ctx context.Context, token, username string, expiresAt time.Time) error {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	exp := expiresAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(
		ctx,
		s.rebind(`INSERT INTO sessions (token, username, expires_at, created_at) VALUES (?, ?, ?, ?)`),
		token, username, exp, now,
	)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	return nil
}

// GetSessionUser resolves an active (unexpired) session token to its UserRecord.
func (s *Store) GetSessionUser(ctx context.Context, token string) (*UserRecord, error) {
	var username, expiresRaw string
	err := s.db.QueryRowContext(
		ctx,
		s.rebind(`SELECT username, expires_at FROM sessions WHERE token = ?`),
		token,
	).Scan(&username, &expiresRaw)
	if err != nil {
		return nil, fmt.Errorf("get session: %w", err)
	}

	expiresAt, parseErr := time.Parse(time.RFC3339Nano, expiresRaw)
	if parseErr != nil || time.Now().UTC().After(expiresAt) {
		_ = s.DeleteSession(ctx, token)
		return nil, fmt.Errorf("session expired")
	}

	return s.GetUserByUsername(ctx, username)
}

// DeleteSession invalidates a session token.
func (s *Store) DeleteSession(ctx context.Context, token string) error {
	_, err := s.db.ExecContext(ctx, s.rebind(`DELETE FROM sessions WHERE token = ?`), token)
	return err
}

// RecordAudit writes an entry to audit_logs capturing who performed an action, where, and on what target.
func (s *Store) RecordAudit(ctx context.Context, username, action, target, source, details string) error {
	if strings.TrimSpace(username) == "" {
		username = "system"
	}
	if strings.TrimSpace(source) == "" {
		source = "system"
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(
		ctx,
		s.rebind(`INSERT INTO audit_logs (username, action, target, source, details, created_at) VALUES (?, ?, ?, ?, ?, ?)`),
		username, strings.ToUpper(strings.TrimSpace(action)), nullString(target), source, nullString(details), now,
	)
	if err != nil {
		return fmt.Errorf("record audit log: %w", err)
	}
	return nil
}

// AuditQuery configures filtering and pagination when listing audit_logs.
type AuditQuery struct {
	Action string
	Search string
	Limit  int
	Offset int
}

// ListAuditLogs returns audit log entries ordered from newest to oldest, optionally filtered by action.
func (s *Store) ListAuditLogs(ctx context.Context, limit int, actionFilter string) ([]AuditLogRecord, error) {
	logs, _, err := s.ListAuditLogsPage(ctx, AuditQuery{
		Action: actionFilter,
		Limit:  limit,
	})
	return logs, err
}

// ListAuditLogsPage returns matching audit_logs ordered newest-first alongside the total match count.
func (s *Store) ListAuditLogsPage(ctx context.Context, q AuditQuery) ([]AuditLogRecord, int, error) {
	var where []string
	var args []any

	if action := strings.ToUpper(strings.TrimSpace(q.Action)); action != "" {
		where = append(where, `action = ?`)
		args = append(args, action)
	}
	if strings.TrimSpace(q.Search) != "" {
		pat := likePattern(q.Search)
		where = append(where, `(LOWER(username) LIKE ? ESCAPE '!' OR LOWER(COALESCE(target, '')) LIKE ? ESCAPE '!' OR LOWER(source) LIKE ? ESCAPE '!' OR LOWER(COALESCE(details, '')) LIKE ? ESCAPE '!')`)
		args = append(args, pat, pat, pat, pat)
	}

	whereSQL := ""
	if len(where) > 0 {
		whereSQL = ` WHERE ` + strings.Join(where, ` AND `)
	}

	var total int
	if err := s.db.QueryRowContext(ctx, s.rebind(`SELECT COUNT(*) FROM audit_logs`+whereSQL), args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count audit logs: %w", err)
	}

	query := `SELECT id, username, action, target, source, details, created_at FROM audit_logs` + whereSQL + ` ORDER BY id DESC`
	queryArgs := append([]any(nil), args...)
	limit := q.Limit
	if q.Offset > 0 && limit <= 0 {
		limit = 200
	}
	if limit > 0 {
		query += ` LIMIT ?`
		queryArgs = append(queryArgs, limit)
		if q.Offset > 0 {
			query += ` OFFSET ?`
			queryArgs = append(queryArgs, q.Offset)
		}
	}

	rows, err := s.db.QueryContext(ctx, s.rebind(query), queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list audit logs: %w", err)
	}
	defer rows.Close()

	var logs []AuditLogRecord
	for rows.Next() {
		var rec AuditLogRecord
		if err := rows.Scan(&rec.ID, &rec.Username, &rec.Action, &rec.Target, &rec.Source, &rec.Details, &rec.CreatedAt); err != nil {
			return nil, 0, fmt.Errorf("scan audit log: %w", err)
		}
		logs = append(logs, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return logs, total, nil
}

// CreateAPIToken persists a hashed API token belonging to username with the default "run" scope.
func (s *Store) CreateAPIToken(ctx context.Context, username, name, prefix, tokenHash string) (*APITokenRecord, error) {
	return s.CreateAPITokenWithScope(ctx, username, name, prefix, tokenHash, "run")
}

// CreateAPITokenWithScope persists a hashed API token with an explicit permission scope ("read", "run", or "write").
func (s *Store) CreateAPITokenWithScope(ctx context.Context, username, name, prefix, tokenHash, scope string) (*APITokenRecord, error) {
	scope = strings.ToLower(strings.TrimSpace(scope))
	if scope == "" {
		scope = "run"
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, err := s.db.ExecContext(
		ctx,
		s.rebind(`INSERT INTO api_tokens (username, name, prefix, token_hash, scope, created_at) VALUES (?, ?, ?, ?, ?, ?)`),
		strings.TrimSpace(username), strings.TrimSpace(name), prefix, tokenHash, scope, now,
	)
	if err != nil {
		return nil, fmt.Errorf("create api token: %w", err)
	}

	var rec APITokenRecord
	err = s.db.QueryRowContext(
		ctx,
		s.rebind(`SELECT id, username, name, prefix, token_hash, COALESCE(scope, 'run'), last_used_at, created_at FROM api_tokens WHERE token_hash = ?`),
		tokenHash,
	).Scan(&rec.ID, &rec.Username, &rec.Name, &rec.Prefix, &rec.TokenHash, &rec.Scope, &rec.LastUsedAt, &rec.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("load created api token: %w", err)
	}
	return &rec, nil
}

// ListAPITokens returns stored API token metadata ordered newest-first; empty username lists all tokens.
func (s *Store) ListAPITokens(ctx context.Context, username string) ([]APITokenRecord, error) {
	query := `SELECT id, username, name, prefix, token_hash, COALESCE(scope, 'run'), last_used_at, created_at FROM api_tokens`
	var args []any
	if u := strings.TrimSpace(username); u != "" {
		query += ` WHERE username = ?`
		args = append(args, u)
	}
	query += ` ORDER BY id DESC`

	rows, err := s.db.QueryContext(ctx, s.rebind(query), args...)
	if err != nil {
		return nil, fmt.Errorf("list api tokens: %w", err)
	}
	defer rows.Close()

	var tokens []APITokenRecord
	for rows.Next() {
		var rec APITokenRecord
		if err := rows.Scan(&rec.ID, &rec.Username, &rec.Name, &rec.Prefix, &rec.TokenHash, &rec.Scope, &rec.LastUsedAt, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan api token: %w", err)
		}
		tokens = append(tokens, rec)
	}
	return tokens, rows.Err()
}

// GetUserByAPITokenHash resolves a SHA-256 token hash to its owning UserRecord and updates last_used_at.
func (s *Store) GetUserByAPITokenHash(ctx context.Context, tokenHash string) (*UserRecord, *APITokenRecord, error) {
	var rec APITokenRecord
	err := s.db.QueryRowContext(
		ctx,
		s.rebind(`SELECT id, username, name, prefix, token_hash, COALESCE(scope, 'run'), last_used_at, created_at FROM api_tokens WHERE token_hash = ?`),
		tokenHash,
	).Scan(&rec.ID, &rec.Username, &rec.Name, &rec.Prefix, &rec.TokenHash, &rec.Scope, &rec.LastUsedAt, &rec.CreatedAt)
	if err != nil {
		return nil, nil, fmt.Errorf("get api token: %w", err)
	}

	u, err := s.GetUserByUsername(ctx, rec.Username)
	if err != nil {
		return nil, nil, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)
	_, _ = s.db.ExecContext(ctx, s.rebind(`UPDATE api_tokens SET last_used_at = ? WHERE id = ?`), now, rec.ID)
	rec.LastUsedAt = sql.NullString{String: now, Valid: true}
	return u, &rec, nil
}

// DeleteAPIToken revokes a token by ID if owned by actorUsername or actorIsAdmin is true.
func (s *Store) DeleteAPIToken(ctx context.Context, id int64, actorUsername string, actorIsAdmin bool) (*APITokenRecord, error) {
	var rec APITokenRecord
	err := s.db.QueryRowContext(
		ctx,
		s.rebind(`SELECT id, username, name, prefix, token_hash, COALESCE(scope, 'run'), last_used_at, created_at FROM api_tokens WHERE id = ?`),
		id,
	).Scan(&rec.ID, &rec.Username, &rec.Name, &rec.Prefix, &rec.TokenHash, &rec.Scope, &rec.LastUsedAt, &rec.CreatedAt)
	if err != nil {
		return nil, err
	}
	if !actorIsAdmin && rec.Username != strings.TrimSpace(actorUsername) {
		return nil, fmt.Errorf("forbidden: cannot revoke another user's token")
	}
	if _, err := s.db.ExecContext(ctx, s.rebind(`DELETE FROM api_tokens WHERE id = ?`), id); err != nil {
		return nil, fmt.Errorf("delete api token %d: %w", id, err)
	}
	return &rec, nil
}
