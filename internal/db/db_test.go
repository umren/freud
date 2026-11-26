package db

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStoreLifecycleAndPragmas(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")
	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	var journalMode string
	if err := store.db.QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if !strings.EqualFold(journalMode, "wal") {
		t.Fatalf("expected WAL journal_mode, got %q", journalMode)
	}

	var busyTimeout int
	if err := store.db.QueryRow("PRAGMA busy_timeout;").Scan(&busyTimeout); err != nil {
		t.Fatalf("query busy_timeout: %v", err)
	}
	if busyTimeout != 5000 {
		t.Fatalf("expected busy_timeout 5000, got %d", busyTimeout)
	}

	ctx := context.Background()
	runID := "run-123"
	if err := store.CreateRun(ctx, runID, "demo-wf"); err != nil {
		t.Fatalf("CreateRun failed: %v", err)
	}

	taskRunID, err := store.CreateTaskRun(ctx, runID, "extract")
	if err != nil {
		t.Fatalf("CreateTaskRun failed: %v", err)
	}

	if err := store.FinishTaskRun(ctx, taskRunID, StatusSuccess, 0, "hello logs"); err != nil {
		t.Fatalf("FinishTaskRun failed: %v", err)
	}

	if err := store.FinishRun(ctx, runID, StatusSuccess); err != nil {
		t.Fatalf("FinishRun failed: %v", err)
	}

	runRec, err := store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("GetRun failed: %v", err)
	}
	if runRec.Status != StatusSuccess || !runRec.FinishedAt.Valid {
		t.Fatalf("unexpected run record: %+v", runRec)
	}

	taskRecs, err := store.ListTaskRuns(ctx, runID)
	if err != nil {
		t.Fatalf("ListTaskRuns failed: %v", err)
	}
	if len(taskRecs) != 1 {
		t.Fatalf("expected 1 task run, got %d", len(taskRecs))
	}
	if taskRecs[0].Logs.String != "hello logs" || taskRecs[0].ExitCode.Int64 != 0 {
		t.Fatalf("unexpected task run record: %+v", taskRecs[0])
	}
}

func TestStoreQueriesAndIncrementalLogs(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "queries.db"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.CreateRunWithDefinition(ctx, "r1", "wf-a", `{"name":"wf-a"}`); err != nil {
		t.Fatalf("CreateRunWithDefinition r1: %v", err)
	}
	tr1, err := store.CreateTaskRunWithMeta(ctx, "r1", "t1", "python", "python:3.13-slim")
	if err != nil {
		t.Fatalf("CreateTaskRunWithMeta: %v", err)
	}
	if err := store.UpdateTaskRunLogs(ctx, tr1, "partial line 1\n"); err != nil {
		t.Fatalf("UpdateTaskRunLogs: %v", err)
	}
	tasks, err := store.ListTaskRuns(ctx, "r1")
	if err != nil || len(tasks) != 1 || tasks[0].Logs.String != "partial line 1\n" {
		t.Fatalf("expected incremental log update, got %+v, err=%v", tasks, err)
	}
	if err := store.FinishTaskRun(ctx, tr1, StatusSuccess, 0, "partial line 1\ndone\n"); err != nil {
		t.Fatalf("FinishTaskRun: %v", err)
	}
	if err := store.FinishRun(ctx, "r1", StatusSuccess); err != nil {
		t.Fatalf("FinishRun r1: %v", err)
	}

	if err := store.CreateRun(ctx, "r2", "wf-a"); err != nil {
		t.Fatalf("CreateRun r2: %v", err)
	}
	if err := store.FinishRun(ctx, "r2", StatusFailed); err != nil {
		t.Fatalf("FinishRun r2: %v", err)
	}

	if err := store.CreateRun(ctx, "r3", "wf-b"); err != nil {
		t.Fatalf("CreateRun r3: %v", err)
	}

	stats, err := store.GetRunStats(ctx)
	if err != nil {
		t.Fatalf("GetRunStats: %v", err)
	}
	if stats.Total != 3 || stats.Success != 1 || stats.Failed != 1 || stats.Running != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}

	allRuns, err := store.ListRuns(ctx, 10, "")
	if err != nil || len(allRuns) != 3 || allRuns[0].ID != "r3" || allRuns[2].ID != "r1" {
		t.Fatalf("expected newest-first order [r3, r2, r1], got %+v, err=%v", allRuns, err)
	}

	failedRuns, err := store.ListRuns(ctx, 10, StatusFailed)
	if err != nil || len(failedRuns) != 1 || failedRuns[0].ID != "r2" {
		t.Fatalf("unexpected failed filter result: %+v, err=%v", failedRuns, err)
	}

	pageRuns, totalRuns, err := store.ListRunsPage(ctx, RunQuery{Limit: 1, Offset: 1})
	if err != nil || totalRuns != 3 || len(pageRuns) != 1 || pageRuns[0].ID != "r2" {
		t.Fatalf("unexpected ListRunsPage offset result: runs=%+v total=%d err=%v", pageRuns, totalRuns, err)
	}
	searchRuns, totalSearch, err := store.ListRunsPage(ctx, RunQuery{Search: "wf-b", Limit: 10})
	if err != nil || totalSearch != 1 || len(searchRuns) != 1 || searchRuns[0].ID != "r3" {
		t.Fatalf("unexpected ListRunsPage search result: runs=%+v total=%d err=%v", searchRuns, totalSearch, err)
	}

	latest, err := store.GetLatestRunByWorkflow(ctx)
	if err != nil {
		t.Fatalf("GetLatestRunByWorkflow: %v", err)
	}
	if latest["wf-a"].ID != "r2" || latest["wf-b"].ID != "r3" {
		t.Fatalf("unexpected latest map: %+v", latest)
	}
}

func TestStoreParentChildRunsAndKilledStats(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "subflows.db"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.CreateRun(ctx, "parent-1", "main-flow"); err != nil {
		t.Fatalf("CreateRun parent-1: %v", err)
	}
	if err := store.CreateRunWithParent(ctx, "child-1", "sub-flow", "", "parent-1", "call_sub"); err != nil {
		t.Fatalf("CreateRunWithParent: %v", err)
	}
	if err := store.FinishRun(ctx, "child-1", StatusKilled); err != nil {
		t.Fatalf("FinishRun child-1: %v", err)
	}
	if err := store.FinishRun(ctx, "parent-1", StatusKilled); err != nil {
		t.Fatalf("FinishRun parent-1: %v", err)
	}

	children, err := store.ListChildRuns(ctx, "parent-1")
	if err != nil || len(children) != 1 {
		t.Fatalf("expected 1 child run, got %+v, err=%v", children, err)
	}
	if children[0].ID != "child-1" || children[0].ParentTaskID.String != "call_sub" {
		t.Fatalf("unexpected child record: %+v", children[0])
	}

	byWF, err := store.ListRunsByWorkflow(ctx, "sub-flow", 10)
	if err != nil || len(byWF) != 1 || byWF[0].ID != "child-1" {
		t.Fatalf("ListRunsByWorkflow mismatch: %+v, err=%v", byWF, err)
	}

	stats, err := store.GetRunStats(ctx)
	if err != nil {
		t.Fatalf("GetRunStats: %v", err)
	}
	if stats.Total != 2 || stats.Killed != 2 {
		t.Fatalf("expected Total=2 Killed=2, got %+v", stats)
	}
}

func TestDetectDialectAndRebind(t *testing.T) {
	if got := DetectDialect("orchestrator.db"); got != DialectSQLite {
		t.Fatalf("expected sqlite for filename, got %s", got)
	}
	if got := DetectDialect(":memory:"); got != DialectSQLite {
		t.Fatalf("expected sqlite for :memory:, got %s", got)
	}
	if got := DetectDialect("postgres://user:pass@localhost:5432/freud?sslmode=disable"); got != DialectPostgres {
		t.Fatalf("expected postgres for postgres://, got %s", got)
	}
	if got := DetectDialect("POSTGRESQL://localhost/freud"); got != DialectPostgres {
		t.Fatalf("expected postgres for POSTGRESQL://, got %s", got)
	}
	if got := DetectDialect("mysql://user:pass@localhost:3306/freud"); got != DialectMySQL {
		t.Fatalf("expected mysql for mysql://, got %s", got)
	}
	if got := DetectDialect("mariadb://user:pass@localhost:3306/freud"); got != DialectMySQL {
		t.Fatalf("expected mysql for mariadb://, got %s", got)
	}
	if got := DetectDialect("user:pass@tcp(127.0.0.1:3306)/freud"); got != DialectMySQL {
		t.Fatalf("expected mysql for native @tcp DSN, got %s", got)
	}

	norm, err := normalizeMySQLDSN("mariadb://u:p@db.local/freud?tls=skip-verify")
	if err != nil {
		t.Fatalf("normalizeMySQLDSN failed: %v", err)
	}
	if !strings.Contains(norm, "u:p@tcp(db.local:3306)/freud") || !strings.Contains(norm, "charset=utf8mb4") || !strings.Contains(norm, "clientFoundRows=true") || !strings.Contains(norm, "tls=skip-verify") {
		t.Fatalf("unexpected normalized mysql DSN: %s", norm)
	}

	got := rebindPostgres("SELECT * FROM runs WHERE status = '?' AND workflow_name = ? LIMIT ?")
	want := "SELECT * FROM runs WHERE status = '?' AND workflow_name = $1 LIMIT $2"
	if got != want {
		t.Fatalf("rebindPostgres mismatch:\ngot:  %s\nwant: %s", got, want)
	}
}

func runRemoteStoreCRUDTest(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	if err := store.CreateRunWithParent(ctx, "remote-parent", "remote-main", `{"name":"remote-main"}`, "", ""); err != nil {
		t.Fatalf("CreateRunWithParent: %v", err)
	}
	trID, err := store.CreateTaskRunWithMeta(ctx, "remote-parent", "step1", "python", "python:3.13-slim")
	if err != nil || trID <= 0 {
		t.Fatalf("CreateTaskRunWithMeta: id=%d err=%v", trID, err)
	}
	if err := store.UpdateTaskRunLogs(ctx, trID, "remote log line 1\n"); err != nil {
		t.Fatalf("UpdateTaskRunLogs: %v", err)
	}
	if err := store.FinishTaskRun(ctx, trID, StatusSuccess, 0, "remote log line 1\ndone 🚀\n"); err != nil {
		t.Fatalf("FinishTaskRun: %v", err)
	}

	if err := store.CreateRunWithParent(ctx, "remote-child", "remote-sub", "", "remote-parent", "step1"); err != nil {
		t.Fatalf("CreateRunWithParent child: %v", err)
	}
	if err := store.FinishRun(ctx, "remote-child", StatusSuccess); err != nil {
		t.Fatalf("FinishRun child: %v", err)
	}
	if err := store.FinishRun(ctx, "remote-parent", StatusSuccess); err != nil {
		t.Fatalf("FinishRun parent: %v", err)
	}

	rec, err := store.GetRun(ctx, "remote-parent")
	if err != nil || rec.Status != StatusSuccess {
		t.Fatalf("GetRun: %+v err=%v", rec, err)
	}
	children, err := store.ListChildRuns(ctx, "remote-parent")
	if err != nil || len(children) != 1 || children[0].ID != "remote-child" {
		t.Fatalf("ListChildRuns: %+v err=%v", children, err)
	}
	tasks, err := store.ListTaskRuns(ctx, "remote-parent")
	if err != nil || len(tasks) != 1 || tasks[0].Logs.String != "remote log line 1\ndone 🚀\n" {
		t.Fatalf("ListTaskRuns: %+v err=%v", tasks, err)
	}
	stats, err := store.GetRunStats(ctx)
	if err != nil || stats.Total != 2 || stats.Success != 2 {
		t.Fatalf("GetRunStats: %+v err=%v", stats, err)
	}
	latest, err := store.GetLatestRunByWorkflow(ctx)
	if err != nil || latest["remote-main"].ID != "remote-parent" {
		t.Fatalf("GetLatestRunByWorkflow: %+v err=%v", latest, err)
	}
}

func TestPostgresStoreIntegration(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("FREUD_TEST_POSTGRES_DSN"))
	if dsn == "" {
		t.Skip("FREUD_TEST_POSTGRES_DSN not set")
	}

	store, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open postgres failed: %v", err)
	}
	defer store.Close()

	if store.Dialect() != DialectPostgres {
		t.Fatalf("expected DialectPostgres, got %s", store.Dialect())
	}

	_, _ = store.db.ExecContext(context.Background(), "TRUNCATE TABLE task_runs, runs, users, sessions, api_tokens, audit_logs RESTART IDENTITY CASCADE")
	runRemoteStoreCRUDTest(t, store)
	runStoreUsersAndAuditChecks(t, store)
}

func TestMySQLStoreIntegration(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("FREUD_TEST_MYSQL_DSN"))
	if dsn == "" {
		t.Skip("FREUD_TEST_MYSQL_DSN not set")
	}

	store, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open mysql/mariadb failed: %v", err)
	}
	defer store.Close()

	if store.Dialect() != DialectMySQL {
		t.Fatalf("expected DialectMySQL, got %s", store.Dialect())
	}

	ctx := context.Background()
	_, _ = store.db.ExecContext(ctx, "DELETE FROM task_runs")
	_, _ = store.db.ExecContext(ctx, "DELETE FROM runs")
	_, _ = store.db.ExecContext(ctx, "DELETE FROM api_tokens")
	_, _ = store.db.ExecContext(ctx, "DELETE FROM sessions")
	_, _ = store.db.ExecContext(ctx, "DELETE FROM audit_logs")
	_, _ = store.db.ExecContext(ctx, "DELETE FROM users")
	runRemoteStoreCRUDTest(t, store)
	runStoreUsersAndAuditChecks(t, store)

	// Re-open on same database to verify idempotent MySQL/MariaDB migration check.
	store2, err := Open(dsn)
	if err != nil {
		t.Fatalf("Re-open mysql/mariadb failed: %v", err)
	}
	if err := store2.Close(); err != nil {
		t.Fatalf("close store2: %v", err)
	}
}

func TestStoreUsersSessionsAndAudit(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "users.db"))
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer store.Close()

	runStoreUsersAndAuditChecks(t, store)
}

func runStoreUsersAndAuditChecks(t *testing.T, store *Store) {
	t.Helper()
	ctx := context.Background()

	hasAdmin, err := store.HasAdmin(ctx)
	if err != nil || hasAdmin {
		t.Fatalf("expected no admin initially, hasAdmin=%v err=%v", hasAdmin, err)
	}

	if _, err := store.CreateUser(ctx, "admin", "hash-admin", true); err != nil {
		t.Fatalf("CreateUser admin: %v", err)
	}
	if _, err := store.CreateUser(ctx, "admin", "hash-dup", false); !errors.Is(err, ErrUserExists) {
		t.Fatalf("expected ErrUserExists on duplicate, got %v", err)
	}
	if _, err := store.CreateUser(ctx, "alice", "hash-alice", false); err != nil {
		t.Fatalf("CreateUser alice: %v", err)
	}

	hasAdmin, err = store.HasAdmin(ctx)
	if err != nil || !hasAdmin {
		t.Fatalf("expected HasAdmin=true, err=%v", err)
	}

	// Cannot delete last admin; can delete regular user.
	if err := store.DeleteUser(ctx, "admin"); !errors.Is(err, ErrLastAdmin) {
		t.Fatalf("expected ErrLastAdmin, got %v", err)
	}
	if err := store.UpdateUserPassword(ctx, "alice", "hash-alice-v2"); err != nil {
		t.Fatalf("UpdateUserPassword: %v", err)
	}

	// API tokens: create, resolve, update last_used_at, list, revoke, and cascade on user delete.
	tokRec, err := store.CreateAPITokenWithScope(ctx, "alice", "alice-ci", "frd_alice", "sha256-alice-1", "write")
	if err != nil || tokRec.Name != "alice-ci" || tokRec.Scope != "write" {
		t.Fatalf("CreateAPITokenWithScope: %+v err=%v", tokRec, err)
	}
	tokUser, loadedTok, err := store.GetUserByAPITokenHash(ctx, "sha256-alice-1")
	if err != nil || tokUser.Username != "alice" || loadedTok.Scope != "write" || !loadedTok.LastUsedAt.Valid {
		t.Fatalf("GetUserByAPITokenHash: user=%+v tok=%+v err=%v", tokUser, loadedTok, err)
	}
	aliceTokens, err := store.ListAPITokens(ctx, "alice")
	if err != nil || len(aliceTokens) != 1 {
		t.Fatalf("ListAPITokens: %+v err=%v", aliceTokens, err)
	}
	if _, err := store.DeleteAPIToken(ctx, tokRec.ID, "bob", false); err == nil {
		t.Fatalf("expected non-admin other user revoke to fail")
	}

	// Sessions: valid vs expired.
	if err := store.CreateSession(ctx, "tok-valid", "alice", time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatalf("CreateSession valid: %v", err)
	}
	if err := store.CreateSession(ctx, "tok-expired", "alice", time.Now().UTC().Add(-time.Minute)); err != nil {
		t.Fatalf("CreateSession expired: %v", err)
	}
	sessUser, err := store.GetSessionUser(ctx, "tok-valid")
	if err != nil || sessUser.Username != "alice" || sessUser.PasswordHash != "hash-alice-v2" {
		t.Fatalf("GetSessionUser valid mismatch: %+v err=%v", sessUser, err)
	}
	if _, err := store.GetSessionUser(ctx, "tok-expired"); err == nil {
		t.Fatalf("expected error for expired session")
	}

	if err := store.DeleteUser(ctx, "alice"); err != nil {
		t.Fatalf("DeleteUser alice: %v", err)
	}
	if _, err := store.GetSessionUser(ctx, "tok-valid"); err == nil {
		t.Fatalf("expected alice session deleted when alice deleted")
	}
	if _, _, err := store.GetUserByAPITokenHash(ctx, "sha256-alice-1"); err == nil {
		t.Fatalf("expected alice api token deleted when alice deleted")
	}

	// Audit logs.
	if err := store.RecordAudit(ctx, "admin", "LOGIN", "dream", "dream", "signed in"); err != nil {
		t.Fatalf("RecordAudit 1: %v", err)
	}
	if err := store.RecordAudit(ctx, "admin", "FLOW_RUN", "etl-example", "dream", "run_id=123"); err != nil {
		t.Fatalf("RecordAudit 2: %v", err)
	}
	logs, err := store.ListAuditLogs(ctx, 10, "")
	if err != nil || len(logs) != 2 || logs[0].Action != "FLOW_RUN" || logs[1].Action != "LOGIN" {
		t.Fatalf("unexpected audit logs: %+v err=%v", logs, err)
	}
	filtered, err := store.ListAuditLogs(ctx, 10, "LOGIN")
	if err != nil || len(filtered) != 1 || filtered[0].Username != "admin" {
		t.Fatalf("unexpected filtered audit logs: %+v err=%v", filtered, err)
	}
	pagedAudit, totalAudit, err := store.ListAuditLogsPage(ctx, AuditQuery{Limit: 1, Offset: 1})
	if err != nil || totalAudit != 2 || len(pagedAudit) != 1 || pagedAudit[0].Action != "LOGIN" {
		t.Fatalf("unexpected paged audit logs: logs=%+v total=%d err=%v", pagedAudit, totalAudit, err)
	}
	searchedAudit, totalAuditSearch, err := store.ListAuditLogsPage(ctx, AuditQuery{Search: "123", Limit: 10})
	if err != nil || totalAuditSearch != 1 || len(searchedAudit) != 1 || searchedAudit[0].Action != "FLOW_RUN" {
		t.Fatalf("unexpected searched audit logs: logs=%+v total=%d err=%v", searchedAudit, totalAuditSearch, err)
	}

	// Settings upsert, read, storage counts, and delete by prefix.
	if err := store.UpsertSetting(ctx, "retention.runs_days", "30"); err != nil {
		t.Fatalf("UpsertSetting create: %v", err)
	}
	if err := store.UpsertSetting(ctx, "retention.runs_days", "14"); err != nil {
		t.Fatalf("UpsertSetting update: %v", err)
	}
	kv, err := store.GetSettingsByPrefix(ctx, "retention.")
	if err != nil || kv["retention.runs_days"] != "14" {
		t.Fatalf("GetSettingsByPrefix mismatch: %+v err=%v", kv, err)
	}
	sc, err := store.GetStorageCounts(ctx)
	if err != nil || sc.AuditLogs < 2 {
		t.Fatalf("GetStorageCounts mismatch: %+v err=%v", sc, err)
	}
	if err := store.DeleteSettingsByPrefix(ctx, "retention."); err != nil {
		t.Fatalf("DeleteSettingsByPrefix: %v", err)
	}
	kvAfter, err := store.GetSettingsByPrefix(ctx, "retention.")
	if err != nil || len(kvAfter) != 0 {
		t.Fatalf("expected settings cleared after DeleteSettingsByPrefix, got %+v err=%v", kvAfter, err)
	}
}

func TestStoreMaintenanceCleanup(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "cleanup.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()
	if err := store.CreateRun(ctx, "run-old", "demo"); err != nil {
		t.Fatal(err)
	}
	tr1, err := store.CreateTaskRun(ctx, "run-old", "t1")
	if err != nil {
		t.Fatal(err)
	}
	_ = store.FinishTaskRun(ctx, tr1, StatusSuccess, 0, "done")
	_ = store.FinishRun(ctx, "run-old", StatusSuccess)

	if err := store.CreateRun(ctx, "run-keep", "demo"); err != nil {
		t.Fatal(err)
	}
	_ = store.RecordAudit(ctx, "admin", "LOGIN", "dream", "dream", "")
	_ = store.CreateSession(ctx, "exp-tok", "admin", time.Now().UTC().Add(-time.Hour))
	_ = store.CreateSession(ctx, "ok-tok", "admin", time.Now().UTC().Add(time.Hour))

	// Backdate finished_at and audit created_at directly to test cutoff pruning.
	oldTS := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339Nano)
	_, _ = store.db.ExecContext(ctx, store.rebind(`UPDATE runs SET finished_at = ? WHERE id = ?`), oldTS, "run-old")
	_, _ = store.db.ExecContext(ctx, store.rebind(`UPDATE audit_logs SET created_at = ?`), oldTS)

	meta, err := store.ListRunRetentionMeta(ctx)
	if err != nil || len(meta) != 2 {
		t.Fatalf("ListRunRetentionMeta: %+v err=%v", meta, err)
	}

	delRuns, delTasks, err := store.DeleteRunsByIDs(ctx, []string{"run-old"})
	if err != nil || delRuns != 1 || delTasks != 1 {
		t.Fatalf("DeleteRunsByIDs: runs=%d tasks=%d err=%v", delRuns, delTasks, err)
	}
	if _, err := store.GetRun(ctx, "run-keep"); err != nil {
		t.Fatalf("expected run-keep still present: %v", err)
	}

	delSess, err := store.DeleteExpiredSessions(ctx, time.Now().UTC())
	if err != nil || delSess != 1 {
		t.Fatalf("DeleteExpiredSessions: %d err=%v", delSess, err)
	}
	delAudit, err := store.PruneAuditLogsBefore(ctx, time.Now().UTC().Add(-24*time.Hour))
	if err != nil || delAudit != 1 {
		t.Fatalf("PruneAuditLogsBefore: %d err=%v", delAudit, err)
	}
	if err := store.Vacuum(ctx); err != nil {
		t.Fatalf("Vacuum: %v", err)
	}
}
