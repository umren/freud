package maintenance

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"freud/internal/config"
	"freud/internal/db"
	"freud/internal/runner"
)

type mockCacheMgr struct {
	images  []runner.CachedImage
	cleared bool
}

func (m *mockCacheMgr) ListCachedImages(_ context.Context) ([]runner.CachedImage, error) {
	return m.images, nil
}

func (m *mockCacheMgr) ClearCachedImages(_ context.Context) (int, int64, error) {
	n := len(m.images)
	var bytes int64
	for _, img := range m.images {
		bytes += img.Size
	}
	m.images = nil
	m.cleared = true
	return n, bytes, nil
}

func TestRetentionPolicyCleanupAndStats(t *testing.T) {
	tmp := t.TempDir()
	wsDir := filepath.Join(tmp, "workspaces")
	if err := os.MkdirAll(wsDir, 0o755); err != nil {
		t.Fatal(err)
	}

	store, err := db.Open(filepath.Join(tmp, "maint.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	ctx := context.Background()

	// Seed 1 active RUNNING run (must NEVER be deleted) and 1 finished run.
	if err := store.CreateRunFull(ctx, "run-active", "wf", "", "", "", "admin"); err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRunFull(ctx, "run-old", "wf", "", "", "", "admin"); err != nil {
		t.Fatal(err)
	}
	trOld, err := store.CreateTaskRunWithMeta(ctx, "run-old", "step1", "python", "python:3.13")
	if err != nil {
		t.Fatal(err)
	}
	_ = store.FinishTaskRun(ctx, trOld, db.StatusSuccess, 0, "old output")
	_ = store.FinishRun(ctx, "run-old", db.StatusSuccess)

	// Workspace folders on disk: old workspace backdated by 10 days, active workspace untouched.
	oldWS := filepath.Join(wsDir, "run-old")
	activeWS := filepath.Join(wsDir, "run-active")
	orphanOldWS := filepath.Join(wsDir, "orphan-old")
	for _, d := range []string{oldWS, activeWS, orphanOldWS} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(d, "artifact.txt"), []byte("1234567890"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	oldMod := time.Now().Add(-10 * 24 * time.Hour)
	_ = os.Chtimes(orphanOldWS, oldMod, oldMod)

	// Add an expired and a valid session.
	_ = store.CreateSession(ctx, "sess-expired", "admin", time.Now().UTC().Add(-time.Hour))
	_ = store.CreateSession(ctx, "sess-valid", "admin", time.Now().UTC().Add(time.Hour))
	_ = store.RecordAudit(ctx, "admin", "LOGIN", "dream", "dream", "ok")

	cacheMgr := &mockCacheMgr{
		images: []runner.CachedImage{
			{ID: "img1", Tag: "orchestrator-cache:abc", Size: 2048},
		},
	}

	svc, err := New(store, wsDir, config.RetentionConfig{
		Interval:       "6h",
		RunsDays:       30,
		WorkspacesDays: 7,
		AuditDays:      90,
	}, cacheMgr)
	if err != nil {
		t.Fatalf("maintenance.New: %v", err)
	}

	// Config defaults apply when no UI overrides saved yet.
	pol, overridden, err := svc.ResolvePolicy(ctx)
	if err != nil || overridden || pol.RunsDays != 30 || pol.WorkspacesDays != 7 {
		t.Fatalf("unexpected initial policy: pol=%+v overridden=%v err=%v", pol, overridden, err)
	}

	// InspectStorage reports disk, DB, and cache footprints.
	rep, err := svc.InspectStorage(ctx)
	if err != nil {
		t.Fatalf("InspectStorage: %v", err)
	}
	if rep.DB.Runs != 2 || rep.WorkspaceDirs != 3 || rep.WorkspacesBytes != 30 || rep.CacheImagesCount != 1 || rep.CacheImagesBytes != 2048 {
		t.Fatalf("unexpected storage report: %+v", rep)
	}

	// Save UI override (0 days runs, 5 days workspaces) and verify persistence & reset.
	if err := svc.SavePolicy(ctx, "admin", "dream", Policy{RunsDays: 0, WorkspacesDays: 5, AuditDays: 0, VacuumSQLite: true}); err != nil {
		t.Fatalf("SavePolicy: %v", err)
	}
	pol2, overridden2, err := svc.ResolvePolicy(ctx)
	if err != nil || !overridden2 || pol2.WorkspacesDays != 5 || !pol2.VacuumSQLite {
		t.Fatalf("expected saved policy override: pol=%+v overridden=%v err=%v", pol2, overridden2, err)
	}

	// RunCleanup prunes expired session & orphan-old workspace (>5d), keeps run-active & fresh run-old.
	res, err := svc.RunCleanup(ctx, "dream", "admin", true)
	if err != nil {
		t.Fatalf("RunCleanup: %v", err)
	}
	if res.DeletedSessions != 1 || res.RemovedWorkspaces != 1 || res.ReclaimedDiskBytes != 10 {
		t.Fatalf("unexpected RunCleanup result: %+v", res)
	}
	if _, err := os.Stat(orphanOldWS); !os.IsNotExist(err) {
		t.Fatalf("expected orphan-old workspace deleted")
	}
	if _, err := os.Stat(activeWS); err != nil {
		t.Fatalf("expected active workspace preserved: %v", err)
	}

	// Test ResetPolicy restores TOML/config defaults.
	if err := svc.ResetPolicy(ctx, "admin", "dream"); err != nil {
		t.Fatalf("ResetPolicy: %v", err)
	}
	pol3, overridden3, err := svc.ResolvePolicy(ctx)
	if err != nil || overridden3 || pol3.RunsDays != 30 {
		t.Fatalf("expected reset to config defaults: pol=%+v overridden=%v err=%v", pol3, overridden3, err)
	}

	// Test ClearDockerCache clears cache and logs CACHE_CLEARED audit.
	removed, reclaimed, err := svc.ClearDockerCache(ctx, "admin", "dream")
	if err != nil || removed != 1 || reclaimed != 2048 || !cacheMgr.cleared {
		t.Fatalf("unexpected ClearDockerCache: removed=%d reclaimed=%d err=%v", removed, reclaimed, err)
	}
}
