package maintenance

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"freud/internal/config"
	"freud/internal/db"
	"freud/internal/runner"
)

const settingPrefix = "retention."

// DockerCacheManager exposes dependency image inspection and cleanup from internal/runner.
type DockerCacheManager interface {
	ListCachedImages(ctx context.Context) ([]runner.CachedImage, error)
	ClearCachedImages(ctx context.Context) (int, int64, error)
}

// Policy defines active retention periods (0 means keep forever).
type Policy struct {
	RunsDays       int  `json:"runs_days"`
	WorkspacesDays int  `json:"workspaces_days"`
	AuditDays      int  `json:"audit_days"`
	VacuumSQLite   bool `json:"vacuum_sqlite"`
}

// StorageReport summarizes database/disk/Docker footprint and current retention state.
type StorageReport struct {
	DB               db.StorageCounts `json:"db"`
	WorkspaceDirs    int              `json:"workspace_dirs"`
	WorkspacesBytes  int64            `json:"workspaces_bytes"`
	CacheAvailable   bool             `json:"cache_available"`
	CacheImagesCount int              `json:"cache_images_count"`
	CacheImagesBytes int64            `json:"cache_images_bytes"`
	CacheErr         string           `json:"cache_err,omitempty"`
	Policy           Policy           `json:"policy"`
	Defaults         Policy           `json:"defaults"`
	Interval         string           `json:"interval"`
	UsingUIOverrides bool             `json:"using_ui_overrides"`
	LastResult       *CleanupResult   `json:"last_result,omitempty"`
}

// CleanupResult describes items removed during a single retention cleanup pass.
type CleanupResult struct {
	RanAt              string `json:"ran_at"`
	Trigger            string `json:"trigger"`
	DeletedRuns        int64  `json:"deleted_runs"`
	DeletedTaskRuns    int64  `json:"deleted_task_runs"`
	DeletedAuditLogs   int64  `json:"deleted_audit_logs"`
	DeletedSessions    int64  `json:"deleted_sessions"`
	RemovedWorkspaces  int    `json:"removed_workspaces"`
	ReclaimedDiskBytes int64  `json:"reclaimed_disk_bytes"`
	Vacuumed           bool   `json:"vacuumed"`
}

// Service manages retention policies, periodic cleanup, and storage inspection.
type Service struct {
	store         *db.Store
	workspacesDir string
	defaults      config.RetentionConfig
	interval      time.Duration
	cacheMgr      DockerCacheManager

	mu         sync.Mutex
	lastResult *CleanupResult
}

// New constructs a maintenance Service initialized from config defaults.
func New(store *db.Store, workspacesDir string, defaults config.RetentionConfig, cacheMgr DockerCacheManager) (*Service, error) {
	if strings.TrimSpace(workspacesDir) == "" {
		workspacesDir = config.DefaultWorkspacesDir
	}
	var interval time.Duration
	if iv := strings.TrimSpace(defaults.Interval); iv != "" {
		d, err := time.ParseDuration(iv)
		if err != nil {
			return nil, fmt.Errorf("parse retention interval %q: %w", iv, err)
		}
		interval = d
	}
	return &Service{
		store:         store,
		workspacesDir: workspacesDir,
		defaults:      defaults,
		interval:      interval,
		cacheMgr:      cacheMgr,
	}, nil
}

// ResolvePolicy loads effective retention settings (DB UI overrides on top of config defaults).
func (s *Service) ResolvePolicy(ctx context.Context) (Policy, bool, error) {
	p := Policy{
		RunsDays:       s.defaults.RunsDays,
		WorkspacesDays: s.defaults.WorkspacesDays,
		AuditDays:      s.defaults.AuditDays,
		VacuumSQLite:   s.defaults.VacuumSQLite,
	}

	raw, err := s.store.GetSettingsByPrefix(ctx, settingPrefix)
	if err != nil {
		return p, false, err
	}
	if len(raw) == 0 {
		return p, false, nil
	}

	if v, ok := raw[settingPrefix+"runs_days"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			p.RunsDays = n
		}
	}
	if v, ok := raw[settingPrefix+"workspaces_days"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			p.WorkspacesDays = n
		}
	}
	if v, ok := raw[settingPrefix+"audit_days"]; ok {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			p.AuditDays = n
		}
	}
	if v, ok := raw[settingPrefix+"vacuum_sqlite"]; ok {
		if b, err := strconv.ParseBool(v); err == nil {
			p.VacuumSQLite = b
		}
	}
	return p, true, nil
}

// SavePolicy persists retention policy overrides in DB settings and records an audit event.
func (s *Service) SavePolicy(ctx context.Context, actor, source string, p Policy) error {
	if p.RunsDays < 0 || p.WorkspacesDays < 0 || p.AuditDays < 0 {
		return fmt.Errorf("retention day values must be >= 0")
	}
	pairs := map[string]string{
		settingPrefix + "runs_days":       strconv.Itoa(p.RunsDays),
		settingPrefix + "workspaces_days": strconv.Itoa(p.WorkspacesDays),
		settingPrefix + "audit_days":      strconv.Itoa(p.AuditDays),
		settingPrefix + "vacuum_sqlite":   strconv.FormatBool(p.VacuumSQLite),
	}
	for k, v := range pairs {
		if err := s.store.UpsertSetting(ctx, k, v); err != nil {
			return err
		}
	}
	_ = s.store.RecordAudit(ctx, actor, "RETENTION_UPDATED", "retention", source,
		fmt.Sprintf("runs=%dd workspaces=%dd audit=%dd vacuum=%v", p.RunsDays, p.WorkspacesDays, p.AuditDays, p.VacuumSQLite))
	return nil
}

// ResetPolicy removes DB UI overrides so freud.toml / env defaults take effect again.
func (s *Service) ResetPolicy(ctx context.Context, actor, source string) error {
	if err := s.store.DeleteSettingsByPrefix(ctx, settingPrefix); err != nil {
		return err
	}
	_ = s.store.RecordAudit(ctx, actor, "RETENTION_UPDATED", "retention", source, "reset_to_config_defaults")
	return nil
}

// InspectStorage computes database, workspace disk, and Docker image cache metrics.
func (s *Service) InspectStorage(ctx context.Context) (*StorageReport, error) {
	dbCounts, err := s.store.GetStorageCounts(ctx)
	if err != nil {
		return nil, err
	}
	policy, overrides, err := s.ResolvePolicy(ctx)
	if err != nil {
		return nil, err
	}
	wsDirs, wsBytes := measureWorkspacesDir(s.workspacesDir)

	s.mu.Lock()
	var lastCopy *CleanupResult
	if s.lastResult != nil {
		c := *s.lastResult
		lastCopy = &c
	}
	s.mu.Unlock()

	rep := &StorageReport{
		DB:              dbCounts,
		WorkspaceDirs:   wsDirs,
		WorkspacesBytes: wsBytes,
		Policy:          policy,
		Defaults: Policy{
			RunsDays:       s.defaults.RunsDays,
			WorkspacesDays: s.defaults.WorkspacesDays,
			AuditDays:      s.defaults.AuditDays,
			VacuumSQLite:   s.defaults.VacuumSQLite,
		},
		Interval:         s.defaults.Interval,
		UsingUIOverrides: overrides,
		LastResult:       lastCopy,
	}

	if s.cacheMgr != nil {
		rep.CacheAvailable = true
		imgs, err := s.cacheMgr.ListCachedImages(ctx)
		if err != nil {
			rep.CacheErr = err.Error()
		} else {
			rep.CacheImagesCount = len(imgs)
			for _, img := range imgs {
				rep.CacheImagesBytes += img.Size
			}
		}
	}
	return rep, nil
}

// ClearDockerCache removes cached orchestrator dependency images via DockerCacheManager.
func (s *Service) ClearDockerCache(ctx context.Context, actor, source string) (int, int64, error) {
	if s.cacheMgr == nil {
		return 0, 0, fmt.Errorf("docker cache manager is not available")
	}
	removed, reclaimed, err := s.cacheMgr.ClearCachedImages(ctx)
	if err != nil {
		return removed, reclaimed, err
	}
	_ = s.store.RecordAudit(ctx, actor, "CACHE_CLEARED", "docker-cache", source,
		fmt.Sprintf("removed=%d bytes=%d", removed, reclaimed))
	return removed, reclaimed, nil
}

// RunPeriodic executes cleanup periodically according to defaults.Interval until ctx cancels.
func (s *Service) RunPeriodic(ctx context.Context) {
	if s.interval <= 0 {
		<-ctx.Done()
		return
	}
	// Run an initial pass shortly after startup, then on every interval tick.
	_, _ = s.RunCleanup(ctx, "timer", "maintenance", false)

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.RunCleanup(ctx, "timer", "maintenance", false)
		}
	}
}

// RunCleanup applies active retention rules (plus expired session cleanup) safely under lock.
func (s *Service) RunCleanup(ctx context.Context, trigger, actor string, forceAudit bool) (*CleanupResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC()
	policy, _, err := s.ResolvePolicy(ctx)
	if err != nil {
		return nil, err
	}

	res := &CleanupResult{
		RanAt:   now.Format(time.RFC3339Nano),
		Trigger: trigger,
	}

	if n, err := s.store.DeleteExpiredSessions(ctx, now); err == nil {
		res.DeletedSessions = n
	} else {
		return res, err
	}

	meta, err := s.store.ListRunRetentionMeta(ctx)
	if err != nil {
		return res, err
	}

	if policy.WorkspacesDays > 0 {
		cutoff := now.Add(-time.Duration(policy.WorkspacesDays) * 24 * time.Hour)
		dirs, bytesReclaimed, err := pruneWorkspaces(s.workspacesDir, meta, cutoff)
		if err != nil {
			return res, err
		}
		res.RemovedWorkspaces = dirs
		res.ReclaimedDiskBytes = bytesReclaimed
	}

	if policy.RunsDays > 0 {
		cutoff := now.Add(-time.Duration(policy.RunsDays) * 24 * time.Hour)
		var toDelete []string
		for _, r := range meta {
			if r.Status == db.StatusRunning || r.FinishedAt == "" {
				continue
			}
			fin, parseErr := time.Parse(time.RFC3339Nano, r.FinishedAt)
			if parseErr == nil && fin.Before(cutoff) {
				toDelete = append(toDelete, r.ID)
			}
		}
		delRuns, delTasks, err := s.store.DeleteRunsByIDs(ctx, toDelete)
		if err != nil {
			return res, err
		}
		res.DeletedRuns = delRuns
		res.DeletedTaskRuns = delTasks
	}

	if policy.AuditDays > 0 {
		cutoff := now.Add(-time.Duration(policy.AuditDays) * 24 * time.Hour)
		delAudit, err := s.store.PruneAuditLogsBefore(ctx, cutoff)
		if err != nil {
			return res, err
		}
		res.DeletedAuditLogs = delAudit
	}

	if policy.VacuumSQLite && (res.DeletedRuns > 0 || res.DeletedAuditLogs > 0 || res.DeletedSessions > 0) {
		if err := s.store.Vacuum(ctx); err != nil {
			return res, err
		}
		res.Vacuumed = true
	}

	s.lastResult = res

	anythingChanged := res.DeletedRuns > 0 || res.DeletedAuditLogs > 0 || res.RemovedWorkspaces > 0 || res.DeletedSessions > 0
	if forceAudit || anythingChanged {
		src := trigger
		if src == "" {
			src = "maintenance"
		}
		details := fmt.Sprintf("runs=%d tasks=%d workspaces=%d audit=%d sessions=%d bytes=%d",
			res.DeletedRuns, res.DeletedTaskRuns, res.RemovedWorkspaces, res.DeletedAuditLogs, res.DeletedSessions, res.ReclaimedDiskBytes)
		_ = s.store.RecordAudit(ctx, actor, "RETENTION_CLEANUP", "maintenance", src, details)
	}
	return res, nil
}

func measureWorkspacesDir(baseDir string) (dirs int, totalBytes int64) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		return 0, 0
	}
	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		dirs++
		_ = filepath.WalkDir(filepath.Join(baseDir, e.Name()), func(_ string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() {
				return nil
			}
			if info, err := d.Info(); err == nil {
				totalBytes += info.Size()
			}
			return nil
		})
	}
	return dirs, totalBytes
}

func pruneWorkspaces(baseDir string, runsMeta []db.RunRetentionMeta, cutoff time.Time) (removed int, reclaimed int64, err error) {
	entries, err := os.ReadDir(baseDir)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, 0, nil
		}
		return 0, 0, err
	}

	byID := make(map[string]db.RunRetentionMeta, len(runsMeta))
	for _, m := range runsMeta {
		byID[m.ID] = m
	}

	for _, e := range entries {
		if !e.IsDir() || strings.HasPrefix(e.Name(), ".") {
			continue
		}
		runID := e.Name()
		wsPath := filepath.Join(baseDir, runID)

		var ageTime time.Time
		if m, ok := byID[runID]; ok {
			if m.Status == db.StatusRunning {
				continue
			}
			if m.FinishedAt != "" {
				if parsed, parseErr := time.Parse(time.RFC3339Nano, m.FinishedAt); parseErr == nil {
					ageTime = parsed
				}
			}
		}
		if ageTime.IsZero() {
			info, statErr := e.Info()
			if statErr != nil {
				continue
			}
			ageTime = info.ModTime().UTC()
		}

		if !ageTime.Before(cutoff) {
			continue
		}

		dirSize := dirBytes(wsPath)
		if rmErr := os.RemoveAll(wsPath); rmErr != nil {
			return removed, reclaimed, rmErr
		}
		removed++
		reclaimed += dirSize
	}
	return removed, reclaimed, nil
}

func dirBytes(path string) int64 {
	var total int64
	_ = filepath.WalkDir(path, func(_ string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err == nil {
			total += info.Size()
		}
		return nil
	})
	return total
}
