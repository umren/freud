package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadDefaultsWithoutConfigFile(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	tmp := t.TempDir()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("Chdir: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(wd) })

	cfg, err := Load(Overrides{})
	if err != nil {
		t.Fatalf("Load defaults: %v", err)
	}
	if cfg.DB != DefaultDBPath || cfg.WorkspacesDir != DefaultWorkspacesDir || cfg.FlowsDir != DefaultFlowsDir || cfg.IncludesDir != DefaultIncludesDir || cfg.Dream.Port != DefaultDreamPort {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.ConfigFile != "" {
		t.Fatalf("expected empty ConfigFile when none exists, got %q", cfg.ConfigFile)
	}
}

func TestLoadTOMLPrecedenceAndRelativePaths(t *testing.T) {
	tmp := t.TempDir()
	cfgDir := filepath.Join(tmp, "project")
	if err := os.MkdirAll(cfgDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	cfgPath := filepath.Join(cfgDir, "freud.toml")
	tomlContent := `
db = "./data/freud.db"
workspaces_dir = "./data/ws"
flows_dir = "./pipelines"
includes_dir = "./shared"
env_file = ".env.prod"
no_cache = true

[dream]
port = 9090
scheduler = true

[tui]
scheduler = false
`
	if err := os.WriteFile(cfgPath, []byte(tomlContent), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}

	// Env overrides TOML, CLI overrides Env.
	t.Setenv("FREUD_INCLUDES_DIR", "/env/includes")
	t.Setenv("FREUD_PORT", "7777")
	cliFlows := "/cli/flows"

	cfg, err := Load(Overrides{
		ConfigPath: cfgPath,
		FlowsDir:   &cliFlows,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.DB != filepath.Join(cfgDir, "data", "freud.db") {
		t.Fatalf("expected DB relative to freud.toml, got %q", cfg.DB)
	}
	if cfg.WorkspacesDir != filepath.Join(cfgDir, "data", "ws") {
		t.Fatalf("expected WorkspacesDir relative to freud.toml, got %q", cfg.WorkspacesDir)
	}
	if cfg.EnvFile != filepath.Join(cfgDir, ".env.prod") {
		t.Fatalf("expected EnvFile relative to freud.toml, got %q", cfg.EnvFile)
	}
	if cfg.IncludesDir != "/env/includes" {
		t.Fatalf("expected env to override TOML includes_dir, got %q", cfg.IncludesDir)
	}
	if cfg.FlowsDir != "/cli/flows" {
		t.Fatalf("expected CLI to override TOML flows_dir, got %q", cfg.FlowsDir)
	}
	if cfg.Dream.Port != 7777 || !cfg.Dream.Scheduler || cfg.TUI.Scheduler || !cfg.NoCache {
		t.Fatalf("unexpected flags/ports: %+v", cfg)
	}
}

func TestLoadValidationErrors(t *testing.T) {
	tmp := t.TempDir()

	if _, err := Load(Overrides{ConfigPath: filepath.Join(tmp, "missing.toml")}); err == nil {
		t.Fatalf("expected error for missing explicit --config")
	}

	badTOML := filepath.Join(tmp, "bad.toml")
	if err := os.WriteFile(badTOML, []byte(`unknown_field = "oops"`), 0o644); err != nil {
		t.Fatalf("write bad.toml: %v", err)
	}
	if _, err := Load(Overrides{ConfigPath: badTOML}); err == nil || !strings.Contains(err.Error(), "unknown config key") {
		t.Fatalf("expected unknown config key error, got %v", err)
	}

	badPort := 70000
	if _, err := Load(Overrides{DreamPort: &badPort}); err == nil || !strings.Contains(err.Error(), "invalid port") {
		t.Fatalf("expected invalid port error, got %v", err)
	}

	for _, dsn := range []string{
		"postgres://freud:secret@localhost:5432/freud?sslmode=disable",
		"mysql://freud:secret@localhost:3306/freud",
		"mariadb://freud:secret@localhost:3306/freud",
	} {
		dsnTOML := filepath.Join(tmp, "dsn.toml")
		if err := os.WriteFile(dsnTOML, []byte("db = \""+dsn+"\"\n"), 0o644); err != nil {
			t.Fatalf("write dsn.toml: %v", err)
		}
		cfg, err := Load(Overrides{ConfigPath: dsnTOML})
		if err != nil {
			t.Fatalf("Load %s: %v", dsn, err)
		}
		if cfg.DB != dsn {
			t.Fatalf("expected DSN %q preserved verbatim, got %q", dsn, cfg.DB)
		}
	}
}

func TestSyncConfigTOMLAndEnvOverrides(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "freud.toml")
	tomlData := `
[sync]
enabled = true
repo = "https://github.com/acme/workflows.git"
flows_path = "pipelines"
includes_path = "shared"
prune = true
cache_dir = "./sync-cache"
ssh_key = "./keys/id_ed25519"
`
	if err := os.WriteFile(cfgPath, []byte(tomlData), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(Overrides{ConfigPath: cfgPath})
	if err != nil {
		t.Fatalf("Load single flows_path: %v", err)
	}
	if !cfg.Sync.Enabled || len(cfg.Sync.FlowsPath) != 1 || cfg.Sync.FlowsPath[0] != "pipelines" || cfg.Sync.Branch != DefaultSyncBranch || cfg.Sync.Interval != DefaultSyncInterval || !cfg.Sync.Prune {
		t.Fatalf("unexpected sync config: %+v", cfg.Sync)
	}
	if cfg.Sync.CacheDir != filepath.Join(tmp, "sync-cache") || cfg.Sync.SSHKey != filepath.Join(tmp, "keys", "id_ed25519") {
		t.Fatalf("expected relative sync paths resolved, got cache=%q ssh=%q", cfg.Sync.CacheDir, cfg.Sync.SSHKey)
	}

	t.Setenv("FREUD_SYNC_FLOWS_PATH", "team-a/flows, team-b/flows")
	t.Setenv("FREUD_SYNC_TOKEN", "pat-123")
	t.Setenv("FREUD_SYNC_INTERVAL", "5m")
	cfg2, err := Load(Overrides{ConfigPath: cfgPath})
	if err != nil {
		t.Fatalf("Load env overrides: %v", err)
	}
	if len(cfg2.Sync.FlowsPath) != 2 || cfg2.Sync.FlowsPath[0] != "team-a/flows" || cfg2.Sync.FlowsPath[1] != "team-b/flows" || cfg2.Sync.Token != "pat-123" || cfg2.Sync.Interval != "5m" {
		t.Fatalf("unexpected sync env overrides: %+v", cfg2.Sync)
	}

	t.Setenv("FREUD_SYNC_FLOWS_PATH", "../outside")
	if _, err := Load(Overrides{ConfigPath: cfgPath}); err == nil {
		t.Fatal("expected error for escaping sync.flows_path")
	}
}
