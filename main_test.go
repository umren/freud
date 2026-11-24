package main

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"freud/internal/config"
)

func TestCLIFlagOverridesRespectTOMLWhenUnset(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := filepath.Join(tmp, "freud.toml")
	tomlData := `
db = "./state/freud.db"
flows_dir = "./custom-flows"
includes_dir = "./custom-includes"
workspaces_dir = "./custom-ws"

[dream]
port = 5555
scheduler = true
`
	if err := os.WriteFile(cfgPath, []byte(tomlData), 0o644); err != nil {
		t.Fatalf("write toml: %v", err)
	}

	fs := flag.NewFlagSet("dream", flag.ContinueOnError)
	var b cliFlagBindings
	registerDreamFlags(fs, &b)
	// Only override --config and --port; leave flows-dir/db unset so TOML values remain.
	if err := fs.Parse([]string{"--config", cfgPath, "--port", "6060"}); err != nil {
		t.Fatalf("parse flags: %v", err)
	}

	cfg, err := config.Load(b.toOverrides(fs))
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}

	if cfg.DB != filepath.Join(tmp, "state", "freud.db") {
		t.Fatalf("expected TOML db path preserved, got %q", cfg.DB)
	}
	if cfg.FlowsDir != filepath.Join(tmp, "custom-flows") || !cfg.FlowsDirExplicit {
		t.Fatalf("expected TOML flows_dir preserved & marked explicit, got %q explicit=%v", cfg.FlowsDir, cfg.FlowsDirExplicit)
	}
	if cfg.IncludesDir != filepath.Join(tmp, "custom-includes") {
		t.Fatalf("expected TOML includes_dir preserved, got %q", cfg.IncludesDir)
	}
	if cfg.WorkspacesDir != filepath.Join(tmp, "custom-ws") {
		t.Fatalf("expected TOML workspaces_dir preserved, got %q", cfg.WorkspacesDir)
	}
	if cfg.Dream.Port != 6060 || !cfg.Dream.Scheduler {
		t.Fatalf("expected port=6060 (CLI) and scheduler=true (TOML), got %+v", cfg.Dream)
	}
}
