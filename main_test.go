package main

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"freud/internal/auth"
	"freud/internal/config"
	"freud/internal/db"
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

func TestCLISubcommandsValidationAndEnvLoader(t *testing.T) {
	if err := runCLI(nil); err == nil {
		t.Fatal("expected error when no CLI args provided")
	}
	if err := runCLI([]string{"unknown-subcmd"}); err == nil {
		t.Fatal("expected error for unknown CLI command")
	}
	if err := runWorkflowCmd(nil); err == nil {
		t.Fatal("expected error for run without workflow path")
	}
	if err := runCacheCmd(nil); err == nil {
		t.Fatal("expected error for cache without subcommand")
	}

	envPath := filepath.Join(t.TempDir(), "custom.env")
	if err := os.WriteFile(envPath, []byte("FOO=bar\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	vars, files, err := loadEnvironment(envPath, "")
	if err != nil || vars["FOO"] != "bar" || len(files) != 1 || files[0] != envPath {
		t.Fatalf("unexpected loadEnvironment output: vars=%+v files=%+v err=%v", vars, files, err)
	}
	if _, _, err := loadEnvironment(filepath.Join(t.TempDir(), "missing.env"), ""); err == nil {
		t.Fatal("expected error for missing explicit env file")
	}
}

func TestRunMCPWithIOAndTokenAuth(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "mcp_cli.db")
	flowsDir := filepath.Join(tmp, "flows")
	if err := os.MkdirAll(flowsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(flowsDir, "ok.yaml"), []byte("name: ok-flow\ntasks:\n  - id: t1\n    runtime: python\n    script: print(1)\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Provision a user + API token in the DB.
	store, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	hash, err := auth.HashPassword("secret123")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateUser(ctx, "mcp-bot", hash, false); err != nil {
		t.Fatal(err)
	}
	rawTok, prefix, tokHash, err := auth.NewAPIToken()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateAPIToken(ctx, "mcp-bot", "ide", prefix, tokHash); err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	// Invalid token must fail cleanly.
	var badOut bytes.Buffer
	err = runMCPWithIO(
		[]string{"--db", dbPath, "--flows-dir", flowsDir, "--token", "frd_invalid"},
		strings.NewReader(""),
		&badOut,
	)
	if err == nil || !strings.Contains(err.Error(), "invalid MCP API token") {
		t.Fatalf("expected invalid token error, got %v", err)
	}

	// Valid token runs stdio MCP cleanly and updates token last_used_at.
	in := strings.Join([]string{
		`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26"}}`,
		`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_flows","arguments":{}}}`,
	}, "\n") + "\n"
	var out bytes.Buffer
	err = runMCPWithIO(
		[]string{"--db", dbPath, "--flows-dir", flowsDir, "--token", rawTok},
		strings.NewReader(in),
		&out,
	)
	if err != nil {
		t.Fatalf("runMCPWithIO failed: %v", err)
	}
	if !strings.Contains(out.String(), `"name":"freud"`) || !strings.Contains(out.String(), "ok-flow") {
		t.Fatalf("unexpected MCP CLI stdout:\n%s", out.String())
	}

	// Verify token usage timestamp was recorded.
	store2, err := db.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()
	toks, err := store2.ListAPITokens(ctx, "mcp-bot")
	if err != nil || len(toks) != 1 || !toks[0].LastUsedAt.Valid {
		t.Fatalf("expected mcp-bot token last_used_at recorded, got %+v err=%v", toks, err)
	}
}
