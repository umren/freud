package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/BurntSushi/toml"
)

const (
	// DefaultConfigFile is the optional TOML configuration filename checked in the current directory.
	DefaultConfigFile = "freud.toml"

	DefaultDBPath        = "orchestrator.db"
	DefaultWorkspacesDir = "workspaces"
	DefaultFlowsDir      = "flows"
	DefaultIncludesDir   = "includes"
	DefaultDreamPort     = 4321
)

// DreamConfig configures the embedded web UI server.
type DreamConfig struct {
	Port      int  `toml:"port"`
	Scheduler bool `toml:"scheduler"`
}

// TUIConfig configures the interactive terminal UI.
type TUIConfig struct {
	Scheduler bool `toml:"scheduler"`
}

// Config holds runtime paths and behavior settings for Freud.
type Config struct {
	ConfigFile       string      `toml:"-"`
	FlowsDirExplicit bool        `toml:"-"`
	DB               string      `toml:"db"`
	WorkspacesDir    string      `toml:"workspaces_dir"`
	FlowsDir         string      `toml:"flows_dir"`
	IncludesDir      string      `toml:"includes_dir"`
	EnvFile          string      `toml:"env_file"`
	NoCache          bool        `toml:"no_cache"`
	Dream            DreamConfig `toml:"dream"`
	TUI              TUIConfig   `toml:"tui"`
}

// Overrides carries optional explicitly-passed CLI flag overrides.
type Overrides struct {
	ConfigPath    string
	DB            *string
	WorkspacesDir *string
	FlowsDir      *string
	IncludesDir   *string
	EnvFile       *string
	NoCache       *bool
	DreamPort     *int
	Scheduler     *bool
}

// Default returns Config populated with standard out-of-the-box defaults.
func Default() Config {
	return Config{
		DB:            DefaultDBPath,
		WorkspacesDir: DefaultWorkspacesDir,
		FlowsDir:      DefaultFlowsDir,
		IncludesDir:   DefaultIncludesDir,
		Dream: DreamConfig{
			Port: DefaultDreamPort,
		},
	}
}

// Load resolves Freud configuration using precedence:
// CLI Overrides > FREUD_* environment variables > optional freud.toml > defaults.
func Load(ov Overrides) (Config, error) {
	cfg := Default()

	cfgPath, explicit, err := resolveConfigFilePath(ov.ConfigPath)
	if err != nil {
		return Config{}, err
	}
	if cfgPath != "" {
		if err := applyTOMLFile(&cfg, cfgPath); err != nil {
			return Config{}, err
		}
		cfg.ConfigFile = cfgPath
	} else if explicit {
		return Config{}, fmt.Errorf("config file path cannot be empty")
	}

	if err := applyEnvVars(&cfg); err != nil {
		return Config{}, err
	}

	applyOverrides(&cfg, ov)

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate verifies required paths and port bounds.
func (c Config) Validate() error {
	if strings.TrimSpace(c.DB) == "" {
		return fmt.Errorf("db path cannot be empty")
	}
	if strings.TrimSpace(c.WorkspacesDir) == "" {
		return fmt.Errorf("workspaces_dir cannot be empty")
	}
	if strings.TrimSpace(c.FlowsDir) == "" {
		return fmt.Errorf("flows_dir cannot be empty")
	}
	if strings.TrimSpace(c.IncludesDir) == "" {
		return fmt.Errorf("includes_dir cannot be empty")
	}
	if c.Dream.Port <= 0 || c.Dream.Port > 65535 {
		return fmt.Errorf("invalid port %d", c.Dream.Port)
	}
	return nil
}

func resolveConfigFilePath(cliPath string) (string, bool, error) {
	if p := strings.TrimSpace(cliPath); p != "" {
		info, err := os.Stat(p)
		if err != nil {
			return "", true, fmt.Errorf("stat config file %q: %w", p, err)
		}
		if info.IsDir() {
			return "", true, fmt.Errorf("config path %q is a directory", p)
		}
		return p, true, nil
	}

	if envPath := strings.TrimSpace(os.Getenv("FREUD_CONFIG")); envPath != "" {
		info, err := os.Stat(envPath)
		if err != nil {
			return "", true, fmt.Errorf("stat FREUD_CONFIG file %q: %w", envPath, err)
		}
		if info.IsDir() {
			return "", true, fmt.Errorf("FREUD_CONFIG path %q is a directory", envPath)
		}
		return envPath, true, nil
	}

	if info, err := os.Stat(DefaultConfigFile); err == nil && !info.IsDir() {
		return DefaultConfigFile, false, nil
	}
	return "", false, nil
}

func applyTOMLFile(cfg *Config, path string) error {
	md, err := toml.DecodeFile(path, cfg)
	if err != nil {
		return fmt.Errorf("parse TOML config %q: %w", path, err)
	}
	if undecoded := md.Undecoded(); len(undecoded) > 0 {
		keys := make([]string, 0, len(undecoded))
		for _, k := range undecoded {
			keys = append(keys, k.String())
		}
		return fmt.Errorf("unknown config key(s) in %q: %s", path, strings.Join(keys, ", "))
	}

	// Resolve relative paths in freud.toml against the directory containing freud.toml.
	baseDir := filepath.Dir(path)
	resolveRel := func(p string, skipSpecial bool) string {
		p = strings.TrimSpace(p)
		if p == "" {
			return ""
		}
		if skipSpecial && (p == ":memory:" || strings.HasPrefix(p, "file:")) {
			return p
		}
		if filepath.IsAbs(p) {
			return filepath.Clean(p)
		}
		return filepath.Clean(filepath.Join(baseDir, p))
	}

	if md.IsDefined("db") {
		cfg.DB = resolveRel(cfg.DB, true)
	}
	if md.IsDefined("workspaces_dir") {
		cfg.WorkspacesDir = resolveRel(cfg.WorkspacesDir, false)
	}
	if md.IsDefined("flows_dir") {
		cfg.FlowsDir = resolveRel(cfg.FlowsDir, false)
		cfg.FlowsDirExplicit = true
	}
	if md.IsDefined("includes_dir") {
		cfg.IncludesDir = resolveRel(cfg.IncludesDir, false)
	}
	if md.IsDefined("env_file") {
		cfg.EnvFile = resolveRel(cfg.EnvFile, false)
	}
	return nil
}

func applyEnvVars(cfg *Config) error {
	if v := strings.TrimSpace(os.Getenv("FREUD_DB")); v != "" {
		cfg.DB = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_WORKSPACES_DIR")); v != "" {
		cfg.WorkspacesDir = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_FLOWS_DIR")); v != "" {
		cfg.FlowsDir = v
		cfg.FlowsDirExplicit = true
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_INCLUDES_DIR")); v != "" {
		cfg.IncludesDir = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_ENV_FILE")); v != "" {
		cfg.EnvFile = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_NO_CACHE")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_NO_CACHE %q: %w", v, err)
		}
		cfg.NoCache = b
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_PORT")); v != "" {
		p, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_PORT %q: %w", v, err)
		}
		cfg.Dream.Port = p
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SCHEDULER")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_SCHEDULER %q: %w", v, err)
		}
		cfg.Dream.Scheduler = b
		cfg.TUI.Scheduler = b
	}
	return nil
}

func applyOverrides(cfg *Config, ov Overrides) {
	if ov.DB != nil {
		cfg.DB = *ov.DB
	}
	if ov.WorkspacesDir != nil {
		cfg.WorkspacesDir = *ov.WorkspacesDir
	}
	if ov.FlowsDir != nil {
		cfg.FlowsDir = *ov.FlowsDir
		cfg.FlowsDirExplicit = true
	}
	if ov.IncludesDir != nil {
		cfg.IncludesDir = *ov.IncludesDir
	}
	if ov.EnvFile != nil {
		cfg.EnvFile = *ov.EnvFile
	}
	if ov.NoCache != nil {
		cfg.NoCache = *ov.NoCache
	}
	if ov.DreamPort != nil {
		cfg.Dream.Port = *ov.DreamPort
	}
	if ov.Scheduler != nil {
		cfg.Dream.Scheduler = *ov.Scheduler
		cfg.TUI.Scheduler = *ov.Scheduler
	}
}
