package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"freud/internal/db"
)

const (
	// DefaultConfigFile is the optional TOML configuration filename checked in the current directory.
	DefaultConfigFile = "freud.toml"

	DefaultDBPath            = "orchestrator.db"
	DefaultWorkspacesDir     = "workspaces"
	DefaultFlowsDir          = "flows"
	DefaultIncludesDir       = "includes"
	DefaultDreamPort         = 4321
	DefaultSyncBranch        = "main"
	DefaultSyncInterval      = "10m"
	DefaultSyncCacheDir      = ".freud-sync"
	DefaultRetentionInterval = "6h"
)

// StringList accepts either a single TOML string or a string array.
type StringList []string

// UnmarshalTOML decodes either a string or slice of strings into StringList.
func (s *StringList) UnmarshalTOML(data any) error {
	switch v := data.(type) {
	case string:
		trimmed := strings.TrimSpace(v)
		if trimmed != "" {
			*s = StringList{trimmed}
		} else {
			*s = nil
		}
		return nil
	case []any:
		out := make(StringList, 0, len(v))
		for _, item := range v {
			str, ok := item.(string)
			if !ok {
				return fmt.Errorf("expected string element in flows_path, got %T", item)
			}
			if trimmed := strings.TrimSpace(str); trimmed != "" {
				out = append(out, trimmed)
			}
		}
		*s = out
		return nil
	default:
		return fmt.Errorf("expected string or string array, got %T", data)
	}
}

// DreamConfig configures the embedded web UI server.
type DreamConfig struct {
	Port      int  `toml:"port"`
	Scheduler bool `toml:"scheduler"`
	NoAuth    bool `toml:"no_auth"`
}

// TUIConfig configures the interactive terminal UI.
type TUIConfig struct {
	Scheduler bool `toml:"scheduler"`
}

// SyncConfig configures Git repository flow synchronization.
type SyncConfig struct {
	Enabled       bool       `toml:"enabled"`
	Repo          string     `toml:"repo"`
	Branch        string     `toml:"branch"`
	Interval      string     `toml:"interval"`
	CacheDir      string     `toml:"cache_dir"`
	FlowsPath     StringList `toml:"flows_path"`
	IncludesPath  string     `toml:"includes_path"`
	Prune         bool       `toml:"prune"`
	Token         string     `toml:"token"`
	SSHKey        string     `toml:"ssh_key"`
	WebhookSecret string     `toml:"webhook_secret"`
}

// RetentionConfig configures automatic cleanup policies for old runs, logs, workspaces, and audit history.
type RetentionConfig struct {
	Interval       string `toml:"interval"`
	RunsDays       int    `toml:"runs_days"`
	WorkspacesDays int    `toml:"workspaces_days"`
	AuditDays      int    `toml:"audit_days"`
	VacuumSQLite   bool   `toml:"vacuum_sqlite"`
}

// Config holds runtime paths and behavior settings for Freud.
type Config struct {
	ConfigFile       string          `toml:"-"`
	FlowsDirExplicit bool            `toml:"-"`
	DB               string          `toml:"db"`
	WorkspacesDir    string          `toml:"workspaces_dir"`
	FlowsDir         string          `toml:"flows_dir"`
	IncludesDir      string          `toml:"includes_dir"`
	EnvFile          string          `toml:"env_file"`
	NoCache          bool            `toml:"no_cache"`
	AgentMadness     bool            `toml:"agent_madness"`
	Dream            DreamConfig     `toml:"dream"`
	TUI              TUIConfig       `toml:"tui"`
	Sync             SyncConfig      `toml:"sync"`
	Retention        RetentionConfig `toml:"retention"`
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
	AgentMadness  *bool
	DreamPort     *int
	Scheduler     *bool
	NoAuth        *bool
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
		Sync: SyncConfig{
			Branch:    DefaultSyncBranch,
			Interval:  DefaultSyncInterval,
			CacheDir:  DefaultSyncCacheDir,
			FlowsPath: StringList{DefaultFlowsDir},
		},
		Retention: RetentionConfig{
			Interval: DefaultRetentionInterval,
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
	if c.Sync.Enabled {
		if strings.TrimSpace(c.Sync.Repo) == "" {
			return fmt.Errorf("sync.repo cannot be empty when sync is enabled")
		}
		if strings.TrimSpace(c.Sync.Branch) == "" {
			return fmt.Errorf("sync.branch cannot be empty when sync is enabled")
		}
		if iv := strings.TrimSpace(c.Sync.Interval); iv != "" {
			d, err := time.ParseDuration(iv)
			if err != nil {
				return fmt.Errorf("invalid sync.interval %q: %w", iv, err)
			}
			if d < 0 {
				return fmt.Errorf("sync.interval must be >= 0 (got %q)", iv)
			}
		}
		for _, p := range c.Sync.FlowsPath {
			if err := validateRepoRelPath("sync.flows_path", p); err != nil {
				return err
			}
		}
		if strings.TrimSpace(c.Sync.IncludesPath) != "" {
			if err := validateRepoRelPath("sync.includes_path", c.Sync.IncludesPath); err != nil {
				return err
			}
		}
	}
	if iv := strings.TrimSpace(c.Retention.Interval); iv != "" {
		d, err := time.ParseDuration(iv)
		if err != nil {
			return fmt.Errorf("invalid retention.interval %q: %w", iv, err)
		}
		if d < 0 {
			return fmt.Errorf("retention.interval must be >= 0 (got %q)", iv)
		}
	}
	if c.Retention.RunsDays < 0 || c.Retention.WorkspacesDays < 0 || c.Retention.AuditDays < 0 {
		return fmt.Errorf("retention day limits must be >= 0")
	}
	return nil
}

func validateRepoRelPath(field, raw string) error {
	p := strings.TrimSpace(raw)
	if p == "" {
		return fmt.Errorf("%s entry cannot be empty", field)
	}
	if filepath.IsAbs(p) || strings.HasPrefix(p, "/") || strings.HasPrefix(p, "\\") {
		return fmt.Errorf("%s %q must be a relative path inside the repository", field, raw)
	}
	clean := filepath.ToSlash(filepath.Clean(p))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return fmt.Errorf("%s %q cannot traverse outside the repository", field, raw)
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
		if skipSpecial && (p == ":memory:" || strings.HasPrefix(p, "file:") || db.DetectDialect(p) != db.DialectSQLite) {
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
	if md.IsDefined("sync", "cache_dir") {
		cfg.Sync.CacheDir = resolveRel(cfg.Sync.CacheDir, false)
	}
	if md.IsDefined("sync", "ssh_key") {
		cfg.Sync.SSHKey = resolveRel(cfg.Sync.SSHKey, false)
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
	if v := strings.TrimSpace(os.Getenv("FREUD_AGENT_MADNESS")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_AGENT_MADNESS %q: %w", v, err)
		}
		cfg.AgentMadness = b
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
	if v := strings.TrimSpace(os.Getenv("FREUD_NO_AUTH")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_NO_AUTH %q: %w", v, err)
		}
		cfg.Dream.NoAuth = b
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_ENABLED")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_SYNC_ENABLED %q: %w", v, err)
		}
		cfg.Sync.Enabled = b
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_REPO")); v != "" {
		cfg.Sync.Repo = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_BRANCH")); v != "" {
		cfg.Sync.Branch = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_INTERVAL")); v != "" {
		cfg.Sync.Interval = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_CACHE_DIR")); v != "" {
		cfg.Sync.CacheDir = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_FLOWS_PATH")); v != "" {
		parts := strings.Split(v, ",")
		list := make(StringList, 0, len(parts))
		for _, part := range parts {
			if p := strings.TrimSpace(part); p != "" {
				list = append(list, p)
			}
		}
		cfg.Sync.FlowsPath = list
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_INCLUDES_PATH")); v != "" {
		cfg.Sync.IncludesPath = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_PRUNE")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_SYNC_PRUNE %q: %w", v, err)
		}
		cfg.Sync.Prune = b
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_TOKEN")); v != "" {
		cfg.Sync.Token = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_SSH_KEY")); v != "" {
		cfg.Sync.SSHKey = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_SYNC_WEBHOOK_SECRET")); v != "" {
		cfg.Sync.WebhookSecret = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_RETENTION_INTERVAL")); v != "" {
		cfg.Retention.Interval = v
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_RETENTION_RUNS_DAYS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_RETENTION_RUNS_DAYS %q: %w", v, err)
		}
		cfg.Retention.RunsDays = n
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_RETENTION_WORKSPACES_DAYS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_RETENTION_WORKSPACES_DAYS %q: %w", v, err)
		}
		cfg.Retention.WorkspacesDays = n
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_RETENTION_AUDIT_DAYS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_RETENTION_AUDIT_DAYS %q: %w", v, err)
		}
		cfg.Retention.AuditDays = n
	}
	if v := strings.TrimSpace(os.Getenv("FREUD_RETENTION_VACUUM_SQLITE")); v != "" {
		b, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid FREUD_RETENTION_VACUUM_SQLITE %q: %w", v, err)
		}
		cfg.Retention.VacuumSQLite = b
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
	if ov.AgentMadness != nil {
		cfg.AgentMadness = *ov.AgentMadness
	}
	if ov.DreamPort != nil {
		cfg.Dream.Port = *ov.DreamPort
	}
	if ov.Scheduler != nil {
		cfg.Dream.Scheduler = *ov.Scheduler
		cfg.TUI.Scheduler = *ov.Scheduler
	}
	if ov.NoAuth != nil {
		cfg.Dream.NoAuth = *ov.NoAuth
	}
}
