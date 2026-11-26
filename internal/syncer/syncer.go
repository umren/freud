package syncer

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"freud/internal/config"
	"freud/internal/db"
	"freud/internal/workflow"
)

const (
	// WorkflowName is the synthetic execution workflow_name recorded in runs for visibility in Executions/TUI.
	WorkflowName = "git-sync"
)

// Options controls a single Sync execution.
type Options struct {
	Trigger     string // timer, manual, webhook, api, mcp
	Actor       string
	Details     string
	ForceRecord bool // record a git-sync execution even if commit did not change
}

// Result describes the outcome of a Sync run.
type Result struct {
	Changed   bool     `json:"changed"`
	Commit    string   `json:"commit,omitempty"`
	CommitMsg string   `json:"commit_msg,omitempty"`
	Added     []string `json:"added,omitempty"`
	Updated   []string `json:"updated,omitempty"`
	Removed   []string `json:"removed,omitempty"`
	RunID     string   `json:"run_id,omitempty"`
	Message   string   `json:"message"`
}

// Status captures current state exposed to Web UI and REST API.
type Status struct {
	Enabled        bool   `json:"enabled"`
	Repo           string `json:"repo,omitempty"`
	Branch         string `json:"branch,omitempty"`
	Interval       string `json:"interval,omitempty"`
	WebhookEnabled bool   `json:"webhook_enabled"`
	Prune          bool   `json:"prune"`
	LastCommit     string `json:"last_commit,omitempty"`
	LastCheckAt    string `json:"last_check_at,omitempty"`
	LastSyncAt     string `json:"last_sync_at,omitempty"`
	LastError      string `json:"last_error,omitempty"`
	LastRunID      string `json:"last_run_id,omitempty"`
}

// Syncer pulls workflows and shared includes from a Git repository into Freud's local directories.
type Syncer struct {
	store       *db.Store
	cfg         config.SyncConfig
	flowsDir    string
	includesDir string
	interval    time.Duration

	mu     sync.Mutex
	status Status
}

// New constructs a Git Syncer for cfg.Sync targeting flowsDir and includesDir.
func New(store *db.Store, cfg config.Config) (*Syncer, error) {
	sc := cfg.Sync
	if strings.TrimSpace(sc.Branch) == "" {
		sc.Branch = config.DefaultSyncBranch
	}
	if len(sc.FlowsPath) == 0 {
		sc.FlowsPath = config.StringList{config.DefaultFlowsDir}
	}
	if strings.TrimSpace(sc.CacheDir) == "" {
		sc.CacheDir = config.DefaultSyncCacheDir
	}

	var interval time.Duration
	if iv := strings.TrimSpace(sc.Interval); iv != "" {
		d, err := time.ParseDuration(iv)
		if err != nil {
			return nil, fmt.Errorf("parse sync interval %q: %w", iv, err)
		}
		interval = d
	}

	return &Syncer{
		store:       store,
		cfg:         sc,
		flowsDir:    cfg.FlowsDir,
		includesDir: cfg.IncludesDir,
		interval:    interval,
		status: Status{
			Enabled:        sc.Enabled,
			Repo:           SanitizeRepoURL(sc.Repo),
			Branch:         sc.Branch,
			Interval:       sc.Interval,
			WebhookEnabled: strings.TrimSpace(sc.WebhookSecret) != "",
			Prune:          sc.Prune,
		},
	}, nil
}

// Enabled reports whether Git sync is active.
func (s *Syncer) Enabled() bool {
	return s != nil && s.cfg.Enabled
}

// WebhookSecret returns the configured webhook verification secret.
func (s *Syncer) WebhookSecret() string {
	if s == nil {
		return ""
	}
	return s.cfg.WebhookSecret
}

// Status returns a thread-safe snapshot of the sync state without exposing credentials.
func (s *Syncer) Status() Status {
	if s == nil {
		return Status{}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.status
}

// RunPeriodic triggers an initial sync and then polls on cfg.Interval until ctx is cancelled.
func (s *Syncer) RunPeriodic(ctx context.Context) {
	if !s.Enabled() {
		return
	}
	_, _ = s.Sync(ctx, Options{Trigger: "timer", Actor: "sync"})
	if s.interval <= 0 {
		<-ctx.Done()
		return
	}

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, _ = s.Sync(ctx, Options{Trigger: "timer", Actor: "sync"})
		}
	}
}

// Sync synchronizes the configured repository branch into FlowsDir (and optional IncludesDir).
func (s *Syncer) Sync(ctx context.Context, opts Options) (*Result, error) {
	if !s.Enabled() {
		return nil, fmt.Errorf("git sync is disabled")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if strings.TrimSpace(opts.Actor) == "" {
		opts.Actor = "sync"
	}
	if strings.TrimSpace(opts.Trigger) == "" {
		opts.Trigger = "manual"
	}

	nowStr := time.Now().UTC().Format(time.RFC3339Nano)
	s.status.LastCheckAt = nowStr

	var logBuf strings.Builder
	fmt.Fprintf(&logBuf, "repo: %s\nbranch: %s\ntrigger: %s (%s)\n",
		SanitizeRepoURL(s.cfg.Repo), s.cfg.Branch, opts.Trigger, opts.Actor)

	if _, err := exec.LookPath("git"); err != nil {
		msg := "git binary not found in PATH"
		runID := s.recordFailure(ctx, opts, &logBuf, msg, errors.New(msg))
		return &Result{RunID: runID, Message: msg}, errors.New(msg)
	}

	gitEnv, cleanupCreds, err := s.buildGitEnv()
	if err != nil {
		runID := s.recordFailure(ctx, opts, &logBuf, "credential setup failed", err)
		return &Result{RunID: runID, Message: err.Error()}, err
	}
	defer cleanupCreds()

	firstClone := false
	prevCommit := ""
	if _, err := os.Stat(filepath.Join(s.cfg.CacheDir, ".git")); os.IsNotExist(err) {
		firstClone = true
		if err := os.MkdirAll(filepath.Dir(s.cfg.CacheDir), 0o755); err != nil {
			runID := s.recordFailure(ctx, opts, &logBuf, "create cache parent dir", err)
			return &Result{RunID: runID, Message: err.Error()}, err
		}
		out, cloneErr := s.runGit(ctx, gitEnv, "", "clone", "--depth", "1", "--single-branch", "--branch", s.cfg.Branch, s.cfg.Repo, s.cfg.CacheDir)
		logBuf.WriteString(s.redactSecrets(out))
		if cloneErr != nil {
			runID := s.recordFailure(ctx, opts, &logBuf, "git clone failed", cloneErr)
			return &Result{RunID: runID, Message: s.redactSecrets(cloneErr.Error())}, cloneErr
		}
	} else {
		prevCommit, _ = s.runGit(ctx, gitEnv, s.cfg.CacheDir, "rev-parse", "HEAD")
		prevCommit = strings.TrimSpace(prevCommit)

		out, fetchErr := s.runGit(ctx, gitEnv, s.cfg.CacheDir, "fetch", "--depth", "1", "origin", s.cfg.Branch)
		logBuf.WriteString(s.redactSecrets(out))
		if fetchErr != nil {
			runID := s.recordFailure(ctx, opts, &logBuf, "git fetch failed", fetchErr)
			return &Result{RunID: runID, Message: s.redactSecrets(fetchErr.Error())}, fetchErr
		}
		out, resetErr := s.runGit(ctx, gitEnv, s.cfg.CacheDir, "reset", "--hard", "FETCH_HEAD")
		logBuf.WriteString(s.redactSecrets(out))
		if resetErr != nil {
			runID := s.recordFailure(ctx, opts, &logBuf, "git reset failed", resetErr)
			return &Result{RunID: runID, Message: s.redactSecrets(resetErr.Error())}, resetErr
		}
	}

	newCommitRaw, err := s.runGit(ctx, gitEnv, s.cfg.CacheDir, "rev-parse", "HEAD")
	if err != nil {
		runID := s.recordFailure(ctx, opts, &logBuf, "read HEAD commit", err)
		return &Result{RunID: runID, Message: err.Error()}, err
	}
	newCommit := strings.TrimSpace(newCommitRaw)
	shortSHA := newCommit
	if len(shortSHA) > 12 {
		shortSHA = shortSHA[:12]
	}

	commitMsgRaw, _ := s.runGit(ctx, gitEnv, s.cfg.CacheDir, "log", "-1", "--format=%s (%an)")
	commitMsg := strings.TrimSpace(commitMsgRaw)
	fmt.Fprintf(&logBuf, "commit: %s\nsubject: %s\n", newCommit, commitMsg)

	if !firstClone && prevCommit == newCommit {
		s.status.LastCommit = newCommit
		s.status.LastError = ""
		msg := fmt.Sprintf("Already up to date at %s", shortSHA)
		fmt.Fprintln(&logBuf, msg)

		var runID string
		if opts.ForceRecord {
			runID = s.recordSuccessRun(ctx, opts, logBuf.String(), shortSHA)
			s.status.LastRunID = runID
			s.status.LastSyncAt = nowStr
			_ = s.store.RecordAudit(ctx, opts.Actor, "FLOW_SYNC", s.status.Repo+"@"+s.cfg.Branch, syncAuditSource(opts), fmt.Sprintf("commit=%s no_changes run_id=%s", shortSHA, runID))
		}
		return &Result{
			Changed:   false,
			Commit:    newCommit,
			CommitMsg: commitMsg,
			RunID:     runID,
			Message:   msg,
		}, nil
	}

	// Stage and pre-validate YAML workflows before touching live FlowsDir.
	stagedFlows, err := s.collectFlattenedFlows()
	if err != nil {
		runID := s.recordFailure(ctx, opts, &logBuf, "collect flows failed", err)
		return &Result{Commit: newCommit, CommitMsg: commitMsg, RunID: runID, Message: err.Error()}, err
	}
	if err := validateStagedFlows(stagedFlows); err != nil {
		runID := s.recordFailure(ctx, opts, &logBuf, "pre-validation failed (live flows preserved)", err)
		return &Result{Commit: newCommit, CommitMsg: commitMsg, RunID: runID, Message: err.Error()}, err
	}

	added, updated, removed, err := applyFlowFiles(s.flowsDir, stagedFlows, s.cfg.Prune)
	if err != nil {
		runID := s.recordFailure(ctx, opts, &logBuf, "apply flows failed", err)
		return &Result{Commit: newCommit, CommitMsg: commitMsg, RunID: runID, Message: err.Error()}, err
	}

	incSummary := ""
	if strings.TrimSpace(s.cfg.IncludesPath) != "" {
		incAdded, incUpdated, incRemoved, incErr := s.syncIncludes()
		if incErr != nil {
			runID := s.recordFailure(ctx, opts, &logBuf, "sync includes failed", incErr)
			return &Result{Commit: newCommit, CommitMsg: commitMsg, RunID: runID, Message: incErr.Error()}, incErr
		}
		incSummary = fmt.Sprintf("includes: +%d ~%d -%d\n", incAdded, incUpdated, incRemoved)
	}

	fmt.Fprintf(&logBuf, "flows synced: added=%v updated=%v removed=%v\n%s", added, updated, removed, incSummary)
	s.status.LastCommit = newCommit
	s.status.LastSyncAt = nowStr
	s.status.LastError = ""

	runID := s.recordSuccessRun(ctx, opts, logBuf.String(), shortSHA)
	s.status.LastRunID = runID

	details := fmt.Sprintf("commit=%s +%d ~%d -%d run_id=%s", shortSHA, len(added), len(updated), len(removed), runID)
	if opts.Details != "" {
		details += ", " + opts.Details
	}
	_ = s.store.RecordAudit(ctx, opts.Actor, "FLOW_SYNC", s.status.Repo+"@"+s.cfg.Branch, syncAuditSource(opts), details)

	return &Result{
		Changed:   true,
		Commit:    newCommit,
		CommitMsg: commitMsg,
		Added:     added,
		Updated:   updated,
		Removed:   removed,
		RunID:     runID,
		Message:   fmt.Sprintf("Synced commit %s (+%d ~%d -%d)", shortSHA, len(added), len(updated), len(removed)),
	}, nil
}

// VerifyWebhook checks shared-secret signatures (GitHub HMAC-SHA256, Gitea HMAC-SHA256,
// GitLab X-Gitlab-Token, or Authorization Bearer) and extracts the pushed branch when present.
func VerifyWebhook(r *http.Request, body []byte, secret string) (targetBranch string, matches bool) {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		return "", false
	}

	// 1. HMAC-SHA256 headers (GitHub: X-Hub-Signature-256: sha256=<hex>, Gitea: X-Gitea-Signature: <hex>).
	sigHex := ""
	if hdr := strings.TrimSpace(r.Header.Get("X-Hub-Signature-256")); strings.HasPrefix(strings.ToLower(hdr), "sha256=") {
		sigHex = strings.TrimSpace(hdr[7:])
	} else if hdr := strings.TrimSpace(r.Header.Get("X-Gitea-Signature")); hdr != "" {
		sigHex = strings.TrimPrefix(strings.ToLower(hdr), "sha256=")
	}
	if sigHex != "" {
		gotMAC, err := hex.DecodeString(sigHex)
		if err != nil {
			return "", false
		}
		mac := hmac.New(sha256.New, []byte(secret))
		_, _ = mac.Write(body)
		expectedMAC := mac.Sum(nil)
		if !hmac.Equal(gotMAC, expectedMAC) {
			return "", false
		}
		return extractWebhookBranch(body), true
	}

	// 2. Plain token headers (GitLab: X-Gitlab-Token, generic: Authorization: Bearer <secret>).
	candidate := strings.TrimSpace(r.Header.Get("X-Gitlab-Token"))
	if candidate == "" {
		if authHdr := strings.TrimSpace(r.Header.Get("Authorization")); len(authHdr) > 7 && strings.EqualFold(authHdr[:7], "bearer ") {
			candidate = strings.TrimSpace(authHdr[7:])
		}
	}
	if candidate == "" || subtle.ConstantTimeCompare([]byte(candidate), []byte(secret)) != 1 {
		return "", false
	}
	return extractWebhookBranch(body), true
}

func extractWebhookBranch(body []byte) string {
	var payload struct {
		Ref string `json:"ref"`
	}
	if len(bytes.TrimSpace(body)) == 0 || json.Unmarshal(body, &payload) != nil {
		return ""
	}
	ref := strings.TrimSpace(payload.Ref)
	if ref == "" {
		return ""
	}
	return strings.TrimPrefix(ref, "refs/heads/")
}

func (s *Syncer) collectFlattenedFlows() (map[string][]byte, error) {
	staged := make(map[string][]byte)
	for _, relBase := range s.cfg.FlowsPath {
		srcDir := filepath.Join(s.cfg.CacheDir, filepath.FromSlash(relBase))
		info, err := os.Stat(srcDir)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("configured flows_path %q does not exist in repository", relBase)
			}
			return nil, err
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("configured flows_path %q is not a directory", relBase)
		}

		err = filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() {
				if d.Name() == ".git" {
					return filepath.SkipDir
				}
				return nil
			}
			ext := strings.ToLower(filepath.Ext(d.Name()))
			if ext != ".yaml" && ext != ".yml" {
				return nil
			}
			rel, err := filepath.Rel(srcDir, path)
			if err != nil {
				return err
			}
			flat := strings.ReplaceAll(filepath.ToSlash(rel), "/", "__")
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if _, exists := staged[flat]; exists {
				return fmt.Errorf("flattened flow filename collision on %q across configured flows_path entries", flat)
			}
			staged[flat] = data
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return staged, nil
}

func validateStagedFlows(staged map[string][]byte) error {
	tmpDir, err := os.MkdirTemp("", "freud-sync-stage-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmpDir)

	for name, data := range staged {
		if err := os.WriteFile(filepath.Join(tmpDir, name), data, 0o644); err != nil {
			return err
		}
	}
	entries, err := workflow.LoadDir(tmpDir)
	if err != nil {
		return err
	}
	var errs []string
	for _, e := range entries {
		if e.ParseErr != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", e.FileName, e.ParseErr))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("invalid workflow(s): %s", strings.Join(errs, "; "))
	}
	return nil
}

func applyFlowFiles(dstDir string, staged map[string][]byte, prune bool) (added, updated, removed []string, err error) {
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return nil, nil, nil, err
	}

	names := make([]string, 0, len(staged))
	for name := range staged {
		names = append(names, name)
	}
	sort.Strings(names)

	for _, name := range names {
		dstPath := filepath.Join(dstDir, name)
		data := staged[name]
		prev, readErr := os.ReadFile(dstPath)
		switch {
		case os.IsNotExist(readErr):
			if err := writeAtomic(dstPath, data); err != nil {
				return nil, nil, nil, err
			}
			added = append(added, name)
		case readErr != nil:
			return nil, nil, nil, readErr
		case !bytes.Equal(prev, data):
			if err := writeAtomic(dstPath, data); err != nil {
				return nil, nil, nil, err
			}
			updated = append(updated, name)
		}
	}

	if prune {
		dirEntries, err := os.ReadDir(dstDir)
		if err != nil {
			return nil, nil, nil, err
		}
		for _, de := range dirEntries {
			if de.IsDir() {
				continue
			}
			ext := strings.ToLower(filepath.Ext(de.Name()))
			if ext != ".yaml" && ext != ".yml" {
				continue
			}
			if _, keep := staged[de.Name()]; !keep {
				if err := os.Remove(filepath.Join(dstDir, de.Name())); err != nil {
					return nil, nil, nil, err
				}
				removed = append(removed, de.Name())
			}
		}
		sort.Strings(removed)
	}

	return added, updated, removed, nil
}

func (s *Syncer) syncIncludes() (added, updated, removed int, err error) {
	srcDir := filepath.Join(s.cfg.CacheDir, filepath.FromSlash(s.cfg.IncludesPath))
	if info, statErr := os.Stat(srcDir); os.IsNotExist(statErr) {
		return 0, 0, 0, nil
	} else if statErr != nil {
		return 0, 0, 0, statErr
	} else if !info.IsDir() {
		return 0, 0, 0, fmt.Errorf("includes_path %q is not a directory", s.cfg.IncludesPath)
	}

	if err := os.MkdirAll(s.includesDir, 0o755); err != nil {
		return 0, 0, 0, err
	}

	seen := make(map[string]struct{})
	err = filepath.WalkDir(srcDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)
		seen[relSlash] = struct{}{}

		dstPath := filepath.Join(s.includesDir, rel)
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		prev, readErr := os.ReadFile(dstPath)
		switch {
		case os.IsNotExist(readErr):
			if err := writeAtomic(dstPath, data); err != nil {
				return err
			}
			added++
		case readErr != nil:
			return readErr
		case !bytes.Equal(prev, data):
			if err := writeAtomic(dstPath, data); err != nil {
				return err
			}
			updated++
		}
		return nil
	})
	if err != nil {
		return 0, 0, 0, err
	}

	if s.cfg.Prune {
		_ = filepath.WalkDir(s.includesDir, func(path string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() {
				return nil
			}
			rel, err := filepath.Rel(s.includesDir, path)
			if err != nil {
				return nil
			}
			if _, keep := seen[filepath.ToSlash(rel)]; !keep {
				if rmErr := os.Remove(path); rmErr == nil {
					removed++
				}
			}
			return nil
		})
	}
	return added, updated, removed, nil
}

func writeAtomic(dst string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".sync-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, dst)
}

func (s *Syncer) buildGitEnv() ([]string, func(), error) {
	env := append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
	var cleanups []func()

	if tok := strings.TrimSpace(s.cfg.Token); tok != "" {
		f, err := os.CreateTemp("", "freud-askpass-*")
		if err != nil {
			return nil, nil, err
		}
		scriptPath := f.Name()
		cleanups = append(cleanups, func() { _ = os.Remove(scriptPath) })

		script := "#!/bin/sh\ncase \"$1\" in\n  *Username*) printf '%s\\n' \"x-access-token\" ;;\n  *) printf '%s\\n' \"$FREUD_GIT_TOKEN_VALUE\" ;;\nesac\n"
		if _, err := f.WriteString(script); err != nil {
			_ = f.Close()
			return nil, nil, err
		}
		_ = f.Close()
		if err := os.Chmod(scriptPath, 0o700); err != nil {
			return nil, nil, err
		}
		env = append(env, "GIT_ASKPASS="+scriptPath, "FREUD_GIT_TOKEN_VALUE="+tok)
	}

	if key := strings.TrimSpace(s.cfg.SSHKey); key != "" {
		env = append(env, fmt.Sprintf("GIT_SSH_COMMAND=ssh -i %s -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new", shellQuote(key)))
	}

	return env, func() {
		for _, fn := range cleanups {
			fn()
		}
	}, nil
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'"
}

func (s *Syncer) runGit(ctx context.Context, env []string, dir string, args ...string) (string, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, "git", args...)
	if dir != "" {
		cmd.Dir = dir
	}
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return string(out), fmt.Errorf("%v: %s", err, strings.TrimSpace(s.redactSecrets(string(out))))
	}
	return string(out), nil
}

func (s *Syncer) redactSecrets(text string) string {
	if tok := strings.TrimSpace(s.cfg.Token); tok != "" {
		text = strings.ReplaceAll(text, tok, "***")
	}
	return text
}

func (s *Syncer) recordSuccessRun(ctx context.Context, opts Options, logs, shortSHA string) string {
	runID, err := s.createSyncRun(ctx, db.StatusSuccess, opts.Actor, logs, 0)
	if err != nil {
		return ""
	}
	_ = shortSHA
	return runID
}

func (s *Syncer) recordFailure(ctx context.Context, opts Options, logBuf *strings.Builder, msg string, err error) string {
	cleanErr := s.redactSecrets(err.Error())
	fmt.Fprintf(logBuf, "ERROR (%s): %s\n", msg, cleanErr)
	s.status.LastError = msg + ": " + cleanErr
	runID, _ := s.createSyncRun(ctx, db.StatusFailed, opts.Actor, logBuf.String(), 1)
	if runID != "" {
		s.status.LastRunID = runID
	}
	details := cleanErr
	if runID != "" {
		details = fmt.Sprintf("run_id=%s %s", runID, cleanErr)
	}
	_ = s.store.RecordAudit(ctx, opts.Actor, "FLOW_SYNC_FAILED", s.status.Repo+"@"+s.cfg.Branch, syncAuditSource(opts), details)
	return runID
}

func (s *Syncer) createSyncRun(ctx context.Context, status, actor, logs string, exitCode int64) (string, error) {
	if s.store == nil {
		return "", nil
	}
	runID := newSyncRunID()
	if err := s.store.CreateRunFull(ctx, runID, WorkflowName, "", "", "", actor); err != nil {
		return "", err
	}
	trID, err := s.store.CreateTaskRunWithMeta(ctx, runID, WorkflowName, "git", s.cfg.Branch)
	if err == nil {
		_ = s.store.FinishTaskRun(ctx, trID, status, exitCode, logs)
	}
	_ = s.store.FinishRun(ctx, runID, status)
	return runID, nil
}

func syncAuditSource(opts Options) string {
	switch opts.Trigger {
	case "api", "mcp", "dream", "webhook", "timer":
		if opts.Trigger == "timer" {
			return "sync"
		}
		return opts.Trigger
	default:
		return "sync"
	}
}

func newSyncRunID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102-150405"), hex.EncodeToString(b[:]))
}

// SanitizeRepoURL strips embedded credentials from HTTP/HTTPS git URLs before display.
func SanitizeRepoURL(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" || u.User == nil {
		return raw
	}
	u.User = nil
	return u.String()
}
