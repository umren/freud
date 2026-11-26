package syncer

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"freud/internal/config"
	"freud/internal/db"
)

func runCmd(t *testing.T, dir string, name string, args ...string) {
	t.Helper()
	cmd := exec.Command(name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"GIT_AUTHOR_NAME=Freud Test",
		"GIT_AUTHOR_EMAIL=test@example.com",
		"GIT_COMMITTER_NAME=Freud Test",
		"GIT_COMMITTER_EMAIL=test@example.com",
	)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v failed: %v\n%s", name, args, err, string(out))
	}
}

func TestSyncerCloneFlattenPreValidateAndPrune(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git binary not available")
	}

	tmp := t.TempDir()
	bareRepo := filepath.Join(tmp, "remote.git")
	workRepo := filepath.Join(tmp, "work")
	flowsDir := filepath.Join(tmp, "live_flows")
	includesDir := filepath.Join(tmp, "live_includes")
	cacheDir := filepath.Join(tmp, "cache_repo")

	runCmd(t, tmp, "git", "init", "--bare", bareRepo)
	runCmd(t, tmp, "git", "clone", bareRepo, workRepo)
	runCmd(t, workRepo, "git", "checkout", "-b", "main")

	// Commit 1: nested flow + root flow + include file.
	if err := os.MkdirAll(filepath.Join(workRepo, "pipelines", "billing"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(workRepo, "shared", "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	flow1 := "name: root-job\ntasks:\n  - id: step1\n    runtime: python\n    script: print(1)\n"
	flow2 := "name: billing-daily\ntasks:\n  - id: step1\n    runtime: python\n    script: print(2)\n"
	if err := os.WriteFile(filepath.Join(workRepo, "pipelines", "root.yaml"), []byte(flow1), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workRepo, "pipelines", "billing", "daily.yaml"), []byte(flow2), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workRepo, "shared", "sub", "lib.py"), []byte("X = 1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runCmd(t, workRepo, "git", "add", "-A")
	runCmd(t, workRepo, "git", "commit", "-m", "initial workflows")
	runCmd(t, workRepo, "git", "push", "-u", "origin", "main")

	store, err := db.Open(filepath.Join(tmp, "sync.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	cfg := config.Default()
	cfg.FlowsDir = flowsDir
	cfg.IncludesDir = includesDir
	cfg.Sync = config.SyncConfig{
		Enabled:      true,
		Repo:         bareRepo,
		Branch:       "main",
		Interval:     "10m",
		CacheDir:     cacheDir,
		FlowsPath:    config.StringList{"pipelines"},
		IncludesPath: "shared",
		Prune:        true,
	}

	s, err := New(store, cfg)
	if err != nil {
		t.Fatalf("New syncer: %v", err)
	}

	ctx := context.Background()
	res, err := s.Sync(ctx, Options{Trigger: "timer", Actor: "sync"})
	if err != nil || !res.Changed || len(res.Added) != 2 || res.RunID == "" {
		t.Fatalf("initial Sync mismatch: res=%+v err=%v", res, err)
	}
	if _, err := os.Stat(filepath.Join(flowsDir, "billing__daily.yaml")); err != nil {
		t.Fatalf("expected flattened billing__daily.yaml: %v", err)
	}
	if _, err := os.Stat(filepath.Join(includesDir, "sub", "lib.py")); err != nil {
		t.Fatalf("expected synced include sub/lib.py: %v", err)
	}

	// Second timer sync with no changes must NOT add a duplicate execution.
	res2, err := s.Sync(ctx, Options{Trigger: "timer", Actor: "sync"})
	if err != nil || res2.Changed || res2.RunID != "" {
		t.Fatalf("expected no-op timer sync, got res=%+v err=%v", res2, err)
	}

	// Commit 2: push broken YAML -> pre-validation rejects it, preserving live flows, and logs FAILED run.
	if err := os.WriteFile(filepath.Join(workRepo, "pipelines", "broken.yaml"), []byte("name: bad\ntasks: []\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runCmd(t, workRepo, "git", "add", "-A")
	runCmd(t, workRepo, "git", "commit", "-m", "broken flow")
	runCmd(t, workRepo, "git", "push", "origin", "main")

	badRes, badErr := s.Sync(ctx, Options{Trigger: "webhook", Actor: "webhook", ForceRecord: true})
	if badErr == nil || badRes.RunID == "" {
		t.Fatalf("expected broken YAML sync to fail and record run, res=%+v err=%v", badRes, badErr)
	}
	if _, err := os.Stat(filepath.Join(flowsDir, "broken.yaml")); !os.IsNotExist(err) {
		t.Fatalf("broken.yaml must NOT be copied into live flowsDir")
	}
	failedRun, err := store.GetRun(ctx, badRes.RunID)
	if err != nil || failedRun.Status != db.StatusFailed || failedRun.WorkflowName != WorkflowName {
		t.Fatalf("expected FAILED git-sync run in Executions, got %+v err=%v", failedRun, err)
	}

	// Commit 3: fix broken YAML and remove root.yaml -> prune removes root.yaml and records SUCCESS.
	_ = os.Remove(filepath.Join(workRepo, "pipelines", "root.yaml"))
	if err := os.WriteFile(filepath.Join(workRepo, "pipelines", "broken.yaml"), []byte(flow1), 0o644); err != nil {
		t.Fatal(err)
	}
	runCmd(t, workRepo, "git", "add", "-A")
	runCmd(t, workRepo, "git", "commit", "-m", "fix broken and delete root")
	runCmd(t, workRepo, "git", "push", "origin", "main")

	res3, err := s.Sync(ctx, Options{Trigger: "manual", Actor: "admin", ForceRecord: true})
	if err != nil || !res3.Changed || len(res3.Removed) != 1 || res3.Removed[0] != "root.yaml" {
		t.Fatalf("expected prune of root.yaml, res=%+v err=%v", res3, err)
	}
	if _, err := os.Stat(filepath.Join(flowsDir, "root.yaml")); !os.IsNotExist(err) {
		t.Fatalf("expected root.yaml pruned from live flowsDir")
	}
}

func TestWebhookVerificationAndSanitization(t *testing.T) {
	if got := SanitizeRepoURL("https://oauth2:secret@github.com/org/repo.git"); got != "https://github.com/org/repo.git" {
		t.Fatalf("unexpected sanitized URL: %q", got)
	}

	body := []byte(`{"ref":"refs/heads/main"}`)
	secret := "top-webhook-secret"
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "/webhooks/sync", strings.NewReader(string(body)))
	req.Header.Set("X-Hub-Signature-256", sig)
	branch, ok := VerifyWebhook(req, body, secret)
	if !ok || branch != "main" {
		t.Fatalf("expected GitHub signature valid for main, got ok=%v branch=%q", ok, branch)
	}

	reqBad := httptest.NewRequest(http.MethodPost, "/webhooks/sync", strings.NewReader(string(body)))
	reqBad.Header.Set("X-Hub-Signature-256", "sha256=deadbeef")
	if _, ok := VerifyWebhook(reqBad, body, secret); ok {
		t.Fatal("expected invalid signature rejected")
	}

	reqGL := httptest.NewRequest(http.MethodPost, "/webhooks/sync", strings.NewReader(`{"ref":"refs/heads/dev"}`))
	reqGL.Header.Set("X-Gitlab-Token", secret)
	branchGL, okGL := VerifyWebhook(reqGL, []byte(`{"ref":"refs/heads/dev"}`), secret)
	if !okGL || branchGL != "dev" {
		t.Fatalf("expected GitLab token valid for dev, got ok=%v branch=%q", okGL, branchGL)
	}
}
