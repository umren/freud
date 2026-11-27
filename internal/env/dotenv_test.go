package env

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseFile(t *testing.T) {
	dir := t.TempDir()
	envPath := filepath.Join(dir, ".env")
	content := `
# Comment line
PLAIN=hello
SPACED = world # inline comment
HASH_IN_VAL=abc#123
SINGLE='literal # value with $VAR'
DOUBLE="line1\nline2\t\"quoted\"" # trailing comment
export EXPORTED=yes
EMPTY=
`
	if err := os.WriteFile(envPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write .env: %v", err)
	}

	got, err := ParseFile(envPath)
	if err != nil {
		t.Fatalf("ParseFile failed: %v", err)
	}

	want := map[string]string{
		"PLAIN":       "hello",
		"SPACED":      "world",
		"HASH_IN_VAL": "abc#123",
		"SINGLE":      "literal # value with $VAR",
		"DOUBLE":      "line1\nline2\t\"quoted\"",
		"EXPORTED":    "yes",
		"EMPTY":       "",
	}

	if len(got) != len(want) {
		t.Fatalf("expected %d vars, got %d: %+v", len(want), len(got), got)
	}
	for k, wv := range want {
		if got[k] != wv {
			t.Fatalf("key %q: expected %q, got %q", k, wv, got[k])
		}
	}
}

func TestParseFileErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		wantErr string
	}{
		{
			name:    "missing equals",
			content: "INVALID_LINE\n",
			wantErr: "expected KEY=VALUE",
		},
		{
			name:    "unterminated single quote",
			content: "KEY='abc\n",
			wantErr: "unterminated single-quoted",
		},
		{
			name:    "unterminated double quote",
			content: "KEY=\"abc\n",
			wantErr: "unterminated double-quoted",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), ".env")
			if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
				t.Fatalf("write .env: %v", err)
			}
			_, err := ParseFile(path)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestLoadDefaultAndPrecedence(t *testing.T) {
	rootDir := t.TempDir()
	wfDir := filepath.Join(rootDir, "workflows")
	if err := os.MkdirAll(wfDir, 0o755); err != nil {
		t.Fatalf("mkdir workflows: %v", err)
	}

	if err := os.WriteFile(filepath.Join(wfDir, ".env"), []byte("A=from_wf\nB=from_wf\n"), 0o600); err != nil {
		t.Fatalf("write wf .env: %v", err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, ".env"), []byte("B=from_cwd\nC=from_cwd\n"), 0o600); err != nil {
		t.Fatalf("write cwd .env: %v", err)
	}

	origWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	t.Cleanup(func() { _ = os.Chdir(origWD) })
	if err := os.Chdir(rootDir); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	vars, loaded, err := LoadDefault(filepath.Join("workflows", "pipe.yaml"))
	if err != nil {
		t.Fatalf("LoadDefault failed: %v", err)
	}
	if len(loaded) != 2 {
		t.Fatalf("expected 2 loaded files, got %v", loaded)
	}
	if vars["A"] != "from_wf" || vars["B"] != "from_cwd" || vars["C"] != "from_cwd" {
		t.Fatalf("unexpected merged vars: %+v", vars)
	}
}

func TestMergeTaskEnv(t *testing.T) {
	base := map[string]string{
		"API_KEY": "secret-123",
		"MODE":    "staging",
	}
	task := map[string]string{
		"MODE":       "production",
		"AUTH_TOKEN": "Bearer ${API_KEY}",
		"LITERAL":    "keep-${UNKNOWN}-$raw",
	}

	merged := MergeTaskEnv(base, task)
	if merged["API_KEY"] != "secret-123" {
		t.Fatalf("expected inherited API_KEY, got %q", merged["API_KEY"])
	}
	if merged["MODE"] != "production" {
		t.Fatalf("expected task override for MODE, got %q", merged["MODE"])
	}
	if merged["AUTH_TOKEN"] != "Bearer secret-123" {
		t.Fatalf("expected expanded AUTH_TOKEN, got %q", merged["AUTH_TOKEN"])
	}
	if merged["LITERAL"] != "keep-${UNKNOWN}-$raw" {
		t.Fatalf("expected unknown/bare vars untouched, got %q", merged["LITERAL"])
	}
}

func TestValidateAndMergeRunEnv(t *testing.T) {
	for _, good := range []string{"A", "TARGET_DATE", "_PRIV_1", "a1_b2"} {
		clean, err := ValidateEnvKey("  " + good + " ")
		if err != nil || clean != good {
			t.Fatalf("expected valid key %q, got %q err=%v", good, clean, err)
		}
	}
	for _, bad := range []string{"", "1ABC", "BAD-KEY", "A=B", "HAS SPACE", "LINE\nBREAK"} {
		if _, err := ValidateEnvKey(bad); err == nil {
			t.Fatalf("expected invalid key %q to fail", bad)
		}
	}

	if _, err := ValidateEnvMap(map[string]string{"OK": "bad\x00val"}); err == nil {
		t.Fatal("expected NUL byte env value to fail")
	}
	if _, err := ValidateEnvMap(map[string]string{"DUP": "1", " DUP": "2"}); err == nil {
		t.Fatal("expected duplicate trimmed key to fail")
	}

	parsed, err := ParseEnvAssignments([]string{"DATE=2026-11-20", "EXPR=a=b=c"})
	if err != nil || parsed["DATE"] != "2026-11-20" || parsed["EXPR"] != "a=b=c" {
		t.Fatalf("unexpected ParseEnvAssignments result: %+v err=%v", parsed, err)
	}
	if _, err := ParseEnvAssignments([]string{"MISSING_EQ"}); err == nil {
		t.Fatal("expected missing '=' to fail")
	}

	lines, err := ParseEnvLines("# comment\n\nTARGET_DATE=2026-11-21\nEMPTY=\n")
	if err != nil || lines["TARGET_DATE"] != "2026-11-21" || lines["EMPTY"] != "" {
		t.Fatalf("unexpected ParseEnvLines result: %+v err=%v", lines, err)
	}

	merged := MergeRunEnv(
		map[string]string{"BASE": "base-v", "DATE": "from-dotenv"},
		map[string]string{"DATE": "default-in-yaml", "EXPANDED": "run=${DATE}", "FROM_BASE": "${BASE}"},
		map[string]string{"DATE": "2026-11-22", "EXTRA": "override"},
	)
	if merged["BASE"] != "base-v" || merged["DATE"] != "2026-11-22" || merged["EXPANDED"] != "run=2026-11-22" || merged["FROM_BASE"] != "base-v" || merged["EXTRA"] != "override" {
		t.Fatalf("unexpected MergeRunEnv result: %+v", merged)
	}
}
