package term

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestBannerPlainAndColored(t *testing.T) {
	plain := Banner(false)
	if strings.Contains(plain, "\033[") {
		t.Fatalf("expected no ANSI codes in plain banner, got %q", plain)
	}
	if !strings.Contains(plain, "Local Workflow & Container Orchestrator") {
		t.Fatalf("expected tagline in plain banner")
	}
	if !strings.Contains(plain, "====*") {
		t.Fatalf("expected cigar in plain banner")
	}

	colored := Banner(true)
	if !strings.Contains(colored, "\033[") {
		t.Fatalf("expected ANSI codes in colored banner")
	}
	if !strings.Contains(colored, "====\033[0m") {
		t.Fatalf("expected colored cigar in banner")
	}
}

func TestHelpersAndEnvControls(t *testing.T) {
	if got := Tag(false, "engine"); got != "[engine]" {
		t.Fatalf("unexpected plain tag: %q", got)
	}
	if got := Tag(true, "engine"); !strings.Contains(got, "[engine]") || !strings.Contains(got, "\033[") {
		t.Fatalf("unexpected colored tag: %q", got)
	}
	if got := Status(true, "SUCCEEDED"); !strings.Contains(got, ansiGreen) {
		t.Fatalf("expected green status, got %q", got)
	}
	if got := Status(true, "FAILED"); !strings.Contains(got, ansiRed) {
		t.Fatalf("expected red status, got %q", got)
	}
	if got := Status(true, "KILLED"); !strings.Contains(got, ansiYellow) {
		t.Fatalf("expected yellow status, got %q", got)
	}

	t.Setenv("NO_COLOR", "1")
	t.Setenv("FORCE_COLOR", "1")
	if ColorEnabled(os.Stdout) {
		t.Fatalf("expected NO_COLOR to disable colors even with FORCE_COLOR")
	}

	t.Setenv("NO_COLOR", "")
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("FORCE_COLOR", "1")
	if !ColorEnabled(os.Stdout) {
		t.Fatalf("expected FORCE_COLOR to enable colors")
	}
	if ColorEnabled(nil) || ColorEnabledWriter(&bytes.Buffer{}) {
		t.Fatalf("expected non-file writer / nil file to disable colors")
	}
	if !strings.Contains(Key(true, "id"), ansiSoftKey) || Accent(false, "a") != "a" || Bold(false, "b") != "b" || Muted(false, "m") != "m" {
		t.Fatalf("unexpected style helper output")
	}
}
