package format

import (
	"strings"
	"testing"
	"time"
)

func TestTimestamp(t *testing.T) {
	got := Timestamp("2026-11-23T12:00:00Z")
	if len(got) != len("2006-01-02 15:04:05") || !strings.Contains(got, ":") {
		t.Fatalf("unexpected formatted timestamp: %q", got)
	}
	if invalid := Timestamp("not-a-timestamp"); invalid != "not-a-timestamp" {
		t.Fatalf("expected raw string passthrough, got %q", invalid)
	}
}

func TestDuration(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{-5 * time.Second, "0ms"},
		{42 * time.Millisecond, "42ms"},
		{2500 * time.Millisecond, "2.5s"},
		{2*time.Minute + 5*time.Second, "2m05s"},
	}
	for _, tc := range cases {
		if got := Duration(tc.in); got != tc.want {
			t.Fatalf("Duration(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestRelativeDuration(t *testing.T) {
	cases := []struct {
		in   time.Duration
		want string
	}{
		{0, "now"},
		{15 * time.Second, "15s"},
		{7 * time.Minute, "7m"},
		{2*time.Hour + 30*time.Minute, "2h 30m"},
		{26 * time.Hour, "1d 2h"},
	}
	for _, tc := range cases {
		if got := RelativeDuration(tc.in); got != tc.want {
			t.Fatalf("RelativeDuration(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestBytes(t *testing.T) {
	cases := []struct {
		in   int64
		want string
	}{
		{512, "512 B"},
		{2048, "2.0 KiB"},
		{5 * 1024 * 1024, "5.0 MiB"},
	}
	for _, tc := range cases {
		if got := Bytes(tc.in); got != tc.want {
			t.Fatalf("Bytes(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
