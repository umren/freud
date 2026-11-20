package cron

import (
	"testing"
	"time"
)

func TestParseAndNext(t *testing.T) {
	base := time.Date(2026, 11, 20, 10, 15, 30, 0, time.UTC)

	cases := []struct {
		expr string
		want time.Time
	}{
		{"*/15 * * * *", time.Date(2026, 11, 20, 10, 30, 0, 0, time.UTC)},
		{"0 */6 * * *", time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)},
		{"30 9 * * *", time.Date(2026, 11, 21, 9, 30, 0, 0, time.UTC)},
		{"@hourly", time.Date(2026, 11, 20, 11, 0, 0, 0, time.UTC)},
		{"@daily", time.Date(2026, 11, 21, 0, 0, 0, 0, time.UTC)},
		{"0 0 * * 7", time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)}, // Sunday
	}

	for _, tc := range cases {
		t.Run(tc.expr, func(t *testing.T) {
			sched, err := Parse(tc.expr)
			if err != nil {
				t.Fatalf("Parse(%q) failed: %v", tc.expr, err)
			}
			got := sched.Next(base)
			if !got.Equal(tc.want) {
				t.Fatalf("Next(%v) = %v, want %v", base, got, tc.want)
			}
		})
	}
}

func TestParseErrors(t *testing.T) {
	invalid := []string{
		"",
		"* * *",
		"60 * * * *",
		"* 25 * * *",
		"* * 0 * *",
		"* * * 13 *",
		"* * * * 8",
		"*/0 * * * *",
		"10-5 * * * *",
	}
	for _, expr := range invalid {
		if err := Validate(expr); err == nil {
			t.Fatalf("expected error for cron %q, got nil", expr)
		}
	}
}
