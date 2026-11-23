package ui

import (
	"html/template"
	"time"

	"freud/internal/format"
)

type codeLine struct {
	Number int
	HTML   template.HTML
}

func formatTimestamp(raw string) string {
	return format.Timestamp(raw)
}

func formatDuration(d time.Duration) string {
	return format.Duration(d)
}

func formatRelativeDuration(d time.Duration) string {
	return format.RelativeDuration(d)
}

func formatHumanBytes(bytes int64) string {
	return format.Bytes(bytes)
}
