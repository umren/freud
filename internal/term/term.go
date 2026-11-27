package term

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/mattn/go-isatty"
)

const (
	ansiReset   = "\033[0m"
	ansiBold    = "\033[1m"
	ansiDim     = "\033[90m"
	ansiRed     = "\033[1;31m"
	ansiGreen   = "\033[1;32m"
	ansiYellow  = "\033[1;33m"
	ansiCyan    = "\033[1;36m"
	ansiViolet  = "\033[1;38;5;141m"
	ansiAccent  = "\033[38;5;141m"
	ansiEmber   = "\033[1;38;5;208m"
	ansiSmoke   = "\033[38;5;246m"
	ansiCigar   = "\033[38;5;130m"
	ansiWhite   = "\033[1;97m"
	ansiSoftKey = "\033[36m"
)

// ColorEnabled reports whether ANSI color escape codes should be emitted for f.
func ColorEnabled(f *os.File) bool {
	if f == nil {
		return false
	}
	if os.Getenv("NO_COLOR") != "" || strings.EqualFold(os.Getenv("TERM"), "dumb") {
		return false
	}
	if os.Getenv("FORCE_COLOR") != "" {
		return true
	}
	fd := f.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
}

// StdoutColor reports whether ANSI color is enabled on os.Stdout.
func StdoutColor() bool {
	return ColorEnabled(os.Stdout)
}

// StderrColor reports whether ANSI color is enabled on os.Stderr.
func StderrColor() bool {
	return ColorEnabled(os.Stderr)
}

// ColorEnabledWriter reports whether ANSI color is enabled for an arbitrary io.Writer.
func ColorEnabledWriter(w io.Writer) bool {
	if f, ok := w.(*os.File); ok {
		return ColorEnabled(f)
	}
	return false
}

func wrap(color bool, code, text string) string {
	if !color || text == "" {
		return text
	}
	return code + text + ansiReset
}

// Tag formats a bracketed component prefix such as "[engine]" or "[runner]".
func Tag(color bool, name string) string {
	label := "[" + name + "]"
	switch name {
	case "freud", "dream", "engine":
		return wrap(color, ansiViolet, label)
	case "runner", "cache", "scheduler":
		return wrap(color, ansiCyan, label)
	default:
		return wrap(color, ansiBold, label)
	}
}

// Status formats an execution status or severity label with semantic color.
func Status(color bool, status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case "SUCCEEDED", "SUCCESS":
		return wrap(color, ansiGreen, status)
	case "FAILED", "ERROR":
		return wrap(color, ansiRed, status)
	case "KILLED", "WARNING":
		return wrap(color, ansiYellow, status)
	case "RUNNING":
		return wrap(color, ansiCyan, status)
	case "SKIPPED":
		return wrap(color, ansiDim, status)
	default:
		return wrap(color, ansiBold, status)
	}
}

// Key highlights identifiers, workflow names, URLs, and command names.
func Key(color bool, text string) string {
	return wrap(color, ansiSoftKey, text)
}

// Accent highlights brand or primary emphasis text.
func Accent(color bool, text string) string {
	return wrap(color, ansiViolet, text)
}

// Bold emphasizes text.
func Bold(color bool, text string) string {
	return wrap(color, ansiBold, text)
}

// Muted renders secondary metadata in dim gray.
func Muted(color bool, text string) string {
	return wrap(color, ansiDim, text)
}

// Banner returns the Freud ASCII portrait (with cigar) and wordmark.
func Banner(color bool) string {
	// Build line-by-line so portrait, cigar/smoke, and wordmark stay aligned in plain and colored modes.
	var b strings.Builder
	b.WriteString("\n")
	fmt.Fprintf(&b, "%s%s\n",
		wrap(color, ansiWhite, "            .---\"\"\"---.             "),
		wrap(color, ansiViolet, " _____ ____  _____ _   _ ____"),
	)
	fmt.Fprintf(&b, "%s%s%s\n",
		wrap(color, ansiWhite, "           /  ~     ~  \\    "),
		wrap(color, ansiSmoke, "(  )    "),
		wrap(color, ansiViolet, "|  ___|  _ \\| ____| | | |  _ \\"),
	)
	fmt.Fprintf(&b, "%s%s%s\n",
		wrap(color, ansiWhite, "          |  (o)===(o)  |    "),
		wrap(color, ansiSmoke, ") )    "),
		wrap(color, ansiViolet, "| |_  | |_) |  _| | | | | | | |"),
	)
	fmt.Fprintf(&b, "%s%s%s\n",
		wrap(color, ansiWhite, "          |      L      |   "),
		wrap(color, ansiSmoke, "( (     "),
		wrap(color, ansiViolet, "|  _| |  _ <| |___| |_| | |_| |"),
	)
	fmt.Fprintf(&b, "%s%s%s\n",
		wrap(color, ansiWhite, "          |   .-----.   |    "),
		wrap(color, ansiSmoke, ") )    "),
		wrap(color, ansiViolet, "|_|   |_| \\_\\_____|\\___/|____/"),
	)
	fmt.Fprintf(&b, "%s%s%s\n",
		wrap(color, ansiWhite, "           \\  `====='  /"),
		wrap(color, ansiCigar, "====="),
		wrap(color, ansiEmber, "*"),
	)
	fmt.Fprintf(&b, "%s%s\n",
		wrap(color, ansiWhite, "            `._______.'             "),
		wrap(color, ansiBold, "Local Workflow & Container Orchestrator"),
	)
	fmt.Fprintf(&b, "%s%s\n",
		wrap(color, ansiDim, "           .-`-.   .-'-.            "),
		wrap(color, ansiMutedOrAccent(color), "Docker · SQLite · DAG · Subflows · Dream UI"),
	)
	fmt.Fprintf(&b, "%s\n\n",
		wrap(color, ansiDim, "          /   / \\ / \\   \\"),
	)
	return b.String()
}

func ansiMutedOrAccent(color bool) string {
	if !color {
		return ""
	}
	return ansiAccent
}
