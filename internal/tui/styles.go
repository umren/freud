package tui

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"freud/internal/db"
	"freud/internal/format"
)

var (
	colorAccent  = lipgloss.Color("#a78bfa")
	colorPrimary = lipgloss.Color("#7c3aed")
	colorBorder  = lipgloss.Color("#313a4f")
	colorMuted   = lipgloss.Color("#8b95ab")
	colorText    = lipgloss.Color("#e6ebf5")
	colorSurface = lipgloss.Color("#141821")
	colorSuccess = lipgloss.Color("#4ade80")
	colorFailed  = lipgloss.Color("#f87171")
	colorKilled  = lipgloss.Color("#fbbf24")
	colorRunning = lipgloss.Color("#38bdf8")

	styleHeader = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorText).
			Background(colorSurface).
			Padding(0, 1)

	styleBrand = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorAccent)

	styleTabActive = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("#ffffff")).
			Background(colorPrimary).
			Padding(0, 2)

	styleTabInactive = lipgloss.NewStyle().
				Foreground(colorMuted).
				Padding(0, 2)

	stylePanel = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorBorder).
			Padding(0, 1)

	stylePanelFocused = lipgloss.NewStyle().
				Border(lipgloss.RoundedBorder()).
				BorderForeground(colorAccent).
				Padding(0, 1)

	styleCard = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(colorBorder).
			Padding(0, 1)

	styleSectionTitle = lipgloss.NewStyle().
				Bold(true).
				Foreground(colorAccent)

	styleMuted = lipgloss.NewStyle().
			Foreground(colorMuted)

	styleKeyHint = lipgloss.NewStyle().
			Bold(true).
			Foreground(colorAccent)

	styleSelectedRow = lipgloss.NewStyle().
				Bold(true).
				Foreground(lipgloss.Color("#ffffff")).
				Background(lipgloss.Color("#2b2349"))

	styleSuccess = lipgloss.NewStyle().Bold(true).Foreground(colorSuccess)
	styleFailed  = lipgloss.NewStyle().Bold(true).Foreground(colorFailed)
	styleKilled  = lipgloss.NewStyle().Bold(true).Foreground(colorKilled)
	styleRunning = lipgloss.NewStyle().Bold(true).Foreground(colorRunning)

	styleToastOK  = lipgloss.NewStyle().Bold(true).Foreground(colorSuccess)
	styleToastErr = lipgloss.NewStyle().Bold(true).Foreground(colorFailed)

	styleLineNo  = lipgloss.NewStyle().Foreground(lipgloss.Color("#525b70"))
	styleYAMLKey = lipgloss.NewStyle().Foreground(lipgloss.Color("#7dd3fc"))
	styleYAMLCom = lipgloss.NewStyle().Foreground(lipgloss.Color("#64748b")).Italic(true)
	styleYAMLStr = lipgloss.NewStyle().Foreground(lipgloss.Color("#86efac"))
	styleYAMLVar = lipgloss.NewStyle().Foreground(lipgloss.Color("#f472b6"))
)

func renderStatusBadge(status string) string {
	switch strings.ToUpper(strings.TrimSpace(status)) {
	case db.StatusSuccess, "SUCCEEDED":
		return styleSuccess.Render("● " + status)
	case db.StatusFailed:
		return styleFailed.Render("● " + status)
	case db.StatusKilled:
		return styleKilled.Render("● " + status)
	case db.StatusRunning:
		return styleRunning.Render("● " + status)
	case db.StatusSkipped:
		return styleMuted.Render("⊘ " + status)
	default:
		return styleMuted.Render("○ " + status)
	}
}

func computeRunDuration(r db.RunRecord, now time.Time) string {
	startT, startErr := time.Parse(time.RFC3339Nano, r.CreatedAt)
	if startErr != nil {
		return "—"
	}
	if r.FinishedAt.Valid {
		if endT, err := time.Parse(time.RFC3339Nano, r.FinishedAt.String); err == nil {
			return format.Duration(endT.Sub(startT))
		}
	}
	if r.Status == db.StatusRunning {
		return format.Duration(now.Sub(startT))
	}
	return "—"
}

func computeTaskDuration(tr db.TaskRunRecord, now time.Time) string {
	startT, err := time.Parse(time.RFC3339Nano, tr.StartedAt)
	if err != nil {
		return "—"
	}
	if tr.FinishedAt.Valid {
		if endT, err := time.Parse(time.RFC3339Nano, tr.FinishedAt.String); err == nil {
			return format.Duration(endT.Sub(startT))
		}
	}
	if tr.Status == db.StatusRunning {
		return format.Duration(now.Sub(startT))
	}
	return "—"
}

// formatYAMLPreview renders numbered, lightly highlighted YAML lines, truncating when maxLines > 0.
func formatYAMLPreview(raw string, maxLines int, showAll bool) (string, int, bool) {
	raw = strings.ReplaceAll(raw, "\r\n", "\n")
	lines := strings.Split(raw, "\n")
	total := len(lines)

	limit := total
	truncated := false
	if !showAll && maxLines > 0 && total > maxLines {
		limit = maxLines
		truncated = true
	}

	var b strings.Builder
	for i := 0; i < limit; i++ {
		if i > 0 {
			b.WriteByte('\n')
		}
		ln := styleLineNo.Render(fmt.Sprintf("%3d │ ", i+1))
		b.WriteString(ln)
		b.WriteString(highlightYAMLLine(lines[i]))
	}
	if truncated {
		b.WriteByte('\n')
		b.WriteString(styleKilled.Render(fmt.Sprintf("... (%d more lines hidden — press [v] or [Enter] to view full YAML)", total-limit)))
	}
	return b.String(), total, truncated
}

func highlightYAMLLine(line string) string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return line
	}
	if strings.HasPrefix(trimmed, "#") {
		return styleYAMLCom.Render(line)
	}
	if strings.Contains(line, "${") {
		return styleYAMLVar.Render(line)
	}
	if idx := strings.IndexByte(line, ':'); idx > 0 {
		prefix := line[:idx]
		if !strings.ContainsAny(strings.TrimSpace(prefix), `"'`) {
			rest := line[idx+1:]
			if strings.HasPrefix(strings.TrimSpace(rest), `"`) || strings.HasPrefix(strings.TrimSpace(rest), `'`) {
				return styleYAMLKey.Render(prefix+":") + styleYAMLStr.Render(rest)
			}
			return styleYAMLKey.Render(prefix+":") + rest
		}
	}
	if strings.HasPrefix(trimmed, "- ") {
		indent := line[:len(line)-len(strings.TrimLeft(line, " \t"))]
		return indent + styleKeyHint.Render("- ") + strings.TrimPrefix(trimmed, "- ")
	}
	return line
}

func truncateText(s string, maxW int) string {
	if maxW <= 0 {
		return ""
	}
	r := []rune(s)
	if len(r) <= maxW {
		return s
	}
	if maxW <= 1 {
		return string(r[:maxW])
	}
	return string(r[:maxW-1]) + "…"
}

func miniFreudArt() string {
	smoke := lipgloss.NewStyle().Foreground(colorMuted)
	cigar := lipgloss.NewStyle().Foreground(lipgloss.Color("#b45309"))
	ember := lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("#fb923c"))
	face := lipgloss.NewStyle().Foreground(colorText)

	lines := []string{
		face.Render("   .---\"\"\"---."),
		face.Render("  /  ~     ~  \\  ") + smoke.Render("( )"),
		face.Render(" |  (o)===(o)  | ") + smoke.Render(" )"),
		face.Render(" |      L      | ") + smoke.Render("( "),
		face.Render(" |   .-----.   | ") + smoke.Render(" )"),
		face.Render("  \\  `====='  /") + cigar.Render("====") + ember.Render("*"),
		face.Render("   `._______.'"),
	}
	return strings.Join(lines, "\n")
}
