package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"

	"freud/internal/format"
)

var auditActionFilters = []string{"", "LOGIN", "LOGOUT", "FLOW_RUN", "FLOW_KILL", "USER_CREATED", "USER_DELETED", "TOKEN_CREATED", "TOKEN_REVOKED", "SETUP_ADMIN"}

func (m *Model) currentAuditFilter() string {
	return auditActionFilters[m.auditFilterIdx]
}

func (m *Model) handleAuditKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "up":
		if m.auditIdx > 0 {
			m.auditIdx--
		}
	case "down", "j":
		if m.auditIdx < len(m.snap.AuditLogs)-1 {
			m.auditIdx++
		}
	case "f":
		m.auditFilterIdx = (m.auditFilterIdx + 1) % len(auditActionFilters)
		m.auditPage = 0
		m.auditIdx = 0
		return m, m.refreshCmd()
	case "[", "pgup":
		if m.auditPage > 0 {
			m.auditPage--
			m.auditIdx = 0
			return m, m.refreshCmd()
		}
	case "]", "pgdown":
		if (m.auditPage+1)*tuiPageSize < m.snap.AuditTotal {
			m.auditPage++
			m.auditIdx = 0
			return m, m.refreshCmd()
		}
	}
	return m, nil
}

func (m *Model) renderAudit() string {
	filterLabel := m.currentAuditFilter()
	if filterLabel == "" {
		filterLabel = "ALL"
	}

	totalPages := max(1, (m.snap.AuditTotal+tuiPageSize-1)/tuiPageSize)
	curPage := min(totalPages, m.auditPage+1)

	var lines []string
	lines = append(lines, fmt.Sprintf("%s   %s   %s",
		styleSectionTitle.Render(fmt.Sprintf("User Activity & Audit Log (%d)", m.snap.AuditTotal)),
		styleKeyHint.Render("[f] Filter: "+filterLabel),
		styleKeyHint.Render(fmt.Sprintf("[/] Page %d/%d", curPage, totalPages)),
	))

	if len(m.snap.AuditLogs) == 0 {
		lines = append(lines, "")
		lines = append(lines, styleMuted.Render("No audit events recorded matching filter "+filterLabel+"."))
		return stylePanelFocused.Width(max(40, m.width-2)).Height(max(12, m.height-6)).Render(strings.Join(lines, "\n"))
	}

	maxVisible := max(6, m.height-10)
	start := 0
	if m.auditIdx >= maxVisible {
		start = m.auditIdx - maxVisible + 1
	}
	end := min(len(m.snap.AuditLogs), start+maxVisible)

	lines = append(lines, styleMuted.Render(fmt.Sprintf("  %-19s %-14s %-15s %-18s %-7s %s",
		"TIME", "USER", "ACTION", "TARGET", "SOURCE", "DETAILS")))

	for i := start; i < end; i++ {
		l := m.snap.AuditLogs[i]
		target := l.Target.String
		if target == "" {
			target = "—"
		}
		details := l.Details.String
		if details == "" {
			details = "—"
		}
		row := fmt.Sprintf("%-19s %-14s %-15s %-18s %-7s %s",
			format.Timestamp(l.CreatedAt),
			truncateText(l.Username, 14),
			truncateText(l.Action, 15),
			truncateText(target, 18),
			truncateText(l.Source, 7),
			truncateText(details, max(12, m.width-86)),
		)
		if i == m.auditIdx {
			lines = append(lines, styleSelectedRow.Render("> "+row))
		} else {
			lines = append(lines, "  "+row)
		}
	}

	return stylePanelFocused.Width(max(40, m.width-2)).Height(max(12, m.height-6)).Render(strings.Join(lines, "\n"))
}
