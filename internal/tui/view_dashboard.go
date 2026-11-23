package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"freud/internal/db"
	"freud/internal/format"
)

func (m *Model) handleDashboardKey(key string) (tea.Model, tea.Cmd) {
	maxIdx := min(len(m.snap.Runs), 10) - 1
	switch key {
	case "up":
		if m.dashRunIdx > 0 {
			m.dashRunIdx--
		}
	case "down", "j":
		if m.dashRunIdx < maxIdx {
			m.dashRunIdx++
		}
	case "enter":
		if len(m.snap.Runs) > 0 && m.dashRunIdx < len(m.snap.Runs) {
			m.openExecutionDetail(m.snap.Runs[m.dashRunIdx].ID)
			return m, m.refreshCmd()
		}
	case "k":
		if len(m.snap.Runs) > 0 && m.dashRunIdx < len(m.snap.Runs) {
			r := m.snap.Runs[m.dashRunIdx]
			if r.Status == db.StatusRunning {
				return m, m.killRunCmd(r.ID)
			}
		}
	}
	return m, nil
}

func (m *Model) renderDashboard() string {
	s := m.snap.Stats
	cards := []string{
		m.renderStatCard("Total Runs", fmt.Sprintf("%d", s.Total), lipgloss.NewStyle().Bold(true).Foreground(colorText)),
		m.renderStatCard("Succeeded", fmt.Sprintf("%d", s.Success), styleSuccess),
		m.renderStatCard("Failed", fmt.Sprintf("%d", s.Failed), styleFailed),
		m.renderStatCard("Killed", fmt.Sprintf("%d", s.Killed), styleKilled),
		m.renderStatCard("Running", fmt.Sprintf("%d", s.Running), styleRunning),
		m.renderStatCard("Success Rate", m.snap.SuccessRate, styleSectionTitle),
		m.renderStatCard("Loaded Flows", fmt.Sprintf("%d", m.snap.FlowCount), lipgloss.NewStyle().Bold(true).Foreground(colorText)),
	}
	statsRow := lipgloss.JoinHorizontal(lipgloss.Top, cards...)

	var schedLines []string
	schedLines = append(schedLines, styleSectionTitle.Render("Upcoming Cron Schedules"))
	if len(m.snap.Upcoming) == 0 {
		schedLines = append(schedLines, styleMuted.Render("No cron-scheduled flows configured."))
	} else {
		limit := min(len(m.snap.Upcoming), 6)
		for i := 0; i < limit; i++ {
			u := m.snap.Upcoming[i]
			schedLines = append(schedLines, fmt.Sprintf("%-18s %-14s %s %s",
				truncateText(u.Name, 18),
				styleKeyHint.Render(u.Cron),
				styleMuted.Render(u.NextLabel),
				styleRunning.Render("in "+u.StartsIn),
			))
		}
	}

	schedW := max(38, m.width-30)
	if m.width < 88 {
		schedW = max(36, m.width-4)
	}
	schedPanel := stylePanel.Width(schedW).Render(strings.Join(schedLines, "\n"))

	var middleRow string
	if m.width >= 88 {
		artPanel := stylePanel.Width(25).Render(miniFreudArt())
		middleRow = lipgloss.JoinHorizontal(lipgloss.Top, artPanel, " ", schedPanel)
	} else {
		middleRow = schedPanel
	}

	var runLines []string
	runLines = append(runLines, styleSectionTitle.Render("Recent Executions (Enter: logs · k: kill active)"))
	if len(m.snap.Runs) == 0 {
		runLines = append(runLines, styleMuted.Render("No workflow runs recorded yet. Open [2 Flows] and press [r] to run one."))
	} else {
		now := time.Now()
		limit := min(len(m.snap.Runs), 8)
		for i := 0; i < limit; i++ {
			r := m.snap.Runs[i]
			dur := computeRunDuration(r, now)
			parentTag := ""
			if r.ParentRunID.Valid && r.ParentRunID.String != "" {
				parentTag = " [subflow]"
			}
			if i == m.dashRunIdx {
				row := fmt.Sprintf("> %-25s %-18s %-11s %8s  %s",
					truncateText(r.ID, 25),
					truncateText(r.WorkflowName+parentTag, 18),
					r.Status,
					dur,
					format.Timestamp(r.CreatedAt),
				)
				runLines = append(runLines, styleSelectedRow.Render(row))
			} else {
				row := fmt.Sprintf("  %-25s %-18s %s %8s  %s",
					truncateText(r.ID, 25),
					truncateText(r.WorkflowName+parentTag, 18),
					renderStatusBadge(r.Status),
					styleMuted.Render(dur),
					styleMuted.Render(format.Timestamp(r.CreatedAt)),
				)
				runLines = append(runLines, row)
			}
		}
	}
	runsPanel := stylePanel.Width(max(40, m.width-2)).Render(strings.Join(runLines, "\n"))

	return lipgloss.JoinVertical(lipgloss.Left, statsRow, middleRow, runsPanel)
}

func (m *Model) renderStatCard(label, val string, valStyle lipgloss.Style) string {
	cardW := max(13, (m.width-10)/7)
	content := styleMuted.Render(truncateText(label, cardW-4)) + "\n" + valStyle.Render(val)
	return styleCard.Width(cardW).Render(content)
}
