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

func (m *Model) handleExecutionsKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()
	if m.selectedRunID == "" {
		switch key {
		case "up":
			if m.execIdx > 0 {
				m.execIdx--
			}
		case "down", "j":
			if m.execIdx < len(m.snap.Runs)-1 {
				m.execIdx++
			}
		case "f":
			m.filterIdx = (m.filterIdx + 1) % len(execStatusFilters)
			m.execPage = 0
			m.execIdx = 0
			return m, m.refreshCmd()
		case "[", "pgup":
			if m.execPage > 0 {
				m.execPage--
				m.execIdx = 0
				return m, m.refreshCmd()
			}
		case "]", "pgdown":
			if (m.execPage+1)*tuiPageSize < m.snap.RunsTotal {
				m.execPage++
				m.execIdx = 0
				return m, m.refreshCmd()
			}
		case "enter":
			if len(m.snap.Runs) > 0 && m.execIdx < len(m.snap.Runs) {
				m.openExecutionDetail(m.snap.Runs[m.execIdx].ID)
				return m, m.refreshCmd()
			}
		case "k":
			if len(m.snap.Runs) > 0 && m.execIdx < len(m.snap.Runs) {
				r := m.snap.Runs[m.execIdx]
				if r.Status == db.StatusRunning {
					return m, m.killRunCmd(r.ID)
				}
			}
		}
		return m, nil
	}

	switch key {
	case "esc", "backspace", "b":
		m.selectedRunID = ""
		m.taskIdx = 0
		m.childIdx = 0
		m.lastLogLen = 0
		return m, m.refreshCmd()
	case "left", "[":
		if m.taskIdx > 0 {
			m.taskIdx--
			m.lastLogLen = 0
			m.syncLogViewport(true)
		}
		return m, nil
	case "right", "]":
		if m.taskIdx < len(m.snap.Tasks)-1 {
			m.taskIdx++
			m.lastLogLen = 0
			m.syncLogViewport(true)
		}
		return m, nil
	case "a":
		m.autoFollow = !m.autoFollow
		if m.autoFollow {
			m.logVP.GotoBottom()
		}
		return m, nil
	case "k":
		if m.snap.SelectedRun != nil && m.snap.SelectedRun.Status == db.StatusRunning {
			return m, m.killRunCmd(m.snap.SelectedRun.ID)
		}
		return m, nil
	case "p":
		if m.snap.SelectedRun != nil && m.snap.SelectedRun.ParentRunID.Valid && m.snap.SelectedRun.ParentRunID.String != "" {
			m.openExecutionDetail(m.snap.SelectedRun.ParentRunID.String)
			return m, m.refreshCmd()
		}
		return m, nil
	case "c":
		if len(m.snap.ChildRuns) > 0 {
			if m.childIdx >= len(m.snap.ChildRuns) {
				m.childIdx = 0
			}
			m.openExecutionDetail(m.snap.ChildRuns[m.childIdx].ID)
			return m, m.refreshCmd()
		}
		return m, nil
	case "C":
		if len(m.snap.ChildRuns) > 1 {
			m.childIdx = (m.childIdx + 1) % len(m.snap.ChildRuns)
		}
		return m, nil
	case "g", "home":
		m.autoFollow = false
		m.logVP.GotoTop()
		return m, nil
	case "G", "end":
		m.autoFollow = true
		m.logVP.GotoBottom()
		return m, nil
	case "up", "pgup":
		m.autoFollow = false
		var cmd tea.Cmd
		m.logVP, cmd = m.logVP.Update(msg)
		return m, cmd
	default:
		var cmd tea.Cmd
		m.logVP, cmd = m.logVP.Update(msg)
		return m, cmd
	}
}

func (m *Model) renderExecutions() string {
	if m.selectedRunID != "" {
		return m.renderExecutionDetail()
	}

	filterLabel := m.currentFilter()
	if filterLabel == "" {
		filterLabel = "ALL"
	}

	totalPages := max(1, (m.snap.RunsTotal+tuiPageSize-1)/tuiPageSize)
	curPage := min(totalPages, m.execPage+1)

	var lines []string
	lines = append(lines, fmt.Sprintf("%s   %s   %s",
		styleSectionTitle.Render(fmt.Sprintf("Execution History (%d)", m.snap.RunsTotal)),
		styleKeyHint.Render("[f] Filter: "+filterLabel),
		styleKeyHint.Render(fmt.Sprintf("[/] Page %d/%d", curPage, totalPages)),
	))

	if len(m.snap.Runs) == 0 {
		lines = append(lines, "")
		lines = append(lines, styleMuted.Render("No executions match filter "+filterLabel+"."))
		return stylePanelFocused.Width(max(40, m.width-2)).Height(max(12, m.height-6)).Render(strings.Join(lines, "\n"))
	}

	now := time.Now()
	maxVisible := max(6, m.height-10)
	start := 0
	if m.execIdx >= maxVisible {
		start = m.execIdx - maxVisible + 1
	}
	end := min(len(m.snap.Runs), start+maxVisible)

	lines = append(lines, styleMuted.Render(fmt.Sprintf("  %-25s %-18s %-11s %-8s  %s",
		"RUN ID", "FLOW", "STATUS", "DURATION", "STARTED")))

	for i := start; i < end; i++ {
		r := m.snap.Runs[i]
		dur := computeRunDuration(r, now)
		flowName := r.WorkflowName
		if r.ParentRunID.Valid && r.ParentRunID.String != "" {
			flowName += " [subflow]"
		}

		if i == m.execIdx {
			row := fmt.Sprintf("> %-25s %-18s %-11s %-8s  %s",
				truncateText(r.ID, 25),
				truncateText(flowName, 18),
				r.Status,
				dur,
				format.Timestamp(r.CreatedAt),
			)
			lines = append(lines, styleSelectedRow.Render(row))
		} else {
			row := fmt.Sprintf("  %-25s %-18s %s %-8s  %s",
				truncateText(r.ID, 25),
				truncateText(flowName, 18),
				renderStatusBadge(r.Status),
				styleMuted.Render(dur),
				styleMuted.Render(format.Timestamp(r.CreatedAt)),
			)
			lines = append(lines, row)
		}
	}

	return stylePanelFocused.Width(max(40, m.width-2)).Height(max(12, m.height-6)).Render(strings.Join(lines, "\n"))
}

func (m *Model) renderExecutionDetail() string {
	w := max(40, m.width-2)
	rec := m.snap.SelectedRun
	if rec == nil {
		return stylePanel.Width(w).Render(styleMuted.Render("Loading execution " + m.selectedRunID + "..."))
	}

	now := time.Now()
	dur := computeRunDuration(*rec, now)

	parentHint := ""
	if rec.ParentRunID.Valid && rec.ParentRunID.String != "" {
		parentHint = "  " + styleKeyHint.Render(fmt.Sprintf("Parent: #%s (%s) [p]", rec.ParentRunID.String, rec.ParentTaskID.String))
	}

	byHint := ""
	if rec.TriggeredBy.Valid && rec.TriggeredBy.String != "" {
		byHint = " · by=" + rec.TriggeredBy.String
	}

	head := fmt.Sprintf("%s  #%s  %s  %s%s",
		styleSectionTitle.Render(rec.WorkflowName),
		rec.ID,
		renderStatusBadge(rec.Status),
		styleMuted.Render("dur="+dur+byHint+" · started="+format.Timestamp(rec.CreatedAt)),
		parentHint,
	)

	var sections []string
	sections = append(sections, stylePanel.Width(w).Render(head))

	if len(m.snap.ChildRuns) > 0 {
		var childParts []string
		for i, cr := range m.snap.ChildRuns {
			lbl := fmt.Sprintf("%s→%s (%s)", cr.ParentTaskID.String, cr.WorkflowName, cr.Status)
			if i == m.childIdx {
				childParts = append(childParts, styleSelectedRow.Render(" "+lbl+" "))
			} else {
				childParts = append(childParts, styleMuted.Render(" "+lbl+" "))
			}
		}
		sections = append(sections, stylePanel.Width(w).Render(
			styleSectionTitle.Render(fmt.Sprintf("Subflows (%d): ", len(m.snap.ChildRuns)))+
				strings.Join(childParts, " ")+
				styleMuted.Render("  [c] open · [C] next"),
		))
	}

	var taskTabs []string
	for i, tr := range m.snap.Tasks {
		label := fmt.Sprintf("%s (%s)", tr.TaskID, tr.Status)
		if i == m.taskIdx {
			taskTabs = append(taskTabs, styleTabActive.Render(label))
		} else {
			taskTabs = append(taskTabs, styleTabInactive.Render(label))
		}
	}
	taskMeta := styleMuted.Render("No tasks recorded yet.")
	if len(m.snap.Tasks) > 0 && m.taskIdx < len(m.snap.Tasks) {
		tr := m.snap.Tasks[m.taskIdx]
		exitStr := "—"
		if tr.ExitCode.Valid {
			exitStr = fmt.Sprintf("%d", tr.ExitCode.Int64)
		}
		rt := tr.Runtime.String
		if tr.Image.String != "" {
			rt += " / " + tr.Image.String
		}
		followLabel := "off"
		if m.autoFollow {
			followLabel = "on"
		}
		taskMeta = fmt.Sprintf("%s  runtime=%s  exit=%s  dur=%s  %s",
			renderStatusBadge(tr.Status),
			styleMuted.Render(rt),
			styleMuted.Render(exitStr),
			styleMuted.Render(computeTaskDuration(tr, now)),
			styleKeyHint.Render("[a] auto-follow: "+followLabel),
		)
	}
	taskStrip := strings.Join(taskTabs, " ")
	if taskStrip == "" {
		taskStrip = styleMuted.Render("(pending)")
	}
	sections = append(sections, stylePanel.Width(w).Render(taskStrip+"\n"+taskMeta))

	logTitle := styleSectionTitle.Render("Task Logs") + "  " + styleMuted.Render(fmt.Sprintf("(%.0f%% · ←/→ switch task · ↑/↓ scroll · g/G top/bottom)", m.logVP.ScrollPercent()*100))
	sections = append(sections, stylePanelFocused.Width(w).Render(logTitle+"\n"+m.logVP.View()))

	return lipgloss.JoinVertical(lipgloss.Left, sections...)
}
