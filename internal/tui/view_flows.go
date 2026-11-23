package tui

import (
	"fmt"
	"strings"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

func (m *Model) handleFlowsKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "up":
		if m.flowIdx > 0 {
			m.flowIdx--
			m.syncYAMLViewport(true)
		}
	case "down", "j":
		if m.flowIdx < len(m.snap.Flows)-1 {
			m.flowIdx++
			m.syncYAMLViewport(true)
		}
	case "v", "enter":
		if len(m.snap.Flows) > 0 {
			m.flowFullCode = true
			m.resizeViewports()
			m.syncYAMLViewport(true)
		}
	case "r":
		return m, m.triggerSelectedFlow()
	}
	return m, nil
}

func (m *Model) renderFlows() string {
	if len(m.snap.Flows) == 0 {
		return stylePanel.Width(max(40, m.width-2)).Render(
			styleSectionTitle.Render("Loaded Flows") + "\n\n" +
				styleMuted.Render(fmt.Sprintf("No YAML workflows found in %q.", m.cfg.FlowsDir)),
		)
	}

	entry := m.snap.Flows[m.flowIdx]

	if m.flowFullCode {
		title := fmt.Sprintf("%s  %s",
			styleSectionTitle.Render("Full YAML: "+entry.FileName),
			styleMuted.Render(fmt.Sprintf("(%d lines · scroll ↑/↓ PgUp/PgDn g/G · [Esc]/[v] back · [r] run)",
				strings.Count(entry.RawYAML, "\n")+1)),
		)
		return stylePanelFocused.Width(max(40, m.width-2)).Render(title + "\n\n" + m.yamlVP.View())
	}

	leftW := max(30, min(42, m.width/3))
	rightW := max(40, m.width-leftW-5)
	panelH := max(14, m.height-6)

	var leftLines []string
	leftLines = append(leftLines, styleSectionTitle.Render(fmt.Sprintf("Loaded Flows (%d)", len(m.snap.Flows))))
	for i, e := range m.snap.Flows {
		var meta string
		if e.ParseErr != nil {
			meta = styleFailed.Render("parse error")
		} else if e.Workflow != nil {
			statusPart := ""
			if last, ok := m.snap.LatestByFlow[e.Workflow.Name]; ok {
				statusPart = " · " + last.Status
			}
			cronPart := ""
			if e.Workflow.Cron != "" {
				cronPart = " · cron"
			}
			meta = fmt.Sprintf("%d tasks%s%s", len(e.Workflow.Tasks), cronPart, statusPart)
		}

		titleLine := truncateText(e.FileName, leftW-6)
		if e.Workflow != nil {
			titleLine = truncateText(e.Workflow.Name, leftW-6)
		}
		if i == m.flowIdx {
			leftLines = append(leftLines, styleSelectedRow.Render("> "+titleLine))
		} else {
			leftLines = append(leftLines, "  "+titleLine)
		}
		leftLines = append(leftLines, "  "+styleMuted.Render(truncateText(e.FileName+" · "+meta, leftW-6)))
	}
	leftPanel := stylePanelFocused.Width(leftW).Height(panelH).Render(strings.Join(leftLines, "\n"))

	var rightLines []string
	if entry.ParseErr != nil {
		rightLines = append(rightLines, styleFailed.Render("Parse Error in "+entry.FileName))
		rightLines = append(rightLines, styleFailed.Render(entry.ParseErr.Error()))
	} else if entry.Workflow != nil {
		wf := entry.Workflow
		cronLabel := "manual"
		if wf.Cron != "" {
			cronLabel = wf.Cron
		}
		rightLines = append(rightLines, fmt.Sprintf("%s  %s  %s",
			styleSectionTitle.Render(wf.Name),
			styleMuted.Render("("+entry.FilePath+")"),
			styleKeyHint.Render("cron: "+cronLabel),
		))

		var taskTags []string
		for _, t := range wf.Tasks {
			if t.Flow != "" {
				taskTags = append(taskTags, fmt.Sprintf("%s(subflow→%s)", t.ID, t.Flow))
			} else if t.Image != "" {
				taskTags = append(taskTags, fmt.Sprintf("%s(%s)", t.ID, t.Image))
			} else {
				taskTags = append(taskTags, fmt.Sprintf("%s(%s)", t.ID, t.Runtime))
			}
		}
		rightLines = append(rightLines, styleMuted.Render("Tasks: ")+truncateText(strings.Join(taskTags, " -> "), rightW-10))
		rightLines = append(rightLines, "")
	}

	previewLimit := max(6, min(yamlPreviewMaxLines, panelH-7))
	preview, totalLines, truncated := formatYAMLPreview(entry.RawYAML, previewLimit, false)
	headerLine := styleSectionTitle.Render(fmt.Sprintf("YAML Code (%d lines)", totalLines))
	if truncated {
		headerLine += "  " + styleMuted.Render("[v] / [Enter] expand full code")
	}
	rightLines = append(rightLines, headerLine)
	rightLines = append(rightLines, preview)

	rightPanel := stylePanel.Width(rightW).Height(panelH).Render(strings.Join(rightLines, "\n"))
	return lipgloss.JoinHorizontal(lipgloss.Top, leftPanel, " ", rightPanel)
}
