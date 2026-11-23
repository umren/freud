package tui

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"freud/internal/db"
)

type tabID int

const (
	tabDashboard tabID = iota
	tabFlows
	tabExecutions
)

const (
	refreshInterval     = 750 * time.Millisecond
	yamlPreviewMaxLines = 16
)

var execStatusFilters = []string{"", db.StatusRunning, db.StatusSuccess, db.StatusFailed, db.StatusKilled}

// Model implements tea.Model for the Freud interactive terminal UI.
type Model struct {
	cfg Config
	wg  *sync.WaitGroup

	width  int
	height int

	activeTab tabID
	snap      snapshot

	// Dashboard selection
	dashRunIdx int

	// Flows tab state
	flowIdx      int
	flowFullCode bool
	yamlVP       viewport.Model

	// Executions tab state
	filterIdx     int
	execIdx       int
	selectedRunID string // non-empty means Execution Detail & Logs view
	taskIdx       int
	childIdx      int
	autoFollow    bool
	lastLogLen    int
	logVP         viewport.Model

	// Transient feedback banner
	toastText  string
	toastIsErr bool
	toastUntil time.Time
}

// NewModel creates an initialized Bubbletea Model for Freud.
func NewModel(cfg Config) *Model {
	if cfg.FlowsDir == "" {
		cfg.FlowsDir = "flows"
	}
	if cfg.Ctx == nil {
		cfg.Ctx = context.Background()
	}

	return &Model{
		cfg:        cfg,
		wg:         &sync.WaitGroup{},
		width:      116,
		height:     34,
		activeTab:  tabDashboard,
		autoFollow: true,
		yamlVP:     viewport.New(80, 20),
		logVP:      viewport.New(80, 14),
	}
}

// Run launches the Bubbletea alt-screen program and waits for manually started runs upon exit.
func Run(cfg Config) error {
	m := NewModel(cfg)
	prog := tea.NewProgram(m, tea.WithAltScreen())
	_, err := prog.Run()
	m.wg.Wait()
	return err
}

func (m *Model) Init() tea.Cmd {
	return tea.Batch(
		m.refreshCmd(),
		tickCmd(),
	)
}

func tickCmd() tea.Cmd {
	return tea.Tick(refreshInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func (m *Model) currentFilter() string {
	return execStatusFilters[m.filterIdx]
}

func (m *Model) refreshCmd() tea.Cmd {
	return loadSnapshotCmd(m.cfg.Store, m.cfg.FlowsDir, m.currentFilter(), m.selectedRunID)
}

func (m *Model) setToast(text string, isErr bool) {
	m.toastText = text
	m.toastIsErr = isErr
	m.toastUntil = time.Now().Add(4 * time.Second)
}

func (m *Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		m.resizeViewports()
		m.syncYAMLViewport(false)
		m.syncLogViewport(false)
		return m, nil

	case tickMsg:
		if !m.toastUntil.IsZero() && time.Now().After(m.toastUntil) {
			m.toastText = ""
			m.toastUntil = time.Time{}
		}
		return m, tea.Batch(m.refreshCmd(), tickCmd())

	case snapshotMsg:
		prevLogLen := m.lastLogLen
		m.snap = msg.snap
		m.clampCursors()
		m.syncYAMLViewport(false)
		m.syncLogViewport(prevLogLen != m.currentTaskLogLen())
		return m, nil

	case runStartedMsg:
		if msg.err != nil {
			m.setToast(fmt.Sprintf("Failed to start %q: %v", msg.name, msg.err), true)
			return m, nil
		}
		m.setToast(fmt.Sprintf("Started %s (%s)", msg.name, msg.runID), false)
		m.openExecutionDetail(msg.runID)
		return m, m.refreshCmd()

	case runKilledMsg:
		if msg.err != nil {
			m.setToast(fmt.Sprintf("Kill %s failed: %v", msg.runID, msg.err), true)
			return m, nil
		}
		m.setToast(fmt.Sprintf("Kill signal sent to %s", msg.runID), false)
		return m, m.refreshCmd()

	case tea.KeyMsg:
		return m.handleKey(msg)
	}

	return m, nil
}

func (m *Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	if key == "ctrl+c" || key == "q" {
		return m, tea.Quit
	}

	// Handle full-screen YAML viewer modal first.
	if m.activeTab == tabFlows && m.flowFullCode {
		switch key {
		case "esc", "v", "enter":
			m.flowFullCode = false
			return m, nil
		case "r":
			return m, m.triggerSelectedFlow()
		case "g", "home":
			m.yamlVP.GotoTop()
			return m, nil
		case "G", "end":
			m.yamlVP.GotoBottom()
			return m, nil
		default:
			var cmd tea.Cmd
			m.yamlVP, cmd = m.yamlVP.Update(msg)
			return m, cmd
		}
	}

	switch key {
	case "1":
		m.activeTab = tabDashboard
		return m, m.refreshCmd()
	case "2":
		m.activeTab = tabFlows
		return m, m.refreshCmd()
	case "3":
		m.activeTab = tabExecutions
		return m, m.refreshCmd()
	case "tab":
		m.activeTab = (m.activeTab + 1) % 3
		return m, m.refreshCmd()
	case "R":
		return m, m.refreshCmd()
	}

	switch m.activeTab {
	case tabDashboard:
		return m.handleDashboardKey(key)
	case tabFlows:
		return m.handleFlowsKey(key)
	case tabExecutions:
		return m.handleExecutionsKey(msg)
	}
	return m, nil
}

func (m *Model) openExecutionDetail(runID string) {
	m.activeTab = tabExecutions
	m.selectedRunID = runID
	m.taskIdx = 0
	m.childIdx = 0
	m.autoFollow = true
	m.lastLogLen = 0
	m.resizeViewports()
}

func (m *Model) triggerSelectedFlow() tea.Cmd {
	if len(m.snap.Flows) == 0 || m.flowIdx >= len(m.snap.Flows) {
		return nil
	}
	entry := m.snap.Flows[m.flowIdx]
	if entry.ParseErr != nil || entry.Workflow == nil {
		m.setToast(fmt.Sprintf("Cannot run invalid flow %q", entry.FileName), true)
		return nil
	}
	if m.cfg.Runner == nil {
		m.setToast("Runner is not configured", true)
		return nil
	}

	wf := entry.Workflow
	ctx := m.cfg.Ctx
	runner := m.cfg.Runner
	wg := m.wg

	return func() tea.Msg {
		runID, doneCh, err := runner.StartAsync(ctx, wf)
		if err != nil {
			return runStartedMsg{name: wf.Name, err: err}
		}
		wg.Add(1)
		go func(ch <-chan error) {
			defer wg.Done()
			<-ch
		}(doneCh)
		return runStartedMsg{runID: runID, name: wf.Name}
	}
}

func (m *Model) killRunCmd(runID string) tea.Cmd {
	if m.cfg.Runner == nil {
		m.setToast("Runner is not configured", true)
		return nil
	}
	runner := m.cfg.Runner
	return func() tea.Msg {
		err := runner.Kill(runID)
		return runKilledMsg{runID: runID, err: err}
	}
}

func (m *Model) clampCursors() {
	if len(m.snap.Flows) == 0 {
		m.flowIdx = 0
	} else if m.flowIdx >= len(m.snap.Flows) {
		m.flowIdx = len(m.snap.Flows) - 1
	}

	if len(m.snap.Runs) == 0 {
		m.execIdx = 0
		m.dashRunIdx = 0
	} else {
		if m.execIdx >= len(m.snap.Runs) {
			m.execIdx = len(m.snap.Runs) - 1
		}
		maxDash := min(len(m.snap.Runs), 10) - 1
		if m.dashRunIdx > maxDash {
			m.dashRunIdx = maxDash
		}
	}

	if len(m.snap.Tasks) == 0 {
		m.taskIdx = 0
	} else if m.taskIdx >= len(m.snap.Tasks) {
		m.taskIdx = len(m.snap.Tasks) - 1
	}

	if len(m.snap.ChildRuns) == 0 {
		m.childIdx = 0
	} else if m.childIdx >= len(m.snap.ChildRuns) {
		m.childIdx = len(m.snap.ChildRuns) - 1
	}
}

func (m *Model) resizeViewports() {
	vpW := max(30, m.width-6)
	yamlH := max(8, m.height-10)
	m.yamlVP.Width = vpW
	m.yamlVP.Height = yamlH

	logH := max(6, m.height-16)
	if len(m.snap.ChildRuns) > 0 {
		logH = max(5, logH-2)
	}
	m.logVP.Width = vpW
	m.logVP.Height = logH
}

func (m *Model) syncYAMLViewport(resetTop bool) {
	if len(m.snap.Flows) == 0 || m.flowIdx >= len(m.snap.Flows) {
		m.yamlVP.SetContent("")
		return
	}
	entry := m.snap.Flows[m.flowIdx]
	rendered, _, _ := formatYAMLPreview(entry.RawYAML, 0, true)
	m.yamlVP.SetContent(rendered)
	if resetTop {
		m.yamlVP.GotoTop()
	}
}

func (m *Model) currentTaskLogLen() int {
	if len(m.snap.Tasks) == 0 || m.taskIdx >= len(m.snap.Tasks) {
		return 0
	}
	return len(m.snap.Tasks[m.taskIdx].Logs.String)
}

func (m *Model) syncLogViewport(logChanged bool) {
	m.resizeViewports()
	if len(m.snap.Tasks) == 0 || m.taskIdx >= len(m.snap.Tasks) {
		m.logVP.SetContent(styleMuted.Render("Waiting for tasks to start..."))
		m.lastLogLen = 0
		return
	}
	tr := m.snap.Tasks[m.taskIdx]
	rawLogs := tr.Logs.String
	m.lastLogLen = len(rawLogs)

	if strings.TrimSpace(rawLogs) == "" {
		if tr.Status == db.StatusRunning {
			m.logVP.SetContent(styleMuted.Render("Container is running — waiting for output..."))
		} else {
			m.logVP.SetContent(styleMuted.Render("No log output recorded for this task."))
		}
		return
	}

	m.logVP.SetContent(rawLogs)
	if m.autoFollow && (logChanged || (m.snap.SelectedRun != nil && m.snap.SelectedRun.Status == db.StatusRunning)) {
		m.logVP.GotoBottom()
	}
}

// View renders the full TUI screen.
func (m *Model) View() string {
	var parts []string
	parts = append(parts, m.renderHeader())

	var body string
	switch m.activeTab {
	case tabDashboard:
		body = m.renderDashboard()
	case tabFlows:
		body = m.renderFlows()
	case tabExecutions:
		body = m.renderExecutions()
	}
	parts = append(parts, body)

	if m.snap.Err != nil {
		parts = append(parts, styleFailed.Render(fmt.Sprintf("DB/Flow error: %v", m.snap.Err)))
	}
	if m.toastText != "" {
		if m.toastIsErr {
			parts = append(parts, styleToastErr.Render("▲ "+m.toastText))
		} else {
			parts = append(parts, styleToastOK.Render("✓ "+m.toastText))
		}
	}

	parts = append(parts, m.renderFooter())
	return lipgloss.JoinVertical(lipgloss.Left, parts...)
}

func (m *Model) renderHeader() string {
	tabs := []struct {
		id    tabID
		label string
	}{
		{tabDashboard, "1 Dashboard"},
		{tabFlows, "2 Flows"},
		{tabExecutions, "3 Executions"},
	}

	var tabBar []string
	for _, t := range tabs {
		if m.activeTab == t.id {
			tabBar = append(tabBar, styleTabActive.Render(t.label))
		} else {
			tabBar = append(tabBar, styleTabInactive.Render(t.label))
		}
	}

	brand := styleBrand.Render("FREUD TUI")
	sched := styleMuted.Render("scheduler: off")
	if m.cfg.SchedulerOn {
		sched = styleSuccess.Render("scheduler: on")
	}
	running := styleMuted.Render("running: 0")
	if m.snap.Stats.Running > 0 {
		running = styleRunning.Render(fmt.Sprintf("running: %d", m.snap.Stats.Running))
	}

	left := brand + "  " + lipgloss.JoinHorizontal(lipgloss.Top, tabBar...)
	right := running + "  " + styleMuted.Render("·") + "  " + sched
	gap := max(2, m.width-lipgloss.Width(left)-lipgloss.Width(right)-4)

	line := left + strings.Repeat(" ", gap) + right
	return styleHeader.Width(max(40, m.width)).Render(line)
}

func (m *Model) renderFooter() string {
	var hints []string
	hints = append(hints, styleKeyHint.Render("1/2/3/Tab")+" views")
	switch m.activeTab {
	case tabDashboard:
		hints = append(hints, styleKeyHint.Render("↑/↓")+" select", styleKeyHint.Render("Enter")+" logs", styleKeyHint.Render("k")+" kill")
	case tabFlows:
		if m.flowFullCode {
			hints = append(hints, styleKeyHint.Render("↑/↓ PgUp/PgDn g/G")+" scroll", styleKeyHint.Render("r")+" run", styleKeyHint.Render("v/Esc")+" close YAML")
		} else {
			hints = append(hints, styleKeyHint.Render("↑/↓")+" select", styleKeyHint.Render("v/Enter")+" full YAML", styleKeyHint.Render("r")+" run flow")
		}
	case tabExecutions:
		if m.selectedRunID == "" {
			hints = append(hints, styleKeyHint.Render("↑/↓")+" select", styleKeyHint.Render("f")+" filter", styleKeyHint.Render("Enter")+" logs", styleKeyHint.Render("k")+" kill")
		} else {
			hints = append(hints, styleKeyHint.Render("←/→")+" task", styleKeyHint.Render("↑/↓ g/G")+" scroll", styleKeyHint.Render("a")+" follow", styleKeyHint.Render("k")+" kill", styleKeyHint.Render("Esc")+" back")
		}
	}
	hints = append(hints, styleKeyHint.Render("R")+" refresh", styleKeyHint.Render("q")+" quit")
	return " " + strings.Join(hints, styleMuted.Render(" · "))
}
