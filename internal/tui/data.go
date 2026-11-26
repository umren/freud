package tui

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"

	"freud/internal/cron"
	"freud/internal/db"
	"freud/internal/format"
	"freud/internal/workflow"
)

// Runner describes the execution and kill operations required by the TUI.
type Runner interface {
	StartAsync(ctx context.Context, wf *workflow.Workflow) (string, <-chan error, error)
	Kill(runID string) error
}

// Config configures the Freud terminal UI.
type Config struct {
	Store       *db.Store
	FlowsDir    string
	Runner      Runner
	Ctx         context.Context
	SchedulerOn bool
}

type upcomingSchedule struct {
	Name      string
	FileName  string
	Cron      string
	NextRunAt time.Time
	NextLabel string
	StartsIn  string
}

const tuiPageSize = 50

type snapshot struct {
	LoadedAt     time.Time
	Stats        db.RunStats
	SuccessRate  string
	FlowCount    int
	Flows        []workflow.WorkflowEntry
	LatestByFlow map[string]db.RunRecord
	Upcoming     []upcomingSchedule
	Runs         []db.RunRecord
	RunsTotal    int
	SelectedRun  *db.RunRecord
	Tasks        []db.TaskRunRecord
	ChildRuns    []db.RunRecord
	AuditLogs    []db.AuditLogRecord
	AuditTotal   int
	Err          error
}

type snapshotMsg struct {
	snap snapshot
}

type runStartedMsg struct {
	runID string
	name  string
	err   error
}

type runKilledMsg struct {
	runID string
	err   error
}

type tickMsg time.Time

func loadSnapshotCmd(store *db.Store, flowsDir, statusFilter, selectedRunID, auditFilter string, execPage, auditPage int) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var out snapshot
		out.LoadedAt = time.Now()

		stats, err := store.GetRunStats(ctx)
		if err != nil {
			out.Err = err
			return snapshotMsg{snap: out}
		}
		out.Stats = stats

		completed := stats.Success + stats.Failed
		if completed > 0 {
			out.SuccessRate = fmt.Sprintf("%.0f%%", float64(stats.Success)*100.0/float64(completed))
		} else {
			out.SuccessRate = "—"
		}

		entries, err := workflow.LoadDir(flowsDir)
		if err != nil {
			out.Err = err
			return snapshotMsg{snap: out}
		}
		out.Flows = entries

		latest, err := store.GetLatestRunByWorkflow(ctx)
		if err != nil {
			out.Err = err
			return snapshotMsg{snap: out}
		}
		out.LatestByFlow = latest

		now := time.Now()
		for _, entry := range entries {
			if entry.Workflow == nil {
				continue
			}
			out.FlowCount++
			if entry.Workflow.Cron == "" {
				continue
			}
			sched, err := cron.Parse(entry.Workflow.Cron)
			if err != nil {
				continue
			}
			next := sched.Next(now)
			if next.IsZero() {
				continue
			}
			out.Upcoming = append(out.Upcoming, upcomingSchedule{
				Name:      entry.Workflow.Name,
				FileName:  entry.FileName,
				Cron:      entry.Workflow.Cron,
				NextRunAt: next,
				NextLabel: next.Format("2006-01-02 15:04"),
				StartsIn:  format.RelativeDuration(next.Sub(now)),
			})
		}
		sort.Slice(out.Upcoming, func(i, j int) bool {
			return out.Upcoming[i].NextRunAt.Before(out.Upcoming[j].NextRunAt)
		})

		runs, runsTotal, err := store.ListRunsPage(ctx, db.RunQuery{
			Status: statusFilter,
			Limit:  tuiPageSize,
			Offset: max(0, execPage) * tuiPageSize,
		})
		if err != nil {
			out.Err = err
			return snapshotMsg{snap: out}
		}
		out.Runs = runs
		out.RunsTotal = runsTotal

		if selectedRunID != "" {
			rec, err := store.GetRun(ctx, selectedRunID)
			if err == nil {
				out.SelectedRun = rec
				tasks, err := store.ListTaskRuns(ctx, selectedRunID)
				if err == nil {
					out.Tasks = tasks
				}
				children, err := store.ListChildRuns(ctx, selectedRunID)
				if err == nil {
					out.ChildRuns = children
				}
			} else if !strings.Contains(err.Error(), "no rows") {
				out.Err = err
			}
		}

		if auditLogs, auditTotal, err := store.ListAuditLogsPage(ctx, db.AuditQuery{
			Action: auditFilter,
			Limit:  tuiPageSize,
			Offset: max(0, auditPage) * tuiPageSize,
		}); err == nil {
			out.AuditLogs = auditLogs
			out.AuditTotal = auditTotal
		}

		return snapshotMsg{snap: out}
	}
}
