package ui

import (
	"fmt"
	"net/http"
	"sort"
	"time"

	"freud/internal/cron"
	"freud/internal/db"
	"freud/internal/workflow"
)

type upcomingScheduleView struct {
	Name             string
	FileName         string
	Cron             string
	NextRunAt        time.Time
	NextRunFormatted string
	StartsIn         string
}

type dashboardViewData struct {
	basePageData
	Stats       db.RunStats
	SuccessRate string
	FlowCount   int
	Upcoming    []upcomingScheduleView
	RecentRuns  []runRowView
}

func (s *Server) buildDashboardData(r *http.Request) (dashboardViewData, error) {
	ctx := r.Context()
	stats, err := s.store.GetRunStats(ctx)
	if err != nil {
		return dashboardViewData{}, err
	}

	entries, err := workflow.LoadDir(s.cfg.FlowsDir)
	if err != nil {
		return dashboardViewData{}, err
	}

	recentRaw, err := s.store.ListRuns(ctx, 8, "")
	if err != nil {
		return dashboardViewData{}, err
	}

	now := time.Now()
	var upcoming []upcomingScheduleView
	validFlows := 0
	for _, e := range entries {
		if e.Workflow == nil {
			continue
		}
		validFlows++
		if e.Workflow.Cron == "" {
			continue
		}
		sched, err := cron.Parse(e.Workflow.Cron)
		if err != nil {
			continue
		}
		next := sched.Next(now)
		if next.IsZero() {
			continue
		}
		upcoming = append(upcoming, upcomingScheduleView{
			Name:             e.Workflow.Name,
			FileName:         e.FileName,
			Cron:             e.Workflow.Cron,
			NextRunAt:        next,
			NextRunFormatted: next.Format("2006-01-02 15:04"),
			StartsIn:         formatRelativeDuration(next.Sub(now)),
		})
	}

	sort.Slice(upcoming, func(i, j int) bool {
		return upcoming[i].NextRunAt.Before(upcoming[j].NextRunAt)
	})

	successRate := "—"
	completed := stats.Success + stats.Failed
	if completed > 0 {
		pct := float64(stats.Success) * 100.0 / float64(completed)
		successRate = fmt.Sprintf("%.0f%%", pct)
	}

	return dashboardViewData{
		basePageData: s.newBaseData(r, "Dashboard", "dashboard", stats.Running),
		Stats:        stats,
		SuccessRate:  successRate,
		FlowCount:    validFlows,
		Upcoming:     upcoming,
		RecentRuns:   toRunRowViews(recentRaw, now),
	}, nil
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildDashboardData(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderPage(w, "dashboard", data)
}

func (s *Server) handleDashboardPartial(w http.ResponseWriter, r *http.Request) {
	data, err := s.buildDashboardData(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	s.renderPartial(w, "dashboard_content", data)
}
