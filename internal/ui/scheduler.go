package ui

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"freud/internal/cron"
	"freud/internal/workflow"
)

// FlowRunner executes a workflow (satisfied by *engine.Engine).
type FlowRunner interface {
	Run(ctx context.Context, wf *workflow.Workflow) (string, error)
}

// Scheduler dispatches due cron workflows asynchronously without blocking the ticker
// and prevents overlapping runs of the same flow.
type Scheduler struct {
	runner   FlowRunner
	flowsDir string

	mu          sync.Mutex
	activeFlows map[string]bool
	lastFired   map[string]string
	wg          sync.WaitGroup
}

// NewScheduler creates an asynchronous cron Scheduler for flows in flowsDir.
func NewScheduler(runner FlowRunner, flowsDir string) *Scheduler {
	return &Scheduler{
		runner:      runner,
		flowsDir:    flowsDir,
		activeFlows: make(map[string]bool),
		lastFired:   make(map[string]string),
	}
}

// StartScheduler runs the cron scheduler loop until ctx is canceled and waits for active runs.
func StartScheduler(ctx context.Context, runner FlowRunner, flowsDir string) {
	NewScheduler(runner, flowsDir).Run(ctx)
}

// Run ticks every 15 seconds, dispatching matching cron flows concurrently, and waits for
// in-flight executions to finish when ctx is canceled.
func (s *Scheduler) Run(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.wg.Wait()
			return
		case now := <-ticker.C:
			s.tick(ctx, now)
		}
	}
}

func (s *Scheduler) tick(ctx context.Context, now time.Time) {
	entries, err := workflow.LoadDir(s.flowsDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[scheduler] Failed to scan %q: %v\n", s.flowsDir, err)
		return
	}

	currentMinute := now.Truncate(time.Minute)
	minuteKey := currentMinute.Format("2006-01-02T15:04")

	for _, entry := range entries {
		if entry.Workflow == nil || entry.Workflow.Cron == "" {
			continue
		}
		sched, err := cron.Parse(entry.Workflow.Cron)
		if err != nil {
			continue
		}
		if !sched.Next(currentMinute.Add(-time.Second)).Equal(currentMinute) {
			continue
		}

		s.mu.Lock()
		if s.lastFired[entry.FileName] == minuteKey {
			s.mu.Unlock()
			continue
		}
		if s.activeFlows[entry.FileName] {
			s.mu.Unlock()
			fmt.Printf("[scheduler] Skipping flow %q (%s): previous execution is still running\n", entry.Workflow.Name, entry.FileName)
			continue
		}
		s.lastFired[entry.FileName] = minuteKey
		s.activeFlows[entry.FileName] = true
		s.wg.Add(1)
		s.mu.Unlock()

		wf := entry.Workflow
		fileName := entry.FileName

		go func() {
			defer s.wg.Done()
			defer func() {
				s.mu.Lock()
				delete(s.activeFlows, fileName)
				s.mu.Unlock()
			}()

			fmt.Printf("[scheduler] Triggering scheduled flow %q (cron=%q)\n", wf.Name, wf.Cron)
			if _, runErr := s.runner.Run(ctx, wf); runErr != nil {
				fmt.Fprintf(os.Stderr, "[scheduler] Flow %q failed: %v\n", wf.Name, runErr)
			}
		}()
	}
}

// Wait blocks until all currently running scheduled flows complete (used in tests/shutdown).
func (s *Scheduler) Wait() {
	s.wg.Wait()
}
