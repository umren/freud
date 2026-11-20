package ui

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"freud/internal/workflow"
)

type fakeFlowRunner struct {
	startedCh chan string
	releaseCh chan struct{}

	mu     sync.Mutex
	counts map[string]int
	active atomic.Int32
	maxAct atomic.Int32
}

func newFakeFlowRunner() *fakeFlowRunner {
	return &fakeFlowRunner{
		startedCh: make(chan string, 10),
		releaseCh: make(chan struct{}),
		counts:    make(map[string]int),
	}
}

func (f *fakeFlowRunner) Run(_ context.Context, wf *workflow.Workflow) (string, error) {
	cur := f.active.Add(1)
	for {
		prev := f.maxAct.Load()
		if cur <= prev || f.maxAct.CompareAndSwap(prev, cur) {
			break
		}
	}

	f.mu.Lock()
	f.counts[wf.Name]++
	f.mu.Unlock()

	f.startedCh <- wf.Name
	<-f.releaseCh
	f.active.Add(-1)
	return "run-id", nil
}

func TestSchedulerRunsFlowsConcurrentlyAndPreventsSelfOverlap(t *testing.T) {
	flowsDir := t.TempDir()
	flowA := "name: flow-a\ncron: \"* * * * *\"\ntasks:\n  - id: t1\n    runtime: python\n    script: print(1)\n"
	flowB := "name: flow-b\ncron: \"* * * * *\"\ntasks:\n  - id: t1\n    runtime: js\n    script: console.log(1)\n"
	if err := os.WriteFile(filepath.Join(flowsDir, "a.yaml"), []byte(flowA), 0o644); err != nil {
		t.Fatalf("write a.yaml: %v", err)
	}
	if err := os.WriteFile(filepath.Join(flowsDir, "b.yaml"), []byte(flowB), 0o644); err != nil {
		t.Fatalf("write b.yaml: %v", err)
	}

	runner := newFakeFlowRunner()
	sched := NewScheduler(runner, flowsDir)

	t0 := time.Date(2026, 11, 20, 16, 0, 5, 0, time.UTC)
	sched.tick(context.Background(), t0)

	// Both flow-a and flow-b should start concurrently without blocking each other.
	seen := make(map[string]bool)
	for i := 0; i < 2; i++ {
		select {
		case name := <-runner.startedCh:
			seen[name] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for concurrent scheduled flows to start")
		}
	}
	if !seen["flow-a"] || !seen["flow-b"] {
		t.Fatalf("expected both flow-a and flow-b to start, got %+v", seen)
	}
	if runner.maxAct.Load() != 2 {
		t.Fatalf("expected 2 flows running concurrently, got max active=%d", runner.maxAct.Load())
	}

	// Same minute tick again must not re-fire.
	sched.tick(context.Background(), t0.Add(20*time.Second))

	// Next minute tick while previous runs are STILL active must skip overlapping runs.
	sched.tick(context.Background(), t0.Add(time.Minute))

	close(runner.releaseCh)
	sched.Wait()

	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.counts["flow-a"] != 1 || runner.counts["flow-b"] != 1 {
		t.Fatalf("expected each flow to run exactly once without overlap, got %+v", runner.counts)
	}
}
