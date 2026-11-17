package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"freud/internal/db"
	"freud/internal/engine"
	"freud/internal/runner"
	"freud/internal/workflow"
)

const (
	dbPath        = "orchestrator.db"
	workspacesDir = "workspaces"
)

func main() {
	if err := runCLI(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "[orchestrator] Error: %v\n", err)
		os.Exit(1)
	}
}

func runCLI(args []string) error {
	if len(args) != 2 || args[0] != "run" {
		printUsage()
		return fmt.Errorf("invalid arguments")
	}

	workflowPath := args[1]

	wf, err := workflow.ParseFile(workflowPath)
	if err != nil {
		return err
	}

	store, err := db.Open(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	dockerRunner, err := runner.NewDockerRunner()
	if err != nil {
		return err
	}
	defer dockerRunner.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	eng := engine.New(store, dockerRunner, workspacesDir)
	runID, err := eng.Run(ctx, wf)
	if err != nil {
		if runID != "" {
			return fmt.Errorf("workflow run %s failed: %w", runID, err)
		}
		return err
	}

	return nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: orchestrator run <path/to/workflow.yaml>")
}
