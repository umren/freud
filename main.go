package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"freud/internal/db"
	"freud/internal/engine"
	"freud/internal/env"
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
	if len(args) < 1 || args[0] != "run" {
		printUsage()
		return fmt.Errorf("invalid arguments")
	}

	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	envFile := fs.String("env-file", "", "Optional path to a .env file")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}

	remaining := fs.Args()
	if len(remaining) != 1 {
		printUsage()
		return fmt.Errorf("invalid arguments")
	}
	workflowPath := remaining[0]

	wf, err := workflow.ParseFile(workflowPath)
	if err != nil {
		return err
	}

	baseEnv, loadedFiles, err := loadEnvironment(*envFile, workflowPath)
	if err != nil {
		return err
	}
	if len(loadedFiles) > 0 {
		fmt.Printf("[orchestrator] Loaded %d env variable(s) from %s\n", len(baseEnv), strings.Join(loadedFiles, ", "))
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
	eng.SetEnv(baseEnv)

	runID, err := eng.Run(ctx, wf)
	if err != nil {
		if runID != "" {
			return fmt.Errorf("workflow run %s failed: %w", runID, err)
		}
		return err
	}

	return nil
}

func loadEnvironment(explicitPath, workflowPath string) (map[string]string, []string, error) {
	if explicitPath != "" {
		vars, err := env.ParseFile(explicitPath)
		if err != nil {
			return nil, nil, err
		}
		return vars, []string{explicitPath}, nil
	}
	return env.LoadDefault(workflowPath)
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: orchestrator run [--env-file <path/to/.env>] <path/to/workflow.yaml>")
}
