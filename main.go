package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"

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
	if len(args) < 1 {
		printUsage()
		return fmt.Errorf("invalid arguments")
	}

	switch args[0] {
	case "run":
		return runWorkflowCmd(args[1:])
	case "cache":
		return runCacheCmd(args[1:])
	default:
		printUsage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

func runWorkflowCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	envFile := fs.String("env-file", "", "Optional path to a .env file")
	noCache := fs.Bool("no-cache", false, "Bypass and rebuild cached dependency images")
	if err := fs.Parse(args); err != nil {
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
	dockerRunner.SetNoCache(*noCache)

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

func runCacheCmd(args []string) error {
	if len(args) != 1 {
		printUsage()
		return fmt.Errorf("invalid cache arguments")
	}

	dockerRunner, err := runner.NewDockerRunner()
	if err != nil {
		return err
	}
	defer dockerRunner.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch args[0] {
	case "ls":
		images, err := dockerRunner.ListCachedImages(ctx)
		if err != nil {
			return err
		}
		if len(images) == 0 {
			fmt.Println("No cached dependency images found.")
			return nil
		}

		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(tw, "TAG\tRUNTIME\tBASE IMAGE\tDEPENDENCIES\tSIZE\tCREATED")
		for _, img := range images {
			fmt.Fprintf(
				tw,
				"%s\t%s\t%s\t%s\t%s\t%s\n",
				img.Tag,
				img.Runtime,
				img.BaseImage,
				img.Deps,
				formatBytes(img.Size),
				img.CreatedAt.Format("2006-01-02 15:04:05Z"),
			)
		}
		return tw.Flush()

	case "clear":
		removed, reclaimed, err := dockerRunner.ClearCachedImages(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("[cache] Removed %d cached image(s), reclaimed %s\n", removed, formatBytes(reclaimed))
		return nil

	default:
		printUsage()
		return fmt.Errorf("unknown cache subcommand %q", args[0])
	}
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

func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  orchestrator run [--env-file <path/to/.env>] [--no-cache] <path/to/workflow.yaml>")
	fmt.Fprintln(os.Stderr, "  orchestrator cache ls")
	fmt.Fprintln(os.Stderr, "  orchestrator cache clear")
}
