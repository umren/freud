package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"freud/internal/db"
	"freud/internal/engine"
	"freud/internal/env"
	"freud/internal/format"
	"freud/internal/runner"
	"freud/internal/term"
	"freud/internal/tui"
	"freud/internal/ui"
	"freud/internal/workflow"
)

const (
	dbPath             = "orchestrator.db"
	workspacesDir      = "workspaces"
	defaultFlowsDir    = "flows"
	defaultIncludesDir = "includes"
	defaultUIPort      = 4321
)

func main() {
	if err := runCLI(os.Args[1:]); err != nil {
		c := term.StderrColor()
		fmt.Fprintf(os.Stderr, "%s %s: %v\n", term.Tag(c, "freud"), term.Status(c, "Error"), err)
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
	case "dream":
		return runDreamCmd(args[1:])
	case "tui":
		return runTUICmd(args[1:])
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

	c := term.StdoutColor()
	fmt.Print(term.Banner(c))

	baseEnv, loadedFiles, err := loadEnvironment(*envFile, workflowPath)
	if err != nil {
		return err
	}
	if len(loadedFiles) > 0 {
		fmt.Printf("%s Loaded %s env variable(s) from %s\n",
			term.Tag(c, "freud"),
			term.Bold(c, fmt.Sprintf("%d", len(baseEnv))),
			term.Key(c, strings.Join(loadedFiles, ", ")),
		)
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
	eng.SetFlowsDir(wf.BaseDir)

	runID, err := eng.Run(ctx, wf)
	eng.Wait()
	if err != nil {
		if runID != "" {
			return fmt.Errorf("workflow run %s failed: %w", runID, err)
		}
		return err
	}

	return nil
}

func runDreamCmd(args []string) error {
	fs := flag.NewFlagSet("dream", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	port := fs.Int("port", defaultUIPort, "HTTP port to serve the Freud web UI")
	flowsDir := fs.String("flows-dir", defaultFlowsDir, "Directory containing workflow YAML files")
	includesDir := fs.String("includes-dir", defaultIncludesDir, "Directory containing shared include files")
	envFile := fs.String("env-file", "", "Optional path to a .env file for UI-triggered and scheduled runs")
	schedulerOn := fs.Bool("scheduler", false, "Enable background cron execution for scheduled flows")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *port <= 0 || *port > 65535 {
		return fmt.Errorf("invalid port %d", *port)
	}

	store, err := db.Open(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	c := term.StdoutColor()
	fmt.Print(term.Banner(c))

	baseEnv, loadedFiles, err := loadEnvironment(*envFile, "")
	if err != nil {
		return err
	}
	if len(loadedFiles) > 0 {
		fmt.Printf("%s Loaded %s env variable(s) from %s\n",
			term.Tag(c, "dream"),
			term.Bold(c, fmt.Sprintf("%d", len(baseEnv))),
			term.Key(c, strings.Join(loadedFiles, ", ")),
		)
	}

	dockerRunner, err := runner.NewDockerRunner()
	if err != nil {
		return err
	}
	defer dockerRunner.Close()

	eng := engine.New(store, dockerRunner, workspacesDir)
	eng.SetEnv(baseEnv)
	eng.SetFlowsDir(*flowsDir)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	addr := fmt.Sprintf(":%d", *port)
	uiServer, err := ui.NewServer(store, ui.Config{
		Addr:        addr,
		FlowsDir:    *flowsDir,
		IncludesDir: *includesDir,
		SchedulerOn: *schedulerOn,
		Runner:      eng,
		RunCtx:      ctx,
	})
	if err != nil {
		return err
	}

	if *schedulerOn {
		go ui.StartScheduler(ctx, eng, *flowsDir)
		fmt.Printf("%s Cron scheduler enabled (watching %s)\n", term.Tag(c, "scheduler"), term.Key(c, *flowsDir+"/"))
	}

	httpServer := &http.Server{
		Addr:              addr,
		Handler:           uiServer.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		fmt.Printf("%s Freud Web UI listening on %s\n", term.Tag(c, "dream"), term.Key(c, "http://localhost"+addr))
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
		close(errCh)
	}()

	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := httpServer.Shutdown(shutdownCtx)
		uiServer.Wait()
		eng.Wait()
		return err
	}
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
		c := term.StdoutColor()
		if len(images) == 0 {
			fmt.Println(term.Muted(c, "No cached dependency images found."))
			return nil
		}

		tw := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n",
			term.Bold(c, "TAG"),
			term.Bold(c, "RUNTIME"),
			term.Bold(c, "BASE IMAGE"),
			term.Bold(c, "DEPENDENCIES"),
			term.Bold(c, "SIZE"),
			term.Bold(c, "CREATED"),
		)
		for _, img := range images {
			fmt.Fprintf(
				tw,
				"%s\t%s\t%s\t%s\t%s\t%s\n",
				term.Key(c, img.Tag),
				img.Runtime,
				img.BaseImage,
				img.Deps,
				format.Bytes(img.Size),
				term.Muted(c, img.CreatedAt.Format("2006-01-02 15:04:05Z")),
			)
		}
		return tw.Flush()

	case "clear":
		removed, reclaimed, err := dockerRunner.ClearCachedImages(ctx)
		if err != nil {
			return err
		}
		c := term.StdoutColor()
		fmt.Printf("%s Removed %s cached image(s), reclaimed %s\n",
			term.Tag(c, "cache"),
			term.Bold(c, fmt.Sprintf("%d", removed)),
			term.Key(c, format.Bytes(reclaimed)),
		)
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

func runTUICmd(args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	flowsDir := fs.String("flows-dir", defaultFlowsDir, "Directory containing workflow YAML files")
	envFile := fs.String("env-file", "", "Optional path to a .env file for TUI-triggered and scheduled runs")
	schedulerOn := fs.Bool("scheduler", false, "Enable background cron execution for scheduled flows")
	noCache := fs.Bool("no-cache", false, "Bypass and rebuild cached dependency images")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store, err := db.Open(dbPath)
	if err != nil {
		return err
	}
	defer store.Close()

	baseEnv, _, err := loadEnvironment(*envFile, "")
	if err != nil {
		return err
	}

	dockerRunner, err := runner.NewDockerRunner()
	if err != nil {
		return err
	}
	defer dockerRunner.Close()
	dockerRunner.SetNoCache(*noCache)
	dockerRunner.SetOutput(io.Discard, io.Discard)

	eng := engine.New(store, dockerRunner, workspacesDir)
	eng.SetEnv(baseEnv)
	eng.SetFlowsDir(*flowsDir)
	eng.SetOutput(io.Discard, io.Discard)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if *schedulerOn {
		sched := ui.NewScheduler(eng, *flowsDir)
		sched.SetOutput(io.Discard, io.Discard)
		go sched.Run(ctx)
	}

	err = tui.Run(tui.Config{
		Store:       store,
		FlowsDir:    *flowsDir,
		Runner:      eng,
		Ctx:         ctx,
		SchedulerOn: *schedulerOn,
	})
	stop()
	eng.Wait()
	return err
}

func printUsage() {
	c := term.StderrColor()
	fmt.Fprint(os.Stderr, term.Banner(c))
	fmt.Fprintln(os.Stderr, term.Bold(c, "Usage:"))
	fmt.Fprintf(os.Stderr, "  %s [--env-file <path/to/.env>] [--no-cache] <path/to/workflow.yaml>\n", term.Key(c, "freud run"))
	fmt.Fprintf(os.Stderr, "  %s [--flows-dir flows] [--env-file <path/to/.env>] [--scheduler] [--no-cache]\n", term.Key(c, "freud tui"))
	fmt.Fprintf(os.Stderr, "  %s [--port 4321] [--flows-dir flows] [--includes-dir includes] [--scheduler]\n", term.Key(c, "freud dream"))
	fmt.Fprintf(os.Stderr, "  %s\n", term.Key(c, "freud cache ls"))
	fmt.Fprintf(os.Stderr, "  %s\n", term.Key(c, "freud cache clear"))
}
