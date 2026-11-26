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
	"path/filepath"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"freud/internal/auth"
	"freud/internal/config"
	"freud/internal/db"
	"freud/internal/engine"
	"freud/internal/env"
	"freud/internal/format"
	"freud/internal/maintenance"
	"freud/internal/mcp"
	"freud/internal/runner"
	"freud/internal/syncer"
	"freud/internal/term"
	"freud/internal/tui"
	"freud/internal/ui"
	"freud/internal/workflow"
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
	case "mcp":
		return runMCPCmd(args[1:])
	default:
		printUsage()
		return fmt.Errorf("unknown command %q", args[0])
	}
}

type cliFlagBindings struct {
	configPath    string
	dbPath        string
	workspacesDir string
	flowsDir      string
	includesDir   string
	envFile       string
	noCache       bool
	agentMadness  bool
	port          int
	scheduler     bool
	noAuth        bool
}

func registerCommonFlags(fs *flag.FlagSet, b *cliFlagBindings) {
	fs.StringVar(&b.configPath, "config", "", "Optional path to freud.toml configuration file")
	fs.StringVar(&b.dbPath, "db", config.DefaultDBPath, "SQLite path, PostgreSQL DSN (postgres://...), or MySQL/MariaDB DSN (mysql://..., mariadb://...)")
	fs.StringVar(&b.workspacesDir, "workspaces-dir", config.DefaultWorkspacesDir, "Directory for task execution workspaces")
	fs.StringVar(&b.flowsDir, "flows-dir", config.DefaultFlowsDir, "Directory containing workflow YAML files")
	fs.StringVar(&b.includesDir, "includes-dir", config.DefaultIncludesDir, "Directory containing shared include files")
	fs.StringVar(&b.envFile, "env-file", "", "Optional path to a .env file")
	fs.BoolVar(&b.noCache, "no-cache", false, "Bypass and rebuild cached dependency images")
	fs.BoolVar(&b.agentMadness, "agent-madness", false, "Allow admin write-scoped API/MCP callers to edit or delete flows and includes when Git sync is disabled")
}

func registerDreamFlags(fs *flag.FlagSet, b *cliFlagBindings) {
	registerCommonFlags(fs, b)
	fs.IntVar(&b.port, "port", config.DefaultDreamPort, "HTTP port to serve the Freud web UI")
	fs.BoolVar(&b.scheduler, "scheduler", false, "Enable background cron execution for scheduled flows")
	fs.BoolVar(&b.noAuth, "no-auth", false, "Disable Dream Web UI authentication")
}

func registerTUIFlags(fs *flag.FlagSet, b *cliFlagBindings) {
	registerCommonFlags(fs, b)
	fs.BoolVar(&b.scheduler, "scheduler", false, "Enable background cron execution for scheduled flows")
}

func (b *cliFlagBindings) toOverrides(fs *flag.FlagSet) config.Overrides {
	ov := config.Overrides{
		ConfigPath: b.configPath,
	}
	fs.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "db":
			ov.DB = &b.dbPath
		case "workspaces-dir":
			ov.WorkspacesDir = &b.workspacesDir
		case "flows-dir":
			ov.FlowsDir = &b.flowsDir
		case "includes-dir":
			ov.IncludesDir = &b.includesDir
		case "env-file":
			ov.EnvFile = &b.envFile
		case "no-cache":
			ov.NoCache = &b.noCache
		case "agent-madness":
			ov.AgentMadness = &b.agentMadness
		case "port":
			ov.DreamPort = &b.port
		case "scheduler":
			ov.Scheduler = &b.scheduler
		case "no-auth":
			ov.NoAuth = &b.noAuth
		}
	})
	return ov
}

type runtimeStack struct {
	store        *db.Store
	dockerRunner *runner.DockerRunner
	eng          *engine.Engine
	baseEnv      map[string]string
	loadedEnv    []string
}

func openRuntime(cfg config.Config, workflowPath string, silent bool) (*runtimeStack, error) {
	if db.DetectDialect(cfg.DB) == db.DialectSQLite && cfg.DB != ":memory:" && !strings.HasPrefix(cfg.DB, "file:") {
		if dir := filepath.Dir(cfg.DB); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return nil, fmt.Errorf("create database directory %q: %w", dir, err)
			}
		}
	}

	store, err := db.Open(cfg.DB)
	if err != nil {
		return nil, err
	}

	baseEnv, loadedFiles, err := loadEnvironment(cfg.EnvFile, workflowPath)
	if err != nil {
		_ = store.Close()
		return nil, err
	}

	dockerRunner, err := runner.NewDockerRunner()
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	dockerRunner.SetNoCache(cfg.NoCache)
	dockerRunner.SetIncludesDir(cfg.IncludesDir)

	eng := engine.New(store, dockerRunner, cfg.WorkspacesDir)
	eng.SetEnv(baseEnv)
	eng.SetFlowsDir(cfg.FlowsDir)

	if silent {
		dockerRunner.SetOutput(io.Discard, io.Discard)
		eng.SetOutput(io.Discard, io.Discard)
	}

	return &runtimeStack{
		store:        store,
		dockerRunner: dockerRunner,
		eng:          eng,
		baseEnv:      baseEnv,
		loadedEnv:    loadedFiles,
	}, nil
}

func (r *runtimeStack) close() {
	if r.dockerRunner != nil {
		_ = r.dockerRunner.Close()
	}
	if r.store != nil {
		_ = r.store.Close()
	}
}

func printConfigSummary(tag string, cfg config.Config, loadedEnv []string, envCount int) {
	c := term.StdoutColor()
	if cfg.ConfigFile != "" {
		fmt.Printf("%s Loaded config from %s\n", term.Tag(c, tag), term.Key(c, cfg.ConfigFile))
	}
	if len(loadedEnv) > 0 {
		fmt.Printf("%s Loaded %s env variable(s) from %s\n",
			term.Tag(c, tag),
			term.Bold(c, fmt.Sprintf("%d", envCount)),
			term.Key(c, strings.Join(loadedEnv, ", ")),
		)
	}
}

func runWorkflowCmd(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var b cliFlagBindings
	registerCommonFlags(fs, &b)
	if err := fs.Parse(args); err != nil {
		return err
	}

	remaining := fs.Args()
	if len(remaining) != 1 {
		printUsage()
		return fmt.Errorf("invalid arguments")
	}
	workflowPath := remaining[0]

	cfg, err := config.Load(b.toOverrides(fs))
	if err != nil {
		return err
	}

	wf, err := workflow.ParseFile(workflowPath)
	if err != nil {
		return err
	}

	c := term.StdoutColor()
	fmt.Print(term.Banner(c))

	rt, err := openRuntime(cfg, workflowPath, false)
	if err != nil {
		return err
	}
	defer rt.close()

	printConfigSummary("freud", cfg, rt.loadedEnv, len(rt.baseEnv))

	if cfg.FlowsDirExplicit {
		rt.eng.SetFlowsDir(cfg.FlowsDir)
	} else if wf.BaseDir != "" {
		rt.eng.SetFlowsDir(wf.BaseDir)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	actor := auth.LocalActor()
	runCtx := engine.WithTriggeredBy(ctx, actor)
	runID, err := rt.eng.Run(runCtx, wf)
	rt.eng.Wait()
	if runID != "" {
		_ = rt.store.RecordAudit(context.Background(), actor, "FLOW_RUN", wf.Name, "cli", "run_id="+runID)
	}
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
	var b cliFlagBindings
	registerDreamFlags(fs, &b)
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(b.toOverrides(fs))
	if err != nil {
		return err
	}

	c := term.StdoutColor()
	fmt.Print(term.Banner(c))

	rt, err := openRuntime(cfg, "", false)
	if err != nil {
		return err
	}
	defer rt.close()

	printConfigSummary("dream", cfg, rt.loadedEnv, len(rt.baseEnv))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	enableAuth := !cfg.Dream.NoAuth
	if enableAuth {
		if created, err := auth.EnsureAdminFromEnv(ctx, rt.store, os.Getenv("FREUD_ADMIN_PASSWORD")); err != nil {
			return err
		} else if created {
			fmt.Printf("%s Initialized admin user from %s\n", term.Tag(c, "dream"), term.Key(c, "FREUD_ADMIN_PASSWORD"))
		} else if hasAdmin, err := rt.store.HasAdmin(ctx); err == nil && !hasAdmin {
			fmt.Printf("%s First-time setup: open %s to set the admin password\n",
				term.Tag(c, "dream"),
				term.Key(c, fmt.Sprintf("http://localhost:%d/setup", cfg.Dream.Port)),
			)
		}
	}

	var gitSyncer *syncer.Syncer
	if cfg.Sync.Enabled {
		gitSyncer, err = syncer.New(rt.store, cfg)
		if err != nil {
			return err
		}
		go gitSyncer.RunPeriodic(ctx)
		st := gitSyncer.Status()
		fmt.Printf("%s Git sync enabled (%s @ %s, every %s)\n",
			term.Tag(c, "sync"),
			term.Key(c, st.Repo),
			term.Key(c, st.Branch),
			term.Bold(c, st.Interval),
		)
	}

	maintSvc, err := maintenance.New(rt.store, cfg.WorkspacesDir, cfg.Retention, rt.dockerRunner)
	if err != nil {
		return err
	}
	go maintSvc.RunPeriodic(ctx)
	if cfg.Retention.RunsDays > 0 || cfg.Retention.WorkspacesDays > 0 || cfg.Retention.AuditDays > 0 {
		fmt.Printf("%s Retention enabled (runs=%dd, workspaces=%dd, audit=%dd, every %s)\n",
			term.Tag(c, "retention"),
			cfg.Retention.RunsDays,
			cfg.Retention.WorkspacesDays,
			cfg.Retention.AuditDays,
			term.Bold(c, cfg.Retention.Interval),
		)
	}

	addr := fmt.Sprintf(":%d", cfg.Dream.Port)
	uiServer, err := ui.NewServer(rt.store, ui.Config{
		Addr:         addr,
		FlowsDir:     cfg.FlowsDir,
		IncludesDir:  cfg.IncludesDir,
		SchedulerOn:  cfg.Dream.Scheduler,
		EnableAuth:   enableAuth,
		AgentMadness: cfg.AgentMadness,
		Runner:       rt.eng,
		Syncer:       gitSyncer,
		Maintenance:  maintSvc,
		RunCtx:       ctx,
	})
	if err != nil {
		return err
	}

	if cfg.Dream.Scheduler {
		go ui.StartScheduler(ctx, rt.eng, cfg.FlowsDir)
		fmt.Printf("%s Cron scheduler enabled (watching %s)\n", term.Tag(c, "scheduler"), term.Key(c, cfg.FlowsDir+"/"))
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
		rt.eng.Wait()
		return err
	}
}

func runTUICmd(args []string) error {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var b cliFlagBindings
	registerTUIFlags(fs, &b)
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(b.toOverrides(fs))
	if err != nil {
		return err
	}

	rt, err := openRuntime(cfg, "", true)
	if err != nil {
		return err
	}
	defer rt.close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.TUI.Scheduler {
		sched := ui.NewScheduler(rt.eng, cfg.FlowsDir)
		sched.SetOutput(io.Discard, io.Discard)
		go sched.Run(ctx)
	}

	err = tui.Run(tui.Config{
		Store:       rt.store,
		FlowsDir:    cfg.FlowsDir,
		Runner:      rt.eng,
		Ctx:         ctx,
		SchedulerOn: cfg.TUI.Scheduler,
	})
	stop()
	rt.eng.Wait()
	return err
}

func runMCPCmd(args []string) error {
	// Reserve stdout strictly for MCP JSON-RPC frames; redirect any stray stdout output to stderr.
	mcpOut := os.Stdout
	os.Stdout = os.Stderr
	return runMCPWithIO(args, os.Stdin, mcpOut)
}

func runMCPWithIO(args []string, in io.Reader, out io.Writer) error {
	fs := flag.NewFlagSet("mcp", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	var b cliFlagBindings
	registerCommonFlags(fs, &b)
	var token string
	fs.StringVar(&token, "token", "", "Optional Freud API token (or FREUD_MCP_TOKEN) to attribute MCP actions to a specific user")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(b.toOverrides(fs))
	if err != nil {
		return err
	}

	rt, err := openRuntime(cfg, "", true)
	if err != nil {
		return err
	}
	defer rt.close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	actorName := auth.LocalActor()
	actorDetails := "stdio"
	actorIsAdmin := true
	actorScope := auth.TokenScopeWrite
	actorHasToken := false
	token = strings.TrimSpace(token)
	if token == "" {
		token = strings.TrimSpace(os.Getenv("FREUD_MCP_TOKEN"))
	}
	if token != "" {
		u, tokRec, err := rt.store.GetUserByAPITokenHash(ctx, auth.HashAPIToken(token))
		if err != nil {
			return fmt.Errorf("invalid MCP API token: %w", err)
		}
		actorName = u.Username
		actorDetails = "token=" + tokRec.Name
		actorIsAdmin = u.IsAdmin
		actorScope = tokRec.Scope
		actorHasToken = true
	}

	var gitSyncer *syncer.Syncer
	if cfg.Sync.Enabled {
		gitSyncer, err = syncer.New(rt.store, cfg)
		if err != nil {
			return err
		}
	}

	mcpSrv := mcp.NewServer(rt.store, mcp.Config{
		FlowsDir:     cfg.FlowsDir,
		IncludesDir:  cfg.IncludesDir,
		Runner:       rt.eng,
		Syncer:       gitSyncer,
		AgentMadness: cfg.AgentMadness,
		RunCtx:       ctx,
		OnLog: func(format string, args ...any) {
			fmt.Fprintf(os.Stderr, "[mcp] "+format+"\n", args...)
		},
	})

	serveCtx := mcp.WithAuth(ctx, actorName, actorDetails, actorIsAdmin, actorScope, actorHasToken)
	err = mcpSrv.ServeStdio(serveCtx, in, out)
	stop()
	mcpSrv.Wait()
	rt.eng.Wait()
	return err
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

func printUsage() {
	c := term.StderrColor()
	fmt.Fprint(os.Stderr, term.Banner(c))
	fmt.Fprintln(os.Stderr, term.Bold(c, "Usage:"))
	fmt.Fprintf(os.Stderr, "  %s [--config freud.toml] [--db path] [--flows-dir dir] [--includes-dir dir] [--workspaces-dir dir] [--env-file .env] [--no-cache] <workflow.yaml>\n", term.Key(c, "freud run"))
	fmt.Fprintf(os.Stderr, "  %s [--config freud.toml] [--db path] [--flows-dir dir] [--includes-dir dir] [--workspaces-dir dir] [--env-file .env] [--scheduler] [--no-cache]\n", term.Key(c, "freud tui"))
	fmt.Fprintf(os.Stderr, "  %s [--config freud.toml] [--port 4321] [--db path] [--flows-dir dir] [--includes-dir dir] [--workspaces-dir dir] [--env-file .env] [--scheduler] [--no-auth] [--no-cache]\n", term.Key(c, "freud dream"))
	fmt.Fprintf(os.Stderr, "  %s [--config freud.toml] [--db path] [--flows-dir dir] [--includes-dir dir] [--workspaces-dir dir] [--env-file .env] [--token frd_...] [--no-cache]\n", term.Key(c, "freud mcp"))
	fmt.Fprintf(os.Stderr, "  %s\n", term.Key(c, "freud cache ls"))
	fmt.Fprintf(os.Stderr, "  %s\n", term.Key(c, "freud cache clear"))
}
