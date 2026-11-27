# Freud 🧠⚙️

A lightweight, **single-binary Go workflow orchestrator** that runs YAML DAG pipelines in Docker containers — with a built-in Web UI, Terminal UI, cron scheduler, Git sync, REST API, and MCP server for AI agents.

> 📖 **Full Technical Documentation:** [`DOCS.md`](DOCS.md)  
> 🤖 **AI & Coding Agent Guide:** [`AGENTS.md`](AGENTS.md)  
> 📜 **License:** [MIT](LICENSE)

## 💡 Why Another Orchestrator?

Most existing workflow orchestrators fall into one of two camps:

- **Python-centric tools (Airflow, Prefect, Dagster)** lock you into a Python runtime and come with heavy multi-service setups, worker queues, and dependency juggling.
- **More feature-rich platforms (like Kestra)** run on a heavy **JVM** stack and increasingly focus on **enterprise clients**, gated features, and commercial tiers.

**Freud** is built to be the opposite:

- **Single lightweight Go binary** — starts in milliseconds, needs only Docker, and fits comfortably on a small VPS or laptop.
- **Language-agnostic by default** — mix `python`, `js` (`Bun`/`Node`), or any custom `Docker` image in the same DAG without runtime lock-in.
- **100% Open Source forever** — licensed under **MIT**, with every feature (Web UI, TUI, Cron, RBAC/Auth, Git Sync, Retention, REST API, and MCP server) available to everyone with **no paid tiers or enterprise paywalls**.

## ✨ Highlights

- 🪶 **Single binary (`CGO_ENABLED=0`)** — zero external runtime dependencies except Docker; SQLite works out of the box (PostgreSQL & MySQL/MariaDB also built in).
- 🐳 **Polyglot Docker tasks** — run `python`, `js` (`bun`/`node`), or any custom `shell` image with automatic dependency layer caching (`orchestrator-cache:*`).
- 📁 **Shared workspace & `/includes`** — steps share `/workspace` during a run and mount helper scripts read-only at `/includes` with zero copying.
- 🖥️ **Dream Web UI + TUI** — inspect DAG topology, stream live task logs, manage users/audit logs, clean up old data, and trigger runs (even with one-off per-run `env` parameters).
- ⏰ **Dynamic Cron Scheduler** — 5-field cron & macros (`@hourly`, `@daily`), automatically picking up workflow changes from disk every 15 seconds.
- 🔄 **Git Sync** — pull workflows & includes from GitHub/GitLab/Gitea on a schedule or via push webhooks.
- 🤖 **Agent-ready by design** — embedded JSON-RPC **MCP server** (`stdio` & HTTP `/mcp`), scoped REST API (`read` / `run` / `write`), YAML validation, and opt-in `agent_madness` editing.

## 🎯 Use Cases

- 📊 **Data Engineering** — scheduled ETL/ELT pipelines extracting from APIs or databases in `Python`, transforming in `Bun`/`JS` or `Shell`, and exchanging artifacts via `/workspace`.
- 🤖 **AI Agents** — expose flows to Claude/Cursor/custom agents via native **MCP** (`stdio` or HTTP) or REST API to inspect specs, trigger parameterized runs, read logs, and optionally create flows (`agent_madness`).
- 🧠 **Machine Learning** — reproducible training, batch inference, feature prep, and evaluation steps inside cached Docker environments (`orchestrator-cache:*`) without polluting the host.
- ☁️ **Cloud & Infrastructure Orchestration** — glue together Terraform/CLI tools, cloud SDK scripts, backup jobs, and health checks using any custom container image in a single DAG.
- ⚙️ **Automated Business Workflows** — recurring invoicing, CRM syncs, report generation, webhook-triggered notifications, and multi-step operational routines on cron or on-demand runs.

## ⚡ Quick Start

### 1. Build

Requires **Go 1.27+** and a running **Docker** daemon:

```bash
CGO_ENABLED=0 go build -o freud .
```

### 2. Run a workflow

```bash
./freud run flows/etl-example.yaml
```

Pass one-off parameters for a single execution:

```bash
./freud run -e TARGET_DATE=2026-11-20 flows/etl-example.yaml
```

### 3. Launch the Web UI or TUI

```bash
# Embedded Web UI (http://localhost:4321) + Cron Scheduler + REST API + HTTP /mcp
./freud dream --scheduler

# Or interactive Terminal UI
./freud tui --scheduler
```

## 🧩 Workflow Example (`flows/hello.yaml`)

```yaml
name: daily-report
cron: "0 8 * * *"
include:
  - includes/helpers.py
tasks:
  - id: extract
    runtime: python
    dependencies: [pydantic]
    script: |
      import helpers
      open("/workspace/data.txt", "w").write("ok")

  - id: publish
    runtime: js
    depends_on: [extract]
    script: |
      const data = await Bun.file("/workspace/data.txt").text();
      console.log("Published:", data);
```

## 🧭 CLI Modes

| Command | Purpose |
| --- | --- |
| `./freud run [flags] <flow.yaml>` | Run a workflow once from the CLI (supports `-e KEY=VAL`) |
| `./freud dream` | Start the Dream Web UI, cron scheduler, REST API v1, and HTTP `/mcp` |
| `./freud tui` | Open the interactive Bubbletea Terminal UI |
| `./freud mcp` | Start the local MCP server over `stdin`/`stdout` |
| `./freud cache ls \| clear` | Inspect or remove cached `orchestrator-cache:*` Docker layers |

## 📚 Learn More

- **[`DOCS.md`](DOCS.md)** — complete reference for `freud.toml`, `FREUD_*` env vars, `.env` & per-run overrides, YAML validation rules, Dream UI pages, REST API `/api/v1/*`, Bearer token scopes (`read`/`run`/`write`), `agent_madness`, MCP tools/resources, Git sync, retention cleanup, PostgreSQL/MySQL setup, and tests.
- **[`AGENTS.md`](AGENTS.md)** — operating rules and conventions for AI agents integrating with Freud.

## 📄 License

Freud is **100% open source** under the **[MIT License](LICENSE)** — free to use, modify, self-host, and build upon forever.
