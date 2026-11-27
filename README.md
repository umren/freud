# Freud 🧠⚙️ (v1.0)

A lightweight, **production-ready**, **single-binary Go workflow orchestrator** already used in real production projects. Runs parallel YAML DAG pipelines in isolated Docker containers — with a built-in Web UI, Terminal UI, cron scheduler, Git sync, REST API, and MCP server for AI agents.

> 🚀 **Version 1.0 — Production-Ready** &nbsp;·&nbsp; 📖 **Full Technical Documentation:** [`DOCS.md`](DOCS.md) &nbsp;·&nbsp; 🤖 **AI Agents:** [`AGENTS.md`](AGENTS.md) &nbsp;·&nbsp; 📜 **License:** [MIT](LICENSE)

## 💡 Why Another Orchestrator?

Most existing workflow orchestrators fall into one of two camps:

- **Python-centric tools (Airflow, Prefect, Dagster)** lock you into the host Python runtime and struggle when pipelines need **different Python versions** or **conflicting dependency sets** — forcing brittle virtualenvs, heavy multi-service setups, and constant dependency juggling.
- **More feature-rich platforms (like Kestra)** run on a heavy **JVM** stack and increasingly focus on **enterprise clients**, gated features, and commercial tiers.

**Freud** takes a simpler, cleaner approach:

- **Isolated versions & dependencies — solved** — every task runs in its own Docker image (`python:3.10`, `python:3.13-slim`, `oven/bun`, custom images, etc.) with per-task cached dependencies (`orchestrator-cache:*`). Different Python versions and conflicting libraries coexist in the same DAG effortlessly.
- **A "clean" core with zero compatibility friction** — the orchestrator itself is a pure-Go binary that doesn't bundle your runtime libraries or cloud SDKs. Because it sits strictly outside your task containers, your code and SDKs will never clash with the orchestrator.
- **Port any existing code in 5 minutes (even via an AI agent)** — no proprietary `@task` decorators or SDK rewrites. Wrap your existing Python/JS/Shell scripts and shared modules cleanly into YAML (any AI agent can do this in ~5 minutes using [`AGENTS.md`](AGENTS.md) or MCP) and immediately rely on a ready, flexible orchestrator.
- **100% Open Source forever** — licensed under **MIT**, with every feature (Web UI, TUI, Cron, RBAC/Auth, Git Sync, Retention, REST API, and MCP server) available to everyone with **no paid tiers or enterprise paywalls**.

## ✨ Highlights

- 🪶 **Single binary (`CGO_ENABLED=0`)** — zero external runtime dependencies except Docker; SQLite works out of the box (PostgreSQL & MySQL/MariaDB also built in).
- ⚡ **Parallel & resilient DAGs** — run independent branches concurrently (`concurrency` limit), configure `timeout` & `retries`, branch with `if:` expressions, and handle failures with `when: on_failure` / `always`.
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
