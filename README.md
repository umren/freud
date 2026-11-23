# Freud (v0.1)

A pure-Go task orchestrator that parses YAML workflow definitions, resolves task dependencies as a DAG, executes tasks in ephemeral Docker containers, shares data across steps via a per-run workspace directory, and persists execution state and logs in SQLite.

## Features

- **Pure-Go stack (no CGO required)**: builds cleanly with `CGO_ENABLED=0`.
- **YAML DAG workflows**: defines tasks, environment variables, inline scripts, and `depends_on` edges with cycle and reference validation.
- **Dockerized runtimes with custom images**:
  - `python` (default `ghcr.io/astral-sh/uv:python3.12-bookworm-slim`, or any custom image such as `python:3.13-slim`)
  - `js` (default `oven/bun:latest`, or any custom image such as `oven/bun:1.2`, `node:22-alpine`)
  - `shell` (or omit `runtime` when `image` is specified, e.g. `alpine:3.20`)
- **Automatic image pulling & dependency caching**: checks local Docker images, pulls missing ones with live progress, and builds cached Docker image layers (`orchestrator-cache:<hash>`) for tasks with `dependencies` so subsequent runs start immediately without reinstalling packages.
- **Shared per-run workspace & read-only includes**: creates `./workspaces/<run_id>` on the host (mounted at `/workspace`) and mounts external helper modules/configs from `includes/` (or explicit `include:` paths) read-only at `/includes` with zero file copying.
- **Reliable log & status capture**: disables `AutoRemove`, waits for container completion, demultiplexes stdout/stderr, prints output, stores logs and exit codes in SQLite, and then removes the container.
- **Fail-fast execution**: stops the pipeline immediately if a task exits with a non-zero status, marking both the task and the workflow run as `FAILED`.
- **Cron schedules & embedded Web UI**: workflows support standard 5-field `cron` expressions (and `@hourly`/`@daily`/etc.), and `freud dream` serves an embedded web UI (Go `html/template` + HTMX & Alpine.js) on port `4321` with Dashboard, Flows, Executions, and Includes pages.
- **SQLite persistence (`orchestrator.db`)**: configured with `PRAGMA journal_mode=WAL;` and `PRAGMA busy_timeout=5000;`.

## Requirements

- Go `1.27+` (matching `go.mod`)
- A running Docker daemon accessible to the CLI (via default socket or `DOCKER_HOST`)

## Project Structure

```text
.
├── main.go                 # CLI entrypoint (`run`, `dream`, `cache`)
├── flows/                  # Workflow YAML definitions (`etl-example.yaml`, `workspace-audit.yaml`)
├── includes/               # Shared helper files mounted read-only at `/includes`
├── go.mod
├── go.sum
└── internal/
    ├── cron/               # 5-field cron parser and next-run calculator
    ├── workflow/           # YAML parsing, validation, cycle detection, topological sort
    ├── env/                # .env file loader and task environment merging
    ├── db/                 # SQLite initialization, WAL pragmas, migrations, state queries
    ├── runner/             # Docker SDK execution, image pull/cache, log capture, cleanup
    ├── engine/             # Pipeline orchestration and workspace lifecycle
    ├── term/               # Terminal ANSI color formatting and ASCII startup banner
    ├── tui/                # Interactive Bubbletea terminal UI (dashboard, flows, logs, run/kill)
    └── ui/                 # Embedded web UI (templates, HTMX/Alpine static assets, HTTP server)
```

## Build

```bash
go mod tidy
go build -o freud .
```

To verify a static pure-Go build without CGO:

```bash
CGO_ENABLED=0 go build -o freud .
```

## Usage

Run a workflow YAML file:

```bash
./freud run flows/etl-example.yaml
```

Optionally pass a custom `.env` file or force-rebuild cached dependency images:

```bash
./freud run --env-file .env.prod flows/etl-example.yaml
./freud run --no-cache flows/etl-example.yaml
```

Start the interactive terminal UI (Dashboard, Flows + YAML viewer, live Executions/Logs, Run & Kill):

```bash
./freud tui
# Optional flags:
./freud tui --flows-dir flows --scheduler
```

Start the embedded Web UI (default port `4321`):

```bash
./freud dream
# Optional flags:
./freud dream --port 4321 --flows-dir flows --includes-dir includes --scheduler
```

The Web UI exposes four pages (no auth required locally):
- **Dashboard (`/`)**: execution counts, success rate, upcoming cron runs, and recent executions (auto-refreshes via HTMX).
- **Flows (`/flows`)**: workflows discovered in `flows/`, cron schedules, SVG task topology, read-only YAML source viewer, and a **Run Now** button to trigger the flow immediately and jump to its live execution page.
- **Executions (`/executions`)**: newest-to-oldest execution history with status filter, search, real-time streaming task logs, Gantt timeline, and execution topology.
- **Includes (`/includes`)**: read-only file browser for `includes/`.

Manage cached dependency images:

```bash
./freud cache ls
./freud cache clear
```

### Environment Variables (`.env`)

- When `--env-file` is omitted, the orchestrator automatically loads `.env` from the workflow's directory and/or the current working directory (current working directory takes precedence if both exist).
- All variables from `.env` are injected into every task container (`os.environ` in Python, `process.env` in JS/Bun, `$VAR` in shell).
- Task-level `env:` entries in YAML override `.env` variables on key collision and can reference `.env` variables via `${VAR}` syntax (for example `AUTH_TOKEN: "${API_KEY}"`).
- Host environment variables are not forwarded automatically, and `.env` values are never printed to orchestrator logs.

You can interrupt a running workflow with `Ctrl+C` (`SIGINT`/`SIGTERM`), which cancels execution and records the active task and run as `FAILED`.

### Example Flows (`flows/`)

1. **[`flows/etl-example.yaml`](flows/etl-example.yaml)** (`cron: "0 */6 * * *"`):
   - **`extract`** (`runtime: python`, `image: python:3.13-slim`, `dependencies: [pydantic, pyyaml]`, `include: [includes/payload_utils.py]`): installs `pydantic` and `pyyaml`, imports `payload_utils` from `/includes` via `PYTHONPATH`, reads variables from `.env`, prints a YAML summary, and writes `/workspace/payload.json`.
   - **`transform`** (`runtime: js`, `depends_on: [extract]`, `dependencies: [lodash-es, ms]`, `include: [includes/math_utils.ts]`): installs `lodash-es` and `ms`, imports `formatStats` from `/includes/math_utils.ts`, reads `/workspace/payload.json`, computes the sum (`102`) with `lodash-es`, and prints the `.env` variables and stats to stdout.
2. **[`flows/workspace-audit.yaml`](flows/workspace-audit.yaml)** (`cron: "*/30 * * * *"`, `include: [includes]`):
   - Three-step `alpine:3.20` -> `python` -> `js` pipeline inspecting mounted `/includes` items and publishing an audit summary.
3. **[`flows/full-pipeline.yaml`](flows/full-pipeline.yaml)** (`cron: "0 2 * * *"`):
   - Orchestrates `etl-example` and `workspace-audit` sequentially via `flow:` subflow tasks (passing an `env:` override to `workspace-audit`) followed by a final summary step.

## Workflow YAML Specification

```yaml
name: my-pipeline
cron: "0 */6 * * *"           # optional 5-field cron expression (or @hourly, @daily, etc.)
include:                      # optional workflow-wide read-only files/dirs mounted under /includes
  - includes/shared.py
tasks:
  - id: step_one
    runtime: python           # "python", "js", or "shell" (optional if image is set)
    image: python:3.13-slim   # optional Docker image override
    dependencies:             # optional packages installed in the task container
      - pydantic
      - pyyaml
    include:                  # optional per-task read-only files/dirs mounted under /includes
      - includes/payload_utils.py
    env:                      # optional key-value environment variables
      MY_VAR: "value"
    script: |
      from payload_utils import build_payload
      print("Hello from Python 3.13")

  - id: step_two
    runtime: js
    depends_on:               # optional list of upstream task IDs
      - step_one
    dependencies:
      - lodash-es
    include:
      - includes/math_utils.ts
    script: |
      import { formatStats } from "/includes/math_utils.ts";
      console.log("Hello from Bun");

  - id: step_three
    image: alpine:3.20        # runtime defaults to "shell" when omitted
    depends_on:
      - step_two
    script: |
      ls -la /workspace /includes

  - id: call_other_flow
    flow: workspace-audit     # invokes another flow by its `name`
    wait: true                # optional (default true); set false for fire-and-forget
    depends_on:
      - step_three
    env:
      PIPELINE_ENV: "prod"
```

### Included Files (`/includes`)

- **Zero-copy read-only mounts**: included paths are bind-mounted with `ReadOnly: true` under `/includes` (never copied into `./workspaces/<run_id>`), so high-throughput workflows (1,000+ runs/day) incur zero extra disk copying.
- **Auto-discovery**: if neither workflow nor task specifies `include:`, and an `includes/` directory exists next to the workflow YAML, it is automatically mounted read-only at `/includes`.
- **Python imports**: `PYTHONPATH=/workspace:/includes` is set automatically for `python` tasks (unless overridden in `env:`), allowing direct `import <module>` from `/includes`.
- **Path mapping**: paths under `includes/...` map to `/includes/...` (e.g. `includes/math_utils.ts` → `/includes/math_utils.ts`); other relative paths map to `/includes/<path>`.

### Validation Rules

- `name` must be non-empty.
- `tasks` must contain at least one task.
- Each task `id` must be unique, non-empty, and must not contain whitespace, `/`, `\`, `.` or `..`.
- `runtime` must be `"python"`, `"js"`, or `"shell"` (if omitted, `image` must be set and `runtime` defaults to `"shell"`).
- `image` is optional for `python` and `js`, required for `shell`, and must not contain whitespace.
- `dependencies` is optional for `python` and `js` (not supported on `shell`); entries must be non-empty.
- `include` paths must be relative and stay inside the workflow directory (no `..` traversal).
- `script` must be non-empty.
- Every entry in `depends_on` must reference an existing task `id`, cannot reference itself, and the resulting graph must be acyclic.

## How Execution Works

1. **Workspace setup**: generates a unique `run_id` (`YYYYMMDD-HHMMSS-<hex>`) and creates `./workspaces/<run_id>/.scripts` on the host.
2. **Topological ordering**: resolves task order via Kahn's algorithm so every dependency finishes before its dependents start.
3. **Container run**:
   - Writes the task script to `./workspaces/<run_id>/.scripts/<task_id>.py` (`python`), `.ts` (`js`), or `.sh` (`shell`).
   - Mounts `./workspaces/<run_id>` as `/workspace` (`WorkingDir: /workspace`).
   - Uses the task's `image` (or runtime default if omitted) and executes the script with the available runtime binary (`uv` / `python3` / `python` for Python, `bun` / `deno` / `node` for JS, `sh` for shell).
   - Waits for exit (`ContainerWait`), captures stdout/stderr (`ContainerLogs`), writes logs to console and SQLite, and removes the container (`ContainerRemove`).

## SQLite Database (`orchestrator.db`)

On startup, the orchestrator creates `orchestrator.db` (if absent) and applies two tables:

- **`runs`**: `id`, `workflow_name`, `status` (`RUNNING`, `SUCCESS`, `FAILED`), `created_at`, `finished_at`
- **`task_runs`**: `id`, `run_id`, `task_id`, `status`, `exit_code`, `logs`, `started_at`, `finished_at`

You can inspect stored runs with Python or `sqlite3`:

```bash
sqlite3 orchestrator.db "SELECT id, workflow_name, status, created_at, finished_at FROM runs;"
sqlite3 orchestrator.db "SELECT id, run_id, task_id, status, exit_code FROM task_runs;"
```

## Testing

Run all unit tests (workflow validation, SQLite pragmas/store, and engine orchestration with a mock runner):

```bash
go test -v ./...
```
