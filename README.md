# Orchestrator (v0.1)

A minimal pure-Go CLI task orchestrator (a lightweight Kestra-inspired prototype) that parses YAML workflow definitions, resolves task dependencies as a DAG, executes tasks in ephemeral Docker containers, shares data across steps via a per-run workspace directory, and persists execution state and logs in SQLite.

## Features

- **Pure-Go stack (no CGO required)**: builds cleanly with `CGO_ENABLED=0`.
- **YAML DAG workflows**: defines tasks, environment variables, inline scripts, and `depends_on` edges with cycle and reference validation.
- **Dockerized runtimes**:
  - `python` via `ghcr.io/astral-sh/uv:python3.12-bookworm-slim` (`uv run`)
  - `js` via `oven/bun:latest` (`bun run`)
- **Automatic image pulling**: checks local Docker images and pulls missing ones while streaming pull progress to the console.
- **Shared per-run workspace**: creates `./workspaces/<run_id>` on the host and mounts it at `/workspace` inside every task container.
- **Reliable log & status capture**: disables `AutoRemove`, waits for container completion, demultiplexes stdout/stderr, prints output, stores logs and exit codes in SQLite, and then removes the container.
- **Fail-fast execution**: stops the pipeline immediately if a task exits with a non-zero status, marking both the task and the workflow run as `FAILED`.
- **SQLite persistence (`orchestrator.db`)**: configured with `PRAGMA journal_mode=WAL;` and `PRAGMA busy_timeout=5000;`.

## Requirements

- Go `1.27+` (matching `go.mod`)
- A running Docker daemon accessible to the CLI (via default socket or `DOCKER_HOST`)

## Project Structure

```text
.
├── main.go                 # CLI entrypoint (`run <path/to/workflow.yaml>`)
├── example.yaml            # Two-step Python -> Bun/JS ETL example workflow
├── go.mod
├── go.sum
└── internal/
    ├── workflow/           # YAML parsing, validation, cycle detection, topological sort
    ├── db/                 # SQLite initialization, WAL pragmas, migrations, state queries
    ├── runner/             # Docker SDK execution, image pull, log capture, cleanup
    └── engine/             # Pipeline orchestration and workspace lifecycle
```

## Build

```bash
go mod tidy
go build -o orchestrator .
```

To verify a static pure-Go build without CGO:

```bash
CGO_ENABLED=0 go build -o orchestrator .
```

## Usage

Run a workflow YAML file:

```bash
./orchestrator run example.yaml
```

You can interrupt a running workflow with `Ctrl+C` (`SIGINT`/`SIGTERM`), which cancels execution and records the active task and run as `FAILED`.

### Example (`example.yaml`)

The included [`example.yaml`](example.yaml) demonstrates file-based data exchange between two runtimes:

1. **`extract`** (`runtime: python`): reads `SOURCE_LABEL` from `env` and writes `/workspace/payload.json` containing a message and a list of numbers (`[10, 20, 30, 42]`).
2. **`transform`** (`runtime: js`, `depends_on: [extract]`): reads `/workspace/payload.json`, computes the sum (`102`), and prints the result to stdout.

## Workflow YAML Specification

```yaml
name: my-pipeline
tasks:
  - id: step_one
    runtime: python           # "python" or "js"
    env:                      # optional key-value environment variables
      MY_VAR: "value"
    script: |
      print("Hello from Python")

  - id: step_two
    runtime: js
    depends_on:               # optional list of upstream task IDs
      - step_one
    script: |
      console.log("Hello from Bun");
```

### Validation Rules

- `name` must be non-empty.
- `tasks` must contain at least one task.
- Each task `id` must be unique, non-empty, and must not contain whitespace, `/`, `\`, `.` or `..`.
- `runtime` must be `"python"` or `"js"`.
- `script` must be non-empty.
- Every entry in `depends_on` must reference an existing task `id`, cannot reference itself, and the resulting graph must be acyclic.

## How Execution Works

1. **Workspace setup**: generates a unique `run_id` (`YYYYMMDD-HHMMSS-<hex>`) and creates `./workspaces/<run_id>/.scripts` on the host.
2. **Topological ordering**: resolves task order via Kahn's algorithm so every dependency finishes before its dependents start.
3. **Container run**:
   - Writes the task script to `./workspaces/<run_id>/.scripts/<task_id>.py` (for `python`) or `.ts` (for `js`).
   - Mounts `./workspaces/<run_id>` as `/workspace` (`WorkingDir: /workspace`).
   - Runs `["uv", "run", "/workspace/.scripts/<task_id>.py"]` or `["bun", "run", "/workspace/.scripts/<task_id>.ts"]`.
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
