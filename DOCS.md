# Freud Technical Reference (`DOCS.md`)

Complete technical reference for **Freud (v0.1)** — configuration, CLI, YAML workflow specification, Dream Web UI, REST API v1, scoped Bearer tokens, `agent_madness`, MCP server, Git sync, retention/maintenance, and database backends.

> Looking for the quick overview? See [`README.md`](README.md).  
> Using an AI coding agent? See [`AGENTS.md`](AGENTS.md).

## Contents

- [Architecture & Project Structure](#architecture--project-structure)
- [Requirements & Build](#requirements--build)
- [CLI Commands](#cli-commands)
- [Configuration (`freud.toml` & `FREUD_*`)](#configuration-freudtoml--freud_)
- [Environment Variables (`.env` & Per-Run Overrides)](#environment-variables-env--per-run-overrides)
- [Example Flows (`flows/`)](#example-flows-flows)
- [Workflow YAML Specification](#workflow-yaml-specification)
- [How Execution Works](#how-execution-works)
- [REST API (`/api/v1`) & Token Scopes](#rest-api-apiv1--token-scopes)
- [Agent Madness Mode (`agent_madness`)](#agent-madness-mode-agent_madness)
- [MCP Server (Model Context Protocol)](#mcp-server-model-context-protocol)
- [Git Repository Sync (`[sync]`)](#git-repository-sync-sync)
- [Retention & Maintenance (`[retention]`)](#retention--maintenance-retention)
- [Database Backends](#database-backends)
- [Testing](#testing)

---

## Architecture & Project Structure

Freud compiles to a single pure-Go binary (`CGO_ENABLED=0`) with embedded Web UI templates/assets, pure-Go SQLite/PostgreSQL/MySQL drivers, Docker SDK execution, and an embedded MCP server:

```text
.
├── main.go                 # CLI entrypoint (`run`, `tui`, `dream`, `mcp`, `cache`)
├── flows/                  # Workflow YAML definitions (`etl-example.yaml`, `workspace-audit.yaml`, `full-pipeline.yaml`, `kill-demo.yaml`)
├── includes/               # Shared helper files mounted read-only at `/includes`
├── go.mod
├── go.sum
└── internal/
    ├── auth/               # Password hashing, session helpers, frd_ Bearer tokens & scopes (`read`, `run`, `write`)
    ├── config/             # `freud.toml`, `FREUD_*` env overrides, and path resolution
    ├── cron/               # 5-field cron & macro parser and next-run calculator
    ├── workflow/           # YAML parsing, validation, cycle detection, topological sort, canonical `SpecMarkdown`
    ├── env/                # `.env` loader, per-run env validation & merging (`MergeRunEnv`)
    ├── format/             # Shared timestamp, duration, and byte-size formatting helpers
    ├── db/                 # SQLite, PostgreSQL, and MySQL/MariaDB migrations and state store
    ├── runner/             # Docker SDK execution, image pull/cache (`orchestrator-cache:*`), log capture, cleanup
    ├── engine/             # Pipeline orchestration, subflows (`flow:`), kill control, workspace lifecycle
    ├── service/            # Shared DTOs, pagination, path-safe read & atomic `agent_madness` write/delete helpers
    ├── syncer/             # Git repository flow/include synchronization + GitHub/Gitea/GitLab push webhooks
    ├── maintenance/        # Age-based cleanup (runs, logs, workspaces, audit, sessions, Docker cache)
    ├── mcp/                # JSON-RPC 2.0 MCP server (`stdio` via `freud mcp` and Streamable HTTP `/mcp`)
    ├── term/               # Terminal ANSI color formatting and ASCII startup banner
    ├── tui/                # Interactive Bubbletea terminal UI (`1 Dashboard`, `2 Flows`, `3 Executions`, `4 Audit`)
    └── ui/                 # Embedded Dream Web UI, cron scheduler, REST API `/api/v1/*`, `/webhooks/sync`, `/mcp`
```

---

## Requirements & Build

- Go `1.27+` (matching `go.mod`)
- A running Docker daemon accessible to the CLI (default socket or `DOCKER_HOST`)
- Optional `git` binary on the host when `[sync].enabled = true`

```bash
go mod tidy
go build -o freud .
```

Static pure-Go build without CGO:

```bash
CGO_ENABLED=0 go build -o freud .
```

---

## CLI Commands

### Run a Single Workflow (`freud run`)

Executes a workflow YAML file directly and records the run under the local OS user (`auth.LocalActor()`):

```bash
./freud run flows/etl-example.yaml
./freud run --env-file .env.prod -e TARGET_DATE=2026-11-20 flows/etl-example.yaml
./freud run --no-cache flows/etl-example.yaml
```

- `-e KEY=VALUE` / `--env KEY=VALUE` can be repeated to inject per-run environment overrides.
- Interrupting with `Ctrl+C` (`SIGINT`/`SIGTERM`) cancels container execution cleanly and records active task/run failure status.

### Terminal UI (`freud tui`)

Interactive Bubbletea TUI (`1 Dashboard`, `2 Flows`, `3 Executions`, `4 Audit`) with passwordless local access (`source: "tui"`):

```bash
./freud tui
./freud tui --flows-dir flows --scheduler
```

### Web UI, Scheduler & REST/MCP Server (`freud dream`)

Starts the embedded Web UI + JSON REST API + Streamable HTTP `/mcp` (port `4321` by default, authentication enabled by default):

```bash
./freud dream
./freud dream --port 4321 --flows-dir flows --includes-dir includes --scheduler
./freud dream --agent-madness --scheduler
./freud dream --no-auth
```

On the very first launch with authentication enabled (when no admin exists), opening Dream UI redirects to **`/setup`** to create the initial `admin` user (password min 8 chars). Alternatively, set `FREUD_ADMIN_PASSWORD` to provision the initial `admin` user non-interactively at startup.

Dream Web UI pages:

- **Dashboard (`/`)**: execution stats, success rate, upcoming cron schedules, and recent runs (auto-refreshes via HTMX).
- **Flows (`/flows`, `/flows/{file}`)**: discovered YAML flows, Git sync card & **Sync Now**, cron schedule & next run, SVG dependency graph, syntax-highlighted YAML viewer, 1-click **Run**, and **Run with Env** parameter form.
- **Executions (`/executions`, `/executions/{id}`)**: paginated run history, status filter, search, `triggered_by`, streaming task logs, Gantt timeline, execution DAG, and **Kill** button.
- **Includes (`/includes`)**: file browser and viewer for `includes/`.
- **API (`/api`)**: interactive endpoint & MCP tool reference plus personal API token generator (`read` / `run` / `write` scopes) and revocation table.
- **Users & Audit (`/audit`)**: paginated audit trail (`SETUP_ADMIN`, `LOGIN`, `LOGOUT`, `FLOW_RUN`, `FLOW_KILL`, `FLOW_SAVED`, `FLOW_DELETED`, `INCLUDE_SAVED`, `INCLUDE_DELETED`, `USER_CREATED`, `USER_DELETED`, `PASSWORD_CHANGED`, `TOKEN_CREATED`, `TOKEN_REVOKED`, `FLOW_SYNC`, `FLOW_SYNC_FAILED`, `RETENTION_CLEANUP`, `RETENTION_UPDATED`, `CACHE_CLEARED`), password updates, and admin user management.
- **Maintenance (`/maintenance`)**: storage metrics (DB rows & SQLite size, `workspaces/` size, Docker cache images), live retention policy controls, **Run Cleanup Now**, and Docker cache clearing.

### Local MCP Server (`freud mcp`)

Runs MCP JSON-RPC 2.0 over `stdin`/`stdout` (all internal logs go to `stderr` so `stdout` carries clean JSON-RPC frames):

```bash
./freud mcp --config freud.toml
./freud mcp --token frd_your_token
```

### Dependency Cache (`freud cache`)

Inspect or clear cached `orchestrator-cache:*` Docker images:

```bash
./freud cache ls
./freud cache clear
```

---

## Configuration (`freud.toml` & `FREUD_*`)

Everything works out of the box with zero configuration (`orchestrator.db`, `workspaces/`, `flows/`, `includes/` in the current working directory). Configuration precedence (highest wins):

1. **CLI flags**: `--config`, `--db`, `--flows-dir`, `--includes-dir`, `--workspaces-dir`, `--env-file`, `--no-cache`, `--agent-madness`, `--port`, `--scheduler`, `--no-auth`, `--token` (on `mcp`), `-e`/`--env` (on `run`)
2. **Environment variables**: `FREUD_CONFIG`, `FREUD_DB`, `FREUD_FLOWS_DIR`, `FREUD_INCLUDES_DIR`, `FREUD_WORKSPACES_DIR`, `FREUD_ENV_FILE`, `FREUD_NO_CACHE`, `FREUD_AGENT_MADNESS`, `FREUD_PORT`, `FREUD_SCHEDULER`, `FREUD_NO_AUTH`, `FREUD_ADMIN_PASSWORD`, `FREUD_MCP_TOKEN`
3. **Optional `freud.toml`** in the current directory (or via `--config <path>`):

```toml
db = "./data/freud.db"              # SQLite path, postgres://..., mysql://..., or mariadb://...
workspaces_dir = "./data/workspaces"
flows_dir = "./pipelines"
includes_dir = "./shared_libs"
env_file = ".env.prod"
no_cache = false
agent_madness = false               # opt-in: allow admin + write tokens to edit/delete flows & includes when [sync].enabled = false

[dream]
port = 4321
scheduler = true
no_auth = false

[tui]
scheduler = false

[sync]
enabled = false
repo = "https://github.com/org/workflows.git"
branch = "main"
interval = "10m"                   # polling interval (e.g. "10m", or "0s" for webhook/manual only)
flows_path = ["flows"]             # string or list of repo directories containing .yaml flows
includes_path = "includes"         # optional repo directory synced into includes_dir
prune = true                       # remove local YAML/include files deleted in Git
# token = ""                       # prefer FREUD_SYNC_TOKEN env var for private HTTPS repos
# ssh_key = "~/.ssh/id_ed25519"    # or FREUD_SYNC_SSH_KEY
# webhook_secret = ""              # prefer FREUD_SYNC_WEBHOOK_SECRET env var

[retention]
interval = "6h"
runs_days = 0                      # delete finished runs & task_runs older than N days (0 = keep forever)
workspaces_days = 0                # delete workspaces/<run_id> directories older than N days
audit_days = 0                     # delete audit_logs entries older than N days
vacuum_sqlite = false
```

- Relative paths inside `freud.toml` are resolved relative to the directory containing `freud.toml`.
- Additional `[sync]` env overrides: `FREUD_SYNC_ENABLED`, `FREUD_SYNC_REPO`, `FREUD_SYNC_BRANCH`, `FREUD_SYNC_INTERVAL`, `FREUD_SYNC_CACHE_DIR`, `FREUD_SYNC_FLOWS_PATH` (comma-separated), `FREUD_SYNC_INCLUDES_PATH`, `FREUD_SYNC_PRUNE`, `FREUD_SYNC_TOKEN`, `FREUD_SYNC_SSH_KEY`, `FREUD_SYNC_WEBHOOK_SECRET`.
- Additional `[retention]` env overrides: `FREUD_RETENTION_INTERVAL`, `FREUD_RETENTION_RUNS_DAYS`, `FREUD_RETENTION_WORKSPACES_DAYS`, `FREUD_RETENTION_AUDIT_DAYS`, `FREUD_RETENTION_VACUUM_SQLITE` (also editable live in Dream UI under `/maintenance`).

---

## Environment Variables (`.env` & Per-Run Overrides)

- **Automatic `.env` loading**: when `--env-file` / `env_file` is omitted, Freud loads `.env` from the workflow's directory and/or the current working directory (working directory overrides on key collision).
- **Container injection**: all keys from `.env` are injected into task containers (`os.environ` in Python, `process.env` in JS/Bun, `$VAR` in Shell). Host OS environment variables are **not** forwarded automatically.
- **Task-level `env:` & `${VAR}` expansion**: `env:` entries in YAML support `${VAR}` references resolved against `.env` and per-run overrides.
- **Per-run overrides**: callers can pass temporary key-value overrides for a single run via:
  - CLI: `./freud run -e TARGET_DATE=2026-11-20 flows/etl-example.yaml`
  - Dream UI: **▶ Run with Env** on `/flows` or `/flows/{file}`
  - REST API: `POST /api/v1/flows/{file}/run` with `{"env": {"TARGET_DATE": "2026-11-20"}}`
  - MCP: `run_flow` with `{"file": "...", "env": {"TARGET_DATE": "2026-11-20"}}`
- **Precedence**: `.env` < task `env:` < per-run `env` (and per-run overrides propagate into child `flow:` subflows).
- **Secret protection**: variable values (from `.env` or per-run overrides) are **never** printed to orchestrator logs, returned by `/api/v1/context` or `get_context`, or stored in `audit_logs` (audit records only store key names as `env_keys=...`). Keys must match `[A-Za-z_][A-Za-z0-9_]*` with no null bytes in values.

---

## Example Flows (`flows/`)

1. **[`flows/etl-example.yaml`](flows/etl-example.yaml)** (`cron: "0 */6 * * *"`):
   - **`extract`** (`runtime: python`, `image: python:3.13-slim`, `dependencies: [pydantic, pyyaml]`, `include: [includes/payload_utils.py]`): installs `pydantic` and `pyyaml`, imports `payload_utils` from `/includes` via `PYTHONPATH`, reads `.env` variables, prints a YAML summary, and writes `/workspace/payload.json`.
   - **`transform`** (`runtime: js`, `depends_on: [extract]`, `dependencies: [lodash-es, ms]`, `include: [includes/math_utils.ts]`): installs `lodash-es` and `ms`, imports `formatStats` from `/includes/math_utils.ts`, reads `/workspace/payload.json`, computes stats with `lodash-es`, and prints stdout.
2. **[`flows/workspace-audit.yaml`](flows/workspace-audit.yaml)** (`cron: "*/30 * * * *"`, `include: [includes]`):
   - Three-step `alpine:3.20` -> `python` -> `js` pipeline inspecting `/includes` and publishing a summary.
3. **[`flows/full-pipeline.yaml`](flows/full-pipeline.yaml)** (`cron: "0 2 * * *"`):
   - Orchestrates `etl-example` and `workspace-audit` sequentially via `flow:` subflow tasks (passing an `env:` override) followed by a final summary step.
4. **[`flows/kill-demo.yaml`](flows/kill-demo.yaml)**:
   - Long-running demonstration flow useful for testing execution cancellation (`Kill`) in UI, TUI, API, or MCP.

---

## Workflow YAML Specification

```yaml
name: my-pipeline
cron: "0 */6 * * *"           # optional 5-field cron expression (or @hourly, @daily, etc.)
concurrency: 4                # optional max parallel tasks per run (0 = unlimited)
include:                      # optional workflow-wide read-only files/dirs mounted under /includes
  - includes/shared.py
tasks:
  - id: step_one
    runtime: python           # "python", "js", or "shell" (optional if image is set)
    image: python:3.13-slim   # optional Docker image override
    timeout: "5m"             # optional per-attempt timeout (Go duration)
    retries: 2                # optional retry attempts after failure/timeout
    retry_delay: "10s"        # optional wait between retries
    if: "${ENV_NAME} != 'off'"# optional condition (supports ==, !=, &&, ||, !, ())
    dependencies:             # optional packages installed & cached via orchestrator-cache:*
      - pydantic
      - pyyaml
    include:                  # optional per-task read-only files/dirs mounted under /includes
      - includes/payload_utils.py
    env:                      # optional key-value environment variables
      MY_VAR: "${ENV_NAME}"
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

  - id: notify_failure
    when: on_failure          # "on_success" (default), "on_failure", or "always"
    image: alpine:3.20
    script: |
      echo "Pipeline failed — run cleanup or send alert"
```

### Included Files (`/includes`)

- **Zero-copy read-only mounts**: included paths are bind-mounted with `ReadOnly: true` under `/includes` (never copied into `./workspaces/<run_id>`).
- **Auto-discovery**: if neither workflow nor task specifies `include:`, and an `includes/` directory exists next to the workflow YAML, it is automatically mounted read-only at `/includes`.
- **Python imports**: `PYTHONPATH=/workspace:/includes` is set automatically for `python` tasks (unless overridden in `env:`), allowing direct `import <module>` from `/includes`.
- **Path mapping**: paths under `includes/...` map to `/includes/...` (e.g. `includes/math_utils.ts` → `/includes/math_utils.ts`); other relative paths map to `/includes/<path>`.

### Validation Rules & DAG Semantics

- `name` must be non-empty; `concurrency` must be `>= 0` (`0` = unlimited parallel ready tasks).
- `tasks` must contain at least one task.
- Each task `id` must be unique, non-empty, and must not contain whitespace, `/`, `\`, `.` or `..`.
- `runtime` must be `"python"`, `"js"`, or `"shell"` (or `"subflow"` / `flow:`; if omitted and `image` is set, `runtime` defaults to `"shell"`).
- `when` must be empty / `"on_success"` (default), `"on_failure"`, or `"always"`. If `when: on_failure` has no `depends_on`, it runs after all regular tasks complete iff any task failed.
- `if` expressions are syntax-checked at parse time and evaluated against resolved task env (`.env` + `task.env` + per-run `env`); false conditions skip the task (`SKIPPED`) and downstream `on_success` tasks.
- `timeout` must be a positive Go duration (`> 0`), `retries` `>= 0`, `retry_delay` `>= 0`.
- `image` is optional for `python` and `js`, required for `shell`, and must not contain whitespace.
- `dependencies` is optional for `python` and `js` (not supported on `shell`); entries must be non-empty.
- `include` paths must be relative and stay inside the workflow directory (no `..` traversal).
- `script` must be non-empty for container tasks.
- Every entry in `depends_on` must reference an existing task `id`, cannot reference itself, and the graph must be acyclic.

---

## How Execution Works

1. **Workspace setup**: generates a unique `run_id` (`YYYYMMDD-HHMMSS-<hex>`) and creates `./workspaces/<run_id>/.scripts` on the host.
2. **Parallel DAG Scheduling**: validates acyclicity via Kahn's algorithm and executes all ready tasks (`inDegree == 0`) concurrently in goroutines (bounded by `concurrency` if set). Evaluates `when` trigger rules (`on_success` / `on_failure` / `always`), `if:` conditions, `timeout` per attempt, and `retries` / `retry_delay`.
3. **Dependency caching (when `dependencies` is non-empty)**: computes a SHA-256 content hash over `(base_image, runtime, sorted(dependencies))`, checks for an existing `orchestrator-cache:<hash>` image, and builds one once if missing (serialized safely for concurrent tasks).
4. **Container execution**:
   - Writes the task script to `./workspaces/<run_id>/.scripts/<task_id>.py` (`python`), `.ts` (`js`), or `.sh` (`shell`).
   - Mounts `./workspaces/<run_id>` at `/workspace` (`WorkingDir: /workspace`) and mounts `/includes` read-only.
   - Runs the script with the resolved container binary (`uv` / `python3` / `python` for Python, `bun` / `deno` / `node` for JS, `sh` for Shell).
   - Streams/captures stdout & stderr (`ContainerLogs`), stores logs and exit codes in the database, and removes the container (`ContainerRemove`).
5. **Dynamic Cron Scheduling**: when `--scheduler` / `scheduler = true` is active, the scheduler scans `flows/` every **15 seconds** with `workflow.LoadDir` — newly saved or updated flows are picked up automatically without restarting the server, with per-minute de-duplication and overlap protection (`RUNNING` runs are skipped).

---

## REST API (`/api/v1`) & Token Scopes

`./freud dream` serves the JSON REST API on the same port (`4321` by default).

### Authentication & Scopes

- Generate a Bearer token (`frd_...`) with a permission scope (`read`, `run`, or `write`; default `run`) in Web UI **API (`/api`)** or via `POST /api/v1/tokens` (using a session cookie or existing `write`-scoped token):
  - **`read`**: read-only endpoints/tools (`flows`, `includes`, `executions`, `audit`, `context`, `spec`, `maintenance` metrics).
  - **`run`**: includes `read`, plus triggering/killing flows and triggering Git sync.
  - **`write`**: includes `run`, plus `agent_madness` flow/include writes & deletes (requires an `admin` account and `sync.enabled = false`) and user/token/maintenance admin mutations.
- Header format:

```bash
export FREUD_TOKEN="frd_your_token_here"
curl -s -H "Authorization: Bearer $FREUD_TOKEN" http://localhost:4321/api/v1/flows | jq
```

- Every API action is recorded in `audit_logs` with `source: "api"` and `token=<name>` (plus `env_keys=...` when per-run env overrides are used, never values).
- Standard JSON error responses (`{"error": "..."}`):
  - `400 Bad Request` — invalid JSON, invalid filename/path, invalid YAML (`PUT`), or invalid per-run env key
  - `401 Unauthorized` — missing, invalid, or revoked Bearer token / session cookie
  - `403 Forbidden` — admin role required, insufficient token scope, or `agent_madness` disabled
  - `409 Conflict` — initial `/setup` pending, or write blocked because `[sync].enabled = true`
  - `422 Unprocessable Entity` — validation failure on `POST /api/v1/flows/validate` or failed Git sync
- When `--no-auth` is enabled, endpoints work locally without credentials (except `/api/v1/tokens`, which requires auth to be enabled).

### Endpoints

| Method | Path | Scope / Role | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/health` | Public | Server health (`status`, `auth_enabled`, `setup_required`, `scheduler_on`) |
| `GET` | `/api/v1/me` | `read` | Current authenticated user (`username`, `is_admin`, `created_at`) |
| `GET` | `/api/v1/context` | `read` | `.env` variable **names** only, counts, `agent_madness` / `writable_*` flags, sync status |
| `GET` | `/api/v1/spec` | `read` | Canonical workflow YAML specification (`?format=raw` for Markdown) |
| `GET` | `/api/v1/flows` | `read` | List discovered workflows in `flows/` with cron schedule and latest run |
| `POST` | `/api/v1/flows/validate` | `read` | Validate a workflow YAML draft (`{"yaml"}`) without saving to disk |
| `GET` | `/api/v1/flows/sync` | `read` | Current Git sync status, branch, commit SHA, and last error |
| `POST` | `/api/v1/flows/sync` | `run` | Trigger Git repository sync immediately |
| `GET` | `/api/v1/flows/{file}` | `read` | Flow details, parsed task list, raw YAML, and recent runs |
| `PUT` | `/api/v1/flows/{file}` | **Admin + `write`** | Create/update flat `.yaml`/`.yml` flow (`{"yaml"}`, validated before atomic write; requires `agent_madness=true`, Git sync off) |
| `DELETE` | `/api/v1/flows/{file}` | **Admin + `write`** | Delete flow YAML file (requires `agent_madness=true`, Git sync off) |
| `POST` | `/api/v1/flows/{file}/run` | `run` | Start workflow asynchronously (`202 Accepted` with `run_id`; optional body `{"env": {"KEY":"VAL"}}`) |
| `GET` | `/api/v1/includes` | `read` | List shared files under `includes/` |
| `GET` | `/api/v1/includes/{path}` | `read` | Read a shared file under `includes/` safely (max 512 KiB, no symlinks/traversal) |
| `PUT` | `/api/v1/includes/{path}` | **Admin + `write`** | Create/update shared file in `includes/` (`{"content"}`, max 512 KiB; requires `agent_madness=true`, Git sync off) |
| `DELETE` | `/api/v1/includes/{path}` | **Admin + `write`** | Delete shared file from `includes/` (requires `agent_madness=true`, Git sync off) |
| `GET` | `/api/v1/executions` | `read` | Stats + paginated runs (`?status=RUNNING\|SUCCESS\|FAILED\|KILLED&q=&limit=50&offset=0`) |
| `GET` | `/api/v1/executions/{id}` | `read` | Run metadata, child subflow runs, and task runs (`?tail_lines=100` optional) |
| `POST` | `/api/v1/executions/{id}/kill` | `run` | Terminate an active run and record `KILLED` |
| `GET` | `/api/v1/audit` | `read` | Paginated audit log (`?action=FLOW_RUN&q=&limit=100&offset=0`) |
| `GET` | `/api/v1/users` | `read` | List registered users (no password hashes) |
| `POST` | `/api/v1/users` | **Admin + `write`** | Create user (`{"username","password","is_admin"}`) |
| `DELETE` | `/api/v1/users/{username}` | **Admin + `write`** | Delete user and revoke all sessions & API tokens |
| `GET` | `/api/v1/tokens` | `read` | List API token metadata (admins see all; members see their own) |
| `POST` | `/api/v1/tokens` | `write` / session | Issue API token (`{"name":"github-ci","scope":"read\|run\|write"}`) — plaintext shown once |
| `DELETE` | `/api/v1/tokens/{id}` | Owner/Admin + `write` | Revoke an API token by ID |
| `GET` | `/api/v1/maintenance` | `read` | Storage metrics and active retention policy |
| `POST` | `/api/v1/maintenance/cleanup` | **Admin + `write`** | Trigger age-based retention cleanup immediately |
| `POST` | `/webhooks/sync` | Webhook Secret | Trigger Git sync from GitHub/Gitea/GitLab push webhooks |

### Quick `curl` Examples

```bash
# Trigger a flow with per-run env overrides and inspect logs
RUN_ID=$(curl -s -X POST \
  -H "Authorization: Bearer $FREUD_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"env":{"TARGET_DATE":"2026-11-20"}}' \
  http://localhost:4321/api/v1/flows/etl-example.yaml/run | jq -r '.run_id')

curl -s -H "Authorization: Bearer $FREUD_TOKEN" \
  "http://localhost:4321/api/v1/executions/$RUN_ID?tail_lines=100" | jq

# Kill a running execution
curl -s -X POST -H "Authorization: Bearer $FREUD_TOKEN" \
  "http://localhost:4321/api/v1/executions/$RUN_ID/kill" | jq
```

---

## Agent Madness Mode (`agent_madness`)

When enabled explicitly (`agent_madness = true` in `freud.toml`, `FREUD_AGENT_MADNESS=true`, or `--agent-madness`), admin agents/callers with a `write`-scoped token (or browser session) can create, update, and delete workflow YAML files in `flows/` and helper files in `includes/` over REST API or MCP.

Built-in safeguards:

1. **Disabled by default**: both `agent_madness` and `sync.enabled` default to `false`.
2. **Git Sync lock**: if `[sync].enabled = true`, all remote writes/deletes are rejected (`409 Conflict` / MCP error) so upstream Git remains the single source of truth.
3. **Dual authorization**: requires **both** `is_admin = true` AND token scope `write` (when auth is enabled).
4. **Pre-validation & atomic writes**: `SaveFlow` requires flat `.yaml`/`.yml` names and valid `workflow.Parse` before atomically renaming into `flows/`; invalid YAML never touches disk.
5. **Path & symlink protection**: `includes/` writes/deletes and reads enforce relative paths, block hidden/dot segments (`.env`, `.git`), block traversal, enforce a 512 KiB size cap, check every existing path component with `os.Lstat` to forbid symlinks, and verify `filepath.EvalSymlinks` stays inside the root directory.
6. **Full auditability**: every mutation writes `FLOW_SAVED`, `FLOW_DELETED`, `INCLUDE_SAVED`, or `INCLUDE_DELETED` into `audit_logs` with actor and token attribution.

---

## MCP Server (Model Context Protocol)

Freud embeds a zero-dependency **MCP JSON-RPC 2.0 server** supporting both **local `stdio`** (`./freud mcp`) and **remote Streamable HTTP** (`POST /mcp` in `./freud dream`).

### Tools & Resources

- **Tools**:
  - `get_context` (`{}`) — `.env` key **names** only (never values), counts, `agent_madness` / `writable_flows` / `writable_includes` / `write_block_reason`, Git sync status, and spec URI
  - `list_flows` (`{}`) — list discovered YAML workflows, cron schedules, and latest run status
  - `get_flow` (`{file}`) — tasks, dependencies, raw YAML, and recent runs
  - `validate_flow` (`{yaml}`) — validate a workflow YAML draft without writing to disk
  - `save_flow` (`{file, yaml}`) / `delete_flow` (`{file}`) — create/update/delete workflows (`agent_madness`, admin + `write` scope, Git sync off)
  - `list_includes` (`{}`) / `get_include` (`{path}`) — list and read shared files in `includes/`
  - `save_include` (`{path, content}`) / `delete_include` (`{path}`) — create/update/delete files in `includes/` (`agent_madness`, admin + `write` scope, Git sync off)
  - `run_flow` (`{file, env?, wait?, timeout_seconds?, tail_lines?}`) — trigger workflow with optional per-run `env` overrides; optionally wait up to 300s (`run` scope)
  - `list_executions` (`{status?, q?, limit?, offset?}`) — paginated execution history and aggregate stats
  - `get_execution` (`{run_id, tail_lines?}`) — run details, `triggered_by`, child subflows, and task logs (optionally tailed)
  - `kill_execution` (`{run_id}`) — cancel an active `RUNNING` execution (`run` scope)
  - `list_audit_logs` (`{action?, q?, limit?, offset?}`) — paginated activity audit log
  - `sync_flows` (`{}`) — trigger Git sync immediately (`run` scope)
- **Resources**:
  - `freud://docs/workflow-spec` (`text/markdown`) — canonical workflow YAML specification (`workflow.SpecMarkdown`)
  - `freud://flows/{file}` (`text/yaml`) — workflow YAML definitions
  - `freud://includes/{path}` (`text/plain`) — shared files inside `includes/`

### Local `stdio` Config Example

```json
{
  "mcpServers": {
    "freud": {
      "command": "/path/to/freud",
      "args": ["mcp", "--config", "/path/to/freud.toml"]
    }
  }
}
```

Pass `--token frd_...` (or set `FREUD_MCP_TOKEN`) to bind `stdio` MCP sessions to a specific Freud user account and enforce its role and token scope (`read` / `run` / `write`).

### Remote HTTP MCP Example

```bash
curl -s -X POST \
  -H "Authorization: Bearer $FREUD_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \
  http://localhost:4321/mcp | jq
```

---

## Git Repository Sync (`[sync]`)

Freud can automatically pull workflow YAML files and `includes/` from any Git repository (GitHub, GitLab, Gitea, Bitbucket, self-hosted) using the host `git` binary — on a timer (`interval`), immediately via webhook (`POST /webhooks/sync`), or manually via **Flows → Sync Now**, `POST /api/v1/flows/sync`, and MCP `sync_flows`.

- **Pre-validation before applying**: incoming workflows are validated in a staging directory via `workflow.LoadDir` before updating `flows/`. Invalid YAML or cyclic dependencies are rejected without breaking active schedules or flows.
- **Nested directories (`flows_path`)**: nested repository directories (e.g. `pipelines/billing/daily.yaml`) are flattened safely into `flows/` as `billing__daily.yaml`. Nested subdirectories under `includes_path` are preserved as-is.
- **Executions & Audit visibility**: sync runs appear as `git-sync` (`workflow_name: "git-sync"`) in **Executions** with full git/diff logs and record `FLOW_SYNC` / `FLOW_SYNC_FAILED` in **Users & Audit**.
- **Webhooks (`POST /webhooks/sync`)**: set `FREUD_SYNC_WEBHOOK_SECRET` and point your Git push webhook to `https://<host>/webhooks/sync`. Supports `X-Hub-Signature-256` (GitHub), `X-Gitea-Signature` (Gitea), `X-Gitlab-Token` (GitLab), and `Authorization: Bearer <secret>`. Pushes to other branches are ignored.

---

## Retention & Maintenance (`[retention]`)

Optional age-based cleanup for runs, workspaces, audit logs, expired sessions, and Docker cache:

- Configure defaults in `freud.toml` (`[retention]`) or `FREUD_RETENTION_*` env vars (`0` disables deletion for that resource).
- Update retention days live and trigger **Run Cleanup Now** or **Clear Cache** from Dream UI **`/maintenance`**, or call `GET /api/v1/maintenance` / `POST /api/v1/maintenance/cleanup`.
- Background cleanup runs periodically on `retention.interval` (when non-zero in `./freud dream`) and never touches active `RUNNING` executions or their workspaces.

---

## Database Backends

Freud supports **SQLite**, **PostgreSQL**, **MySQL**, and **MariaDB** with pure-Go drivers (`CGO_ENABLED=0`) and automatic startup migrations.

| Backend | Pure-Go Driver | Example `db` / `--db` / `FREUD_DB` value |
| --- | --- | --- |
| **SQLite** *(default)* | `modernc.org/sqlite` | `orchestrator.db` or `./data/freud.db` |
| **PostgreSQL** | `github.com/jackc/pgx/v5/stdlib` | `postgres://user:pass@localhost:5432/freud?sslmode=disable` |
| **MySQL** | `github.com/go-sql-driver/mysql` | `mysql://user:pass@localhost:3306/freud` (or `user:pass@tcp(localhost:3306)/freud`) |
| **MariaDB** | `github.com/go-sql-driver/mysql` | `mariadb://user:pass@localhost:3306/freud` |

Notes:
- **SQLite** configures `PRAGMA journal_mode=WAL;`, `PRAGMA busy_timeout=5000;`, and `PRAGMA foreign_keys=ON;`; parent directories for relative/custom `.db` files are created automatically.
- **MySQL / MariaDB** enforce `utf8mb4` (`LONGTEXT` for `logs` and `definition_json`) and `clientFoundRows=true`.
- Managed tables:
  - **`runs`**: `id`, `workflow_name`, `status`, `created_at`, `finished_at`, `definition_json`, `parent_run_id`, `parent_task_id`, `triggered_by`
  - **`task_runs`**: `id`, `run_id`, `task_id`, `status`, `exit_code`, `logs`, `started_at`, `finished_at`, `runtime`, `image`
  - **`users`**: `id`, `username`, `password_hash`, `is_admin`, `created_at`
  - **`sessions`**: `token`, `username`, `expires_at`, `created_at`
  - **`api_tokens`**: `id`, `username`, `name`, `scope`, `prefix`, `token_hash`, `last_used_at`, `created_at`
  - **`audit_logs`**: `id`, `username`, `action`, `target`, `source`, `details`, `created_at`
  - **`settings`**: `key_name`, `val`, `updated_at`

```bash
./freud dream --db "postgres://freud:secret@localhost:5432/freud?sslmode=disable"
./freud run --db "mariadb://freud:secret@localhost:3306/freud" flows/etl-example.yaml
sqlite3 orchestrator.db "SELECT id, workflow_name, status, created_at, finished_at FROM runs;"
```

---

## Testing

Run all unit tests across packages with race detector:

```bash
go test -count=1 -race -cover ./...
```

Run live PostgreSQL / MySQL database integration tests:

```bash
FREUD_TEST_POSTGRES_DSN="postgres://freud:freud@127.0.0.1:5432/freud?sslmode=disable" go test -v -race ./internal/db
FREUD_TEST_MYSQL_DSN="mysql://freud:freud@127.0.0.1:3306/freud" go test -v -race ./internal/db
```
