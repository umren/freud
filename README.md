# Freud (v0.1)

A pure-Go task orchestrator that parses YAML workflow definitions, resolves task dependencies as a DAG, executes tasks in ephemeral Docker containers, shares data across steps via a per-run workspace directory, and persists execution state and logs in SQLite, PostgreSQL, MySQL, or MariaDB.

## Features

- **Pure-Go stack (no CGO required)**: builds cleanly with `CGO_ENABLED=0`.
- **YAML DAG workflows**: defines tasks, environment variables, inline scripts, and `depends_on` edges with cycle and reference validation.
- **Dockerized runtimes with custom images**:
  - `python` (default `ghcr.io/astral-sh/uv:python3.12-bookworm-slim`, or any custom image such as `python:3.13-slim`)
  - `js` (default `oven/bun:latest`, or any custom image such as `oven/bun:1.2`, `node:22-alpine`)
  - `shell` (or omit `runtime` when `image` is specified, e.g. `alpine:3.20`)
- **Automatic image pulling & dependency caching**: checks local Docker images, pulls missing ones with live progress, and builds cached Docker image layers (`orchestrator-cache:<hash>`) for tasks with `dependencies` so subsequent runs start immediately without reinstalling packages.
- **Shared per-run workspace & read-only includes**: creates `./workspaces/<run_id>` on the host (mounted at `/workspace`) and mounts external helper modules/configs from `includes/` (or explicit `include:` paths) read-only at `/includes` with zero file copying.
- **Reliable log & status capture**: disables `AutoRemove`, waits for container completion, demultiplexes stdout/stderr, prints output, stores logs and exit codes in the database, and then removes the container.
- **Fail-fast execution & Kill support**: stops the pipeline immediately if a task exits with a non-zero status (`FAILED`), and supports canceling active runs and subflows (`KILLED`) from the Web UI or TUI.
- **Cron schedules, TUI & embedded Web UI**: workflows support standard 5-field `cron` expressions (and `@hourly`/`@daily`/etc.), `freud tui` provides an interactive Bubbletea terminal UI, and `freud dream` serves an embedded web UI (Go `html/template` + HTMX & Alpine.js) on port `4321`.
- **Multi-database persistence (pure-Go)**: defaults to local SQLite (`orchestrator.db` with `PRAGMA journal_mode=WAL;` and `PRAGMA busy_timeout=5000;`), and automatically switches to **PostgreSQL**, **MySQL**, or **MariaDB** when given a DSN.

## Requirements

- Go `1.27+` (matching `go.mod`)
- A running Docker daemon accessible to the CLI (via default socket or `DOCKER_HOST`)

## Project Structure

```text
.
├── main.go                 # CLI entrypoint (`run`, `tui`, `dream`, `cache`)
├── flows/                  # Workflow YAML definitions (`etl-example.yaml`, `workspace-audit.yaml`, `full-pipeline.yaml`, `kill-demo.yaml`)
├── includes/               # Shared helper files mounted read-only at `/includes`
├── go.mod
├── go.sum
└── internal/
    ├── config/             # `freud.toml`, `FREUD_*` env overrides, and path resolution
    ├── cron/               # 5-field cron parser and next-run calculator
    ├── workflow/           # YAML parsing, validation, cycle detection, topological sort
    ├── env/                # .env file loader and task environment merging
    ├── format/             # Shared timestamp, duration, and byte-size formatting helpers
    ├── db/                 # SQLite, PostgreSQL, and MySQL/MariaDB migrations and state store
    ├── runner/             # Docker SDK execution, image pull/cache, log capture, cleanup
    ├── engine/             # Pipeline orchestration, subflows, kill control, workspace lifecycle
    ├── term/               # Terminal ANSI color formatting and ASCII startup banner
    ├── tui/                # Interactive Bubbletea terminal UI (dashboard, flows, logs, run/kill)
    ├── service/            # Shared query helpers and JSON DTOs used by REST API and MCP
    ├── mcp/                # Model Context Protocol (MCP) server (stdio & Streamable HTTP)
    └── ui/                 # Embedded web UI, REST API v1, and HTTP /mcp endpoint
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

Start the interactive terminal UI (Dashboard, Flows + YAML viewer, live Executions/Logs, Audit Log, Run & Kill — passwordless local access, actions recorded as `local`):

```bash
./freud tui
# Optional flags:
./freud tui --flows-dir flows --scheduler
```

Start the embedded Web UI (default port `4321`, authentication enabled by default):

```bash
./freud dream
# Optional flags:
./freud dream --port 4321 --flows-dir flows --includes-dir includes --scheduler
# Optional local mode without login/auth:
./freud dream --no-auth
```

On the very first launch of `./freud dream` (when no admin user exists yet), opening the UI automatically redirects to **`/setup`** in your browser to create the initial `admin` account and password (at least 8 characters). Alternatively, set `FREUD_ADMIN_PASSWORD` to provision the initial admin non-interactively on startup.

The Web UI exposes six pages:
- **Dashboard (`/`)**: execution counts, success rate, upcoming cron runs, and recent executions (auto-refreshes via HTMX).
- **Flows (`/flows`)**: workflows discovered in `flows/`, cron schedules, SVG task topology, read-only YAML source viewer, and a **Run Now** button to trigger the flow immediately and jump to its live execution page.
- **Executions (`/executions`)**: newest-to-oldest execution history with status filter, search, who triggered each run, real-time streaming task logs, Gantt timeline, and execution topology.
- **Includes (`/includes`)**: read-only file browser for `includes/`.
- **API (`/api`)**: built-in REST API v1 & MCP endpoint/parameter reference plus personal Bearer API token creation and revocation.
- **Users & Audit (`/audit`)**: searchable activity audit log (`SETUP_ADMIN`, `LOGIN`, `LOGOUT`, `FLOW_RUN`, `FLOW_KILL`, `USER_CREATED`, `USER_DELETED`, `PASSWORD_CHANGED`, `TOKEN_CREATED`, `TOKEN_REVOKED`), password self-service, and admin user management (all users share the same flow permissions; admins can additionally add/remove users).

Manage cached dependency images:

```bash
./freud cache ls
./freud cache clear
```

### Configuration (`freud.toml` & Environment Variables)

Everything works out of the box with zero configuration (`orchestrator.db`, `workspaces/`, `flows/`, `includes/` in the current directory).
When you want custom locations, Freud loads settings with the following precedence (highest wins):

1. **CLI flags**: `--config`, `--db`, `--flows-dir`, `--includes-dir`, `--workspaces-dir`, `--env-file`, `--no-cache`, `--port`, `--scheduler`, `--no-auth`
2. **Environment variables**: `FREUD_CONFIG`, `FREUD_DB`, `FREUD_FLOWS_DIR`, `FREUD_INCLUDES_DIR`, `FREUD_WORKSPACES_DIR`, `FREUD_ENV_FILE`, `FREUD_NO_CACHE`, `FREUD_PORT`, `FREUD_SCHEDULER`, `FREUD_NO_AUTH`, `FREUD_ADMIN_PASSWORD`
3. **Optional `freud.toml`** in the current directory (or via `--config <path>`):

```toml
db = "./data/freud.db"              # SQLite path, postgres://..., mysql://..., or mariadb://...
workspaces_dir = "./data/workspaces"
flows_dir = "./pipelines"
includes_dir = "./shared_libs"
env_file = ".env.prod"
no_cache = false

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
```

Additional environment overrides for `[sync]`: `FREUD_SYNC_ENABLED`, `FREUD_SYNC_REPO`, `FREUD_SYNC_BRANCH`, `FREUD_SYNC_INTERVAL`, `FREUD_SYNC_CACHE_DIR`, `FREUD_SYNC_FLOWS_PATH` (comma-separated), `FREUD_SYNC_INCLUDES_PATH`, `FREUD_SYNC_PRUNE`, `FREUD_SYNC_TOKEN`, `FREUD_SYNC_SSH_KEY`, `FREUD_SYNC_WEBHOOK_SECRET`.

Relative paths inside `freud.toml` are resolved relative to the directory containing `freud.toml`.

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

## REST API (`/api/v1`)

`./freud dream` also serves a JSON REST API on the same port (`4321` by default) for CI/CD triggers, scripts, and monitoring.

### Authentication & API Tokens

* Generate a personal Bearer token (`frd_...`) in the Web UI under **API (`/api`) → API Tokens** or via `POST /api/v1/tokens` (signed in with a browser session cookie or existing token).
* Pass the token in the `Authorization` header:

```bash
export FREUD_TOKEN="frd_your_token_here"
curl -s -H "Authorization: Bearer $FREUD_TOKEN" http://localhost:4321/api/v1/flows | jq
```

* Every API action (`FLOW_RUN`, `FLOW_KILL`, `USER_CREATED`, `USER_DELETED`, `TOKEN_CREATED`, `TOKEN_REVOKED`) is recorded in `audit_logs` with `source: "api"` and the token name in `details`.
* Error responses use JSON (`{"error": "..."}`) with standard status codes:
  * `401 Unauthorized` — missing, invalid, or revoked Bearer token / session cookie
  * `403 Forbidden` — admin privileges required (`POST/DELETE /api/v1/users`, or revoking another user's token as non-admin)
  * `409 Conflict` — Dream UI authentication is enabled, and initial admin `/setup` has not been completed yet (except `GET /api/v1/health`)
* When `./freud dream --no-auth` is active, API endpoints work without a token (except `/api/v1/tokens`, which requires authentication to be enabled).

### Endpoints

| Method | Path | Auth / Role | Description |
| --- | --- | --- | --- |
| `GET` | `/api/v1/health` | Public | Server health (`status`, `auth_enabled`, `setup_required`, `scheduler_on`) |
| `GET` | `/api/v1/me` | Any user | Current authenticated user (`username`, `is_admin`, `created_at`) |
| `GET` | `/api/v1/flows` | Any user | List discovered workflows in `flows/` with cron schedule and latest run |
| `GET` | `/api/v1/flows/{file}` | Any user | Flow details, parsed task list, raw YAML, and recent runs |
| `POST` | `/api/v1/flows/{file}/run` | Any user | Start workflow execution asynchronously (`202 Accepted` with `run_id`) |
| `GET` | `/api/v1/executions` | Any user | Run stats + paginated runs (`?status=RUNNING\|SUCCESS\|FAILED\|KILLED&q=&limit=50&offset=0`) |
| `GET` | `/api/v1/executions/{id}` | Any user | Execution metadata, child subflow runs, and task runs including logs |
| `POST` | `/api/v1/executions/{id}/kill` | Any user | Terminate an active run and record `KILLED` status |
| `GET` | `/api/v1/audit` | Any user | Paginated activity audit log (`?action=FLOW_RUN&q=&limit=100&offset=0`) |
| `GET` | `/api/v1/users` | Any user | List registered accounts (without password hashes) |
| `POST` | `/api/v1/users` | **Admin only** | Create user (`{"username","password","is_admin"}`) |
| `DELETE` | `/api/v1/users/{username}` | **Admin only** | Delete user account (revokes sessions & API tokens) |
| `GET` | `/api/v1/tokens` | Any user | List API tokens (admins see all tokens; members see their own) |
| `POST` | `/api/v1/tokens` | Any user | Issue a new API token (`{"name":"github-ci"}`) — raw token returned once |
| `DELETE` | `/api/v1/tokens/{id}` | Owner or Admin | Revoke an API token by ID |

### Quick Examples

Trigger a flow and poll its status + task logs:

```bash
# 1. Start etl-example.yaml
RUN_ID=$(curl -s -X POST \
  -H "Authorization: Bearer $FREUD_TOKEN" \
  http://localhost:4321/api/v1/flows/etl-example.yaml/run | jq -r '.run_id')

# 2. Inspect execution status, exit codes, and logs
curl -s -H "Authorization: Bearer $FREUD_TOKEN" \
  "http://localhost:4321/api/v1/executions/$RUN_ID" | jq

# 3. Kill a running execution if needed
curl -s -X POST -H "Authorization: Bearer $FREUD_TOKEN" \
  "http://localhost:4321/api/v1/executions/$RUN_ID/kill" | jq
```

Create a user (admin token required) and inspect recent audit events:

```bash
curl -s -X POST \
  -H "Authorization: Bearer $FREUD_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"username":"ci-bot","password":"strong-password","is_admin":false}' \
  http://localhost:4321/api/v1/users | jq

curl -s -H "Authorization: Bearer $FREUD_TOKEN" \
  "http://localhost:4321/api/v1/audit?limit=20" | jq
```

## MCP Server (Model Context Protocol)

Freud embeds a zero-dependency **MCP JSON-RPC 2.0 server** inside the same binary, supporting both **local `stdio`** (`./freud mcp`) and **remote Streamable HTTP** (`POST /mcp` on `./freud dream`).

### Exposed Tools & Resources

- **Tools**:
  - `list_flows` — list discovered YAML flows, cron schedules, and latest run status
  - `get_flow` (`{file}`) — inspect workflow tasks, dependencies, raw YAML, and recent runs
  - `run_flow` (`{file, wait?, timeout_seconds?}`) — trigger a workflow asynchronously or wait (up to 300s) for completion & task logs
  - `list_executions` (`{status?, q?, limit?, offset?}`) — query paginated execution history and status stats
  - `get_execution` (`{run_id}`) — inspect execution status, `triggered_by`, child subflows, and task logs
  - `kill_execution` (`{run_id}`) — terminate an active `RUNNING` execution
  - `list_audit_logs` (`{action?, q?, limit?, offset?}`) — query paginated activity audit trail
  - `sync_flows` — trigger Git repository sync now and return commit/diff summary & `run_id`
- **Resources**:
  - `freud://flows/{file}` (`text/yaml`) via `resources/list` and `resources/read`

### Local `stdio` Mode (`./freud mcp`)

Runs passwordless as your OS user by default (`source: "mcp"`, `details: "stdio"`), or attributes runs to a specific Freud account with `--token` / `FREUD_MCP_TOKEN`. All orchestrator output goes to `stderr` so `stdout` carries clean JSON-RPC frames.

Example MCP client config (Claude Desktop, Cursor, Antigravity, etc.):

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

### Remote HTTP Mode (`/mcp` in `./freud dream`)

`POST http://localhost:4321/mcp` accepts MCP JSON-RPC 2.0 requests using the same `Authorization: Bearer frd_...` token (or session cookie) and logs all tool executions in `audit_logs` under the token owner with `source: "mcp"`:

```bash
curl -s -X POST \
  -H "Authorization: Bearer $FREUD_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' \
  http://localhost:4321/mcp | jq
```

## Git Repository Sync (`[sync]`)

Freud can automatically pull workflow YAML files and shared `includes/` from any Git repository (GitHub, GitLab, Gitea, Bitbucket, self-hosted) using the host `git` binary — on a timer (`interval`), immediately via webhook (`POST /webhooks/sync`), or manually via **Flows → Sync Now**, `POST /api/v1/flows/sync`, and the `sync_flows` MCP tool.

- **Pre-validation before applying:** incoming workflows are validated in a staging directory via `workflow.LoadDir` before updating `flows/`. Invalid YAML or cyclic dependencies are rejected without breaking active schedules or flows.
- **Nested directories (`flows_path`):** if workflows inside a repository folder are organized in subdirectories (for example `pipelines/billing/daily.yaml`), Freud flattens them safely into `flows/` as `billing__daily.yaml`. Nested subdirectories under `includes_path` are preserved as-is.
- **Executions & Audit visibility:** whenever new commits arrive, sync fails, or sync is triggered manually/via webhook/API/MCP, a `git-sync` run (`workflow_name: "git-sync"`) is recorded with full git/diff/error logs so it appears directly in **Executions** (Web UI & TUI) alongside `FLOW_SYNC` / `FLOW_SYNC_FAILED` entries in **Users & Audit**.
- **Webhooks (`POST /webhooks/sync`):** set `FREUD_SYNC_WEBHOOK_SECRET` and point your Git provider push webhook to `https://<host>/webhooks/sync`. Supports `X-Hub-Signature-256` (GitHub), `X-Gitea-Signature` (Gitea), `X-Gitlab-Token` (GitLab), and `Authorization: Bearer <secret>`. Pushes to branches other than `sync.branch` are safely ignored.

## Database Backends

Freud supports **SQLite**, **PostgreSQL**, **MySQL**, and **MariaDB** out of the box using pure-Go drivers (no CGO required) and automatically runs schema migrations on startup.

The backend is selected automatically from the `db` setting (`--db`, `FREUD_DB`, or `db` in `freud.toml`):

| Backend | Pure-Go Driver | Example `--db` / `FREUD_DB` / `freud.toml` value |
| --- | --- | --- |
| **SQLite** *(default)* | `modernc.org/sqlite` | `orchestrator.db` or `./data/freud.db` |
| **PostgreSQL** | `github.com/jackc/pgx/v5/stdlib` | `postgres://user:pass@localhost:5432/freud?sslmode=disable` |
| **MySQL** | `github.com/go-sql-driver/mysql` | `mysql://user:pass@localhost:3306/freud` (or `user:pass@tcp(localhost:3306)/freud`) |
| **MariaDB** | `github.com/go-sql-driver/mysql` | `mariadb://user:pass@localhost:3306/freud` |

Notes:
- **SQLite** uses `PRAGMA journal_mode=WAL;`, `PRAGMA busy_timeout=5000;`, and `PRAGMA foreign_keys=ON;`; parent directories for relative/custom `.db` file paths are created automatically.
- **MySQL / MariaDB** connections enforce `utf8mb4` (`LONGTEXT` for `logs` and `definition_json`) and `clientFoundRows=true`.
- Schema tables created automatically:
  - **`runs`**: `id`, `workflow_name`, `status` (`RUNNING`, `SUCCESS`, `FAILED`, `KILLED`), `created_at`, `finished_at`, `definition_json`, `parent_run_id`, `parent_task_id`, `triggered_by`
  - **`task_runs`**: `id`, `run_id`, `task_id`, `status`, `exit_code`, `logs`, `started_at`, `finished_at`, `runtime`, `image`
  - **`users`**: `id`, `username`, `password_hash`, `is_admin`, `created_at`
  - **`sessions`**: `token`, `username`, `expires_at`, `created_at`
  - **`api_tokens`**: `id`, `username`, `name`, `prefix`, `token_hash`, `last_used_at`, `created_at`
  - **`audit_logs`**: `id`, `username`, `action`, `target`, `source`, `details`, `created_at`

Example launching Dream UI or running a flow against PostgreSQL or MariaDB:

```bash
./freud dream --db "postgres://freud:secret@localhost:5432/freud?sslmode=disable"
./freud run --db "mariadb://freud:secret@localhost:3306/freud" flows/etl-example.yaml
```

You can inspect stored runs in SQLite with `sqlite3`:

```bash
sqlite3 orchestrator.db "SELECT id, workflow_name, status, created_at, finished_at FROM runs;"
sqlite3 orchestrator.db "SELECT id, run_id, task_id, status, exit_code FROM task_runs;"
```

## Testing

Run all unit tests (including SQLite store, config, workflow validation, engine orchestration, TUI, and Web UI):

```bash
go test -race ./...
```

To also run the database integration tests against live PostgreSQL and MySQL/MariaDB instances:

```bash
FREUD_TEST_POSTGRES_DSN="postgres://freud:freud@127.0.0.1:5432/freud?sslmode=disable" go test -v -race ./internal/db
FREUD_TEST_MYSQL_DSN="mysql://freud:freud@127.0.0.1:3306/freud" go test -v -race ./internal/db
```
