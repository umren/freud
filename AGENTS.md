# AGENTS.md — Freud Guide for AI & Coding Agents

Freud is a single-binary Go container workflow orchestrator (`CGO_ENABLED=0`) with Dream Web UI, Bubbletea TUI, REST API v1, Git Sync, Retention/Maintenance, and an embedded MCP server.

## Architecture & Packages

- `main.go` — CLI entrypoint (`run`, `dream`, `tui`, `mcp`, `cache ls|clear`).
- `internal/workflow/` — YAML parser, validator, cycle detector, topological sort (`Workflow`, `Task`, canonical `SpecMarkdown`).
- `internal/cron/` — 5-field cron & macro parser.
- `internal/env/` — `.env` loader, per-run env validation & merging (`LoadDefault`, `ParseFile`, `ValidateEnvMap`, `MergeRunEnv`, `SortedKeys`). **Values are never logged or exposed via API/MCP.**
- `internal/db/` — Unified store supporting **SQLite**, **PostgreSQL**, and **MySQL/MariaDB** (`runs`, `task_runs`, `users`, `sessions`, `api_tokens`, `audit_logs`, `settings`).
- `internal/runner/` — Docker SDK execution & `orchestrator-cache:*` dependency layer caching.
- `internal/engine/` — Pipeline execution engine, subflows (`flow:`), workspace management, kill propagation, `TriggeredBy` context.
- `internal/service/` — Shared DTOs, pagination helpers, path-safe include/flow read & atomic `agent_madness` write/delete helpers, and log tailing (`ApplyTailLines`).
- `internal/syncer/` — `os/exec git` repository sync (`flows_path`, `includes_path`, `prune`) + GitHub/Gitea/GitLab webhook verification.
- `internal/maintenance/` — Age-based cleanup for runs/task_runs, `workspaces/<run_id>`, audit logs, expired sessions, and Docker cache.
- `internal/mcp/` — JSON-RPC 2.0 MCP server (`stdio` via `freud mcp`, Streamable HTTP via `POST /mcp`).
- `internal/ui/` — Dream Web UI (embedded Go templates + HTMX/Alpine), `/api` docs, REST API `/api/v1/*`, `/webhooks/sync`, `/mcp`.
- `internal/tui/` — Bubbletea interactive terminal UI (`1 Dashboard`, `2 Flows`, `3 Executions`, `4 Audit`).

## Operating Rules for Agents

1. **Workflow YAML & Includes Conventions**
   - Always check the live schema via MCP resource `freud://docs/workflow-spec` or `GET /api/v1/spec`, and validate drafts with `validate_flow` / `POST /api/v1/flows/validate` before deploying.
   - `workflow.LoadDir` expects flat `.yaml`/`.yml` files directly inside `flows/`. When Git sync pulls nested subdirectories (e.g. `billing/daily.yaml`), they are flattened as `billing__daily.yaml`.
   - Shared scripts live under `includes/` (mounted read-only at `/includes` in containers; `PYTHONPATH` includes `/includes` for Python). Inspect them remotely via `list_includes` / `get_include` or `/api/v1/includes`.
   - Call `get_context` or `GET /api/v1/context` to see available `.env` variable **names** (`env_keys`) before writing `${VAR}` references; `.env` values are intentionally never exposed.
   - When diagnosing noisy task failures, pass `tail_lines` to `get_execution` / `run_flow` / `GET /api/v1/executions/{id}?tail_lines=100`.
2. **Git Sync Awareness & `agent_madness`**
   - Both `[sync].enabled` and `agent_madness` default to `false`.
   - If `[sync]` is active (`sync.enabled = true`), manage workflow and include edits in the upstream Git repository and trigger `sync_flows` / `POST /api/v1/flows/sync`. Remote file writes/deletes (`PUT`/`DELETE` API and `save_*`/`delete_*` MCP tools) are automatically blocked whenever Git sync is enabled.
   - When `agent_madness = true` (`FREUD_AGENT_MADNESS=true` / `--agent-madness`) and Git sync is disabled, admin users with a `write`-scoped token (or browser session) can create, update, and delete flows (`PUT/DELETE /api/v1/flows/{file}`, `save_flow`, `delete_flow`) and includes (`PUT/DELETE /api/v1/includes/{path}`, `save_include`, `delete_include`). Check `get_context` / `GET /api/v1/context` (`writable_flows`, `writable_includes`, `write_block_reason`) before attempting writes.
3. **MCP `stdio` Safety**
   - In `./freud mcp`, `stdout` is strictly reserved for JSON-RPC 2.0 messages (`os.Stdout` is redirected to `os.Stderr` in `main.go`). Never write logs or debug output to stdout in CLI/engine paths invoked by `mcp`.
4. **Authentication, Scopes & Audit**
   - `freud run` and `freud tui` run passwordless locally as `auth.LocalActor()`.
   - Remote REST API (`/api/v1/*`) and HTTP MCP (`/mcp`) authenticate with `Authorization: Bearer frd_<token>` (generated in Dream UI `/api` or `POST /api/v1/tokens`) and record every action into `triggered_by` and `audit_logs`.
   - API tokens support three hierarchical scopes: `read` (inspect only), `run` (default — also run/kill/sync flows), and `write` (required together with an `admin` account for `agent_madness` file mutations and user/token/maintenance admin mutations).

## Verification Commands

```bash
gofmt -l .
go vet ./...
go test -count=1 -race -cover ./...
CGO_ENABLED=0 go build -o freud .
```
