# AGENTS.md — Freud Guide for AI & Coding Agents

Freud is a single-binary Go container workflow orchestrator (`CGO_ENABLED=0`) with Dream Web UI, Bubbletea TUI, REST API v1, Git Sync, Retention/Maintenance, and an embedded MCP server.

## Architecture & Packages

- `main.go` — CLI entrypoint (`run`, `dream`, `tui`, `mcp`, `cache ls|clear`).
- `internal/workflow/` — YAML parser, validator, cycle detector, topological sort (`Workflow`, `Task`, canonical `SpecMarkdown`).
- `internal/cron/` — 5-field cron & macro parser.
- `internal/env/` — `.env` loader (`LoadDefault`, `ParseFile`, `SortedKeys`). **Values are never logged or exposed via API/MCP.**
- `internal/db/` — Unified store supporting **SQLite**, **PostgreSQL**, and **MySQL/MariaDB** (`runs`, `task_runs`, `users`, `sessions`, `api_tokens`, `audit_logs`, `settings`).
- `internal/runner/` — Docker SDK execution & `orchestrator-cache:*` dependency layer caching.
- `internal/engine/` — Pipeline execution engine, subflows (`flow:`), workspace management, kill propagation, `TriggeredBy` context.
- `internal/service/` — Shared DTOs, pagination helpers, path-safe `ListIncludes` / `ReadInclude`, and log tailing (`ApplyTailLines`).
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
2. **Git Sync Awareness**
   - If `[sync]` is active (`sync.enabled = true` with `prune = true`), manage workflow edits in the upstream Git repository and trigger `sync_flows` / `POST /api/v1/flows/sync` rather than hand-editing `flows/`, otherwise pruned changes will be overwritten.
3. **MCP `stdio` Safety**
   - In `./freud mcp`, `stdout` is strictly reserved for JSON-RPC 2.0 messages (`os.Stdout` is redirected to `os.Stderr` in `main.go`). Never write logs or debug output to stdout in CLI/engine paths invoked by `mcp`.
4. **Authentication & Audit**
   - `freud run` and `freud tui` run passwordless locally as `auth.LocalActor()`.
   - Remote REST API (`/api/v1/*`) and HTTP MCP (`/mcp`) authenticate with `Authorization: Bearer frd_<token>` (generated in Dream UI `/api` or `POST /api/v1/tokens`) and record every action into `triggered_by` and `audit_logs`.

## Verification Commands

```bash
gofmt -l .
go vet ./...
go test -count=1 -race -cover ./...
CGO_ENABLED=0 go build -o freud .
```
