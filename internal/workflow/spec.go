package workflow

// SpecMarkdown documents Freud's YAML workflow schema and runtime behavior for agents and users.
const SpecMarkdown = `# Freud Workflow YAML Specification

Workflows are YAML files stored in the configured flows directory (` + "`flows/`" + `).

## Top-Level Fields

- ` + "`name`" + ` (string, **required**): Non-empty unique workflow identifier.
- ` + "`cron`" + ` (string, optional): Standard 5-field cron expression or macro (` + "`@hourly`" + `, ` + "`@daily`" + `, ` + "`@weekly`" + `, etc.). Triggers automatically when scheduler is enabled.
- ` + "`include`" + ` (list of strings, optional): Relative file or directory paths inside the project/includes directory mounted read-only under ` + "`/includes`" + ` inside every task container. Cannot traverse outside (` + "`..`" + ` rejected).
- ` + "`tasks`" + ` (list of Task, **required**, at least 1): Execution steps resolved topologically via ` + "`depends_on`" + `.

## Task Fields

- ` + "`id`" + ` (string, **required**): Unique within workflow; non-empty; no whitespace, ` + "`/`" + `, ` + "`\\`" + `, ` + "`.`" + `, or ` + "`..`" + `.
- ` + "`depends_on`" + ` (list of strings, optional): Upstream task IDs that must succeed before this task runs. Graph must be acyclic.

### Container Tasks
- ` + "`runtime`" + ` (string): ` + "`\"python\"`" + `, ` + "`\"js\"`" + `, or ` + "`\"shell\"`" + ` (defaults to ` + "`\"shell\"`" + ` if omitted when ` + "`image`" + ` is set).
- ` + "`image`" + ` (string, optional for ` + "`python`" + `/` + "`js`" + `; **required** for ` + "`shell`" + `): Docker image tag (no whitespace).
- ` + "`dependencies`" + ` (list of strings, optional for ` + "`python`" + `/` + "`js`" + `; **not supported** on ` + "`shell`" + `): Packages installed and cached into ` + "`orchestrator-cache:*`" + ` layers.
- ` + "`include`" + ` (list of strings, optional): Per-task relative paths mounted read-only at ` + "`/includes`" + `.
- ` + "`env`" + ` (map<string,string>, optional): Task environment overrides. Supports referencing ` + "`.env`" + ` or per-run variables via ` + "`${VAR}`" + ` syntax.
- ` + "`script`" + ` (string, **required** for container tasks): Script body written to ` + "`/workspace/.scripts/<task_id>.(py|ts|sh)`" + `.

### Subflow Tasks (mutually exclusive with container fields except ` + "`id`" + `, ` + "`depends_on`" + `, ` + "`env`" + `)
- ` + "`flow`" + ` (string, **required** for subflow): Name of target workflow (` + "`Workflow.Name`" + `) to invoke.
- ` + "`wait`" + ` (bool, optional, default ` + "`true`" + `): Wait for child workflow completion; ` + "`false`" + ` triggers fire-and-forget.

## Runtime Environment Inside Containers

- Working directory: ` + "`/workspace`" + ` (shared across tasks within the same run ID).
- Includes mount: ` + "`/includes`" + ` (read-only). ` + "`PYTHONPATH`" + ` includes ` + "`/includes`" + ` for Python tasks.
- Base environment: All keys from loaded ` + "`.env`" + ` file(s) are injected into task containers (values are never logged).
- Per-run overrides: Callers may pass temporary ` + "`env`" + ` parameters per execution (CLI ` + "`-e/--env`" + `, Dream UI, REST API ` + "`{\"env\": ...}`" + `, MCP ` + "`run_flow`" + `). Precedence: ` + "`.env`" + ` < task ` + "`env:`" + ` < per-run ` + "`env`" + ` (and ` + "`${VAR}`" + ` inside task ` + "`env:`" + ` resolves against merged ` + "`.env`" + ` + per-run ` + "`env`" + `).

## Example

` + "```yaml" + `
name: my-pipeline
cron: "0 */6 * * *"
include:
  - includes/shared.py
tasks:
  - id: extract
    runtime: python
    dependencies: [pydantic, pyyaml]
    env:
      TARGET_ENV: "${ENV_NAME}"
    script: |
      from shared import helper
      print(helper())

  - id: transform
    runtime: js
    depends_on: [extract]
    dependencies: [lodash-es]
    script: |
      console.log("transform step")
` + "```" + `
`
