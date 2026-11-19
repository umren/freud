package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/mattn/go-isatty"

	"freud/internal/workflow"
)

const (
	defaultPythonImage = "ghcr.io/astral-sh/uv:python3.12-bookworm-slim"
	defaultJSImage     = "oven/bun:latest"

	containerWorkspaceDir = "/workspace"
	scriptsSubdir         = ".scripts"
)

// Result holds the execution outcome of a single task container.
type Result struct {
	ExitCode int64
	Logs     string
}

// RuntimeSpec defines the image, script extension, and command for a task execution.
type RuntimeSpec struct {
	Image     string
	Extension string
	Command   func(containerScriptPath string) []string
}

const (
	pythonWithDepsScript = `script="$1"
shift
if command -v uv >/dev/null 2>&1; then
	uv venv /tmp/.venv >/dev/null 2>&1 && uv pip install --python /tmp/.venv/bin/python "$@" && exec /tmp/.venv/bin/python "$script"
elif command -v python3 >/dev/null 2>&1; then
	PIP_ROOT_USER_ACTION=ignore python3 -m pip install --no-cache-dir --disable-pip-version-check "$@" && exec python3 "$script"
else
	PIP_ROOT_USER_ACTION=ignore python -m pip install --no-cache-dir --disable-pip-version-check "$@" && exec python "$script"
fi`

	jsWithDepsScript = `script="$1"
shift
if command -v bun >/dev/null 2>&1; then
	mkdir -p /tmp/deps && (cd /tmp/deps && bun add "$@") && NODE_PATH="/tmp/deps/node_modules${NODE_PATH:+:$NODE_PATH}" exec bun run "$script"
elif command -v npm >/dev/null 2>&1; then
	mkdir -p /tmp/deps && npm install --prefix /tmp/deps --no-save "$@" && NODE_PATH="/tmp/deps/node_modules${NODE_PATH:+:$NODE_PATH}" exec node "$script"
else
	echo "No supported JS package manager (bun or npm) found in image" >&2
	exit 1
fi`
)

func resolveRuntimeSpec(task workflow.Task) (RuntimeSpec, error) {
	deps := task.Dependencies

	switch task.Runtime {
	case workflow.RuntimePython:
		img := task.Image
		if img == "" {
			return RuntimeSpec{
				Image:     defaultPythonImage,
				Extension: ".py",
				Command: func(scriptPath string) []string {
					if len(deps) == 0 {
						return []string{"uv", "run", scriptPath}
					}
					cmd := make([]string, 0, 3+len(deps)*2)
					cmd = append(cmd, "uv", "run")
					for _, dep := range deps {
						cmd = append(cmd, "--with", dep)
					}
					cmd = append(cmd, scriptPath)
					return cmd
				},
			}, nil
		}
		return RuntimeSpec{
			Image:     img,
			Extension: ".py",
			Command: func(scriptPath string) []string {
				if len(deps) == 0 {
					return []string{
						"sh", "-c",
						`if command -v uv >/dev/null 2>&1; then exec uv run "$1"; elif command -v python3 >/dev/null 2>&1; then exec python3 "$1"; else exec python "$1"; fi`,
						"sh", scriptPath,
					}
				}
				cmd := make([]string, 0, 5+len(deps))
				cmd = append(cmd, "sh", "-c", pythonWithDepsScript, "sh", scriptPath)
				cmd = append(cmd, deps...)
				return cmd
			},
		}, nil

	case workflow.RuntimeJS:
		img := task.Image
		if img == "" {
			return RuntimeSpec{
				Image:     defaultJSImage,
				Extension: ".ts",
				Command: func(scriptPath string) []string {
					if len(deps) == 0 {
						return []string{"bun", "run", scriptPath}
					}
					cmd := make([]string, 0, 5+len(deps))
					cmd = append(cmd, "sh", "-c", jsWithDepsScript, "sh", scriptPath)
					cmd = append(cmd, deps...)
					return cmd
				},
			}, nil
		}
		return RuntimeSpec{
			Image:     img,
			Extension: ".ts",
			Command: func(scriptPath string) []string {
				if len(deps) == 0 {
					return []string{
						"sh", "-c",
						`if command -v bun >/dev/null 2>&1; then exec bun run "$1"; elif command -v deno >/dev/null 2>&1; then exec deno run --allow-all "$1"; else exec node "$1"; fi`,
						"sh", scriptPath,
					}
				}
				cmd := make([]string, 0, 5+len(deps))
				cmd = append(cmd, "sh", "-c", jsWithDepsScript, "sh", scriptPath)
				cmd = append(cmd, deps...)
				return cmd
			},
		}, nil

	case workflow.RuntimeShell, "":
		if task.Image == "" {
			return RuntimeSpec{}, fmt.Errorf("task %q with runtime %q requires an image", task.ID, task.Runtime)
		}
		if len(deps) > 0 {
			return RuntimeSpec{}, fmt.Errorf("task %q with runtime %q does not support dependencies", task.ID, task.Runtime)
		}
		return RuntimeSpec{
			Image:     task.Image,
			Extension: ".sh",
			Command: func(scriptPath string) []string {
				return []string{"sh", scriptPath}
			},
		}, nil

	default:
		return RuntimeSpec{}, fmt.Errorf("unsupported runtime %q for task %q", task.Runtime, task.ID)
	}
}

// DockerRunner executes workflow tasks inside ephemeral Docker containers.
type DockerRunner struct {
	cli *client.Client
}

// NewDockerRunner initializes a Docker SDK client using environment settings and API negotiation.
func NewDockerRunner() (*DockerRunner, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}
	return &DockerRunner{cli: cli}, nil
}

// Close releases resources held by the Docker client.
func (r *DockerRunner) Close() error {
	if r.cli == nil {
		return nil
	}
	return r.cli.Close()
}

// RunTask writes the task script to the shared workspace, ensures the runtime image exists,
// executes the container, captures logs, and removes the container afterward.
func (r *DockerRunner) RunTask(ctx context.Context, runID, hostWorkspace string, task workflow.Task) (*Result, error) {
	spec, err := resolveRuntimeSpec(task)
	if err != nil {
		return nil, err
	}

	absWorkspace, err := filepath.Abs(hostWorkspace)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace path %q: %w", hostWorkspace, err)
	}

	scriptFilename := task.ID + spec.Extension
	hostScriptPath := filepath.Join(absWorkspace, scriptsSubdir, scriptFilename)
	if err := os.WriteFile(hostScriptPath, []byte(task.Script), 0o644); err != nil {
		return nil, fmt.Errorf("write script file %q: %w", hostScriptPath, err)
	}

	if err := r.ensureImage(ctx, spec.Image); err != nil {
		return nil, err
	}

	containerScriptPath := containerWorkspaceDir + "/" + scriptsSubdir + "/" + scriptFilename
	cfg := &container.Config{
		Image:      spec.Image,
		Cmd:        spec.Command(containerScriptPath),
		Env:        formatEnv(task.Env),
		WorkingDir: containerWorkspaceDir,
		Tty:        false,
	}
	hostCfg := &container.HostConfig{
		AutoRemove: false,
		Mounts: []mount.Mount{
			{
				Type:   mount.TypeBind,
				Source: absWorkspace,
				Target: containerWorkspaceDir,
			},
		},
	}

	containerName := fmt.Sprintf("orch-%s-%s", runID, task.ID)
	created, err := r.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, containerName)
	if err != nil {
		return nil, fmt.Errorf("create container for task %q: %w", task.ID, err)
	}

	// Ensure the container is removed only after logs have been collected or on setup failure.
	defer r.removeContainer(created.ID)

	if err := r.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return nil, fmt.Errorf("start container %q for task %q: %w", created.ID, task.ID, err)
	}

	exitCode, waitErr := r.waitContainer(ctx, created.ID)

	logs, logErr := r.readAndPrintLogs(created.ID)
	if logErr != nil && waitErr == nil {
		return &Result{ExitCode: exitCode, Logs: logs}, fmt.Errorf("read logs for task %q: %w", task.ID, logErr)
	}
	if waitErr != nil {
		return &Result{ExitCode: exitCode, Logs: logs}, fmt.Errorf("wait for task %q container: %w", task.ID, waitErr)
	}

	return &Result{
		ExitCode: exitCode,
		Logs:     logs,
	}, nil
}

func (r *DockerRunner) ensureImage(ctx context.Context, imageName string) error {
	_, err := r.cli.ImageInspect(ctx, imageName)
	if err == nil {
		return nil
	}
	if !client.IsErrNotFound(err) {
		return fmt.Errorf("inspect image %q: %w", imageName, err)
	}

	fmt.Printf("[runner] Image %s not found locally, pulling...\n", imageName)
	pullStream, err := r.cli.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull image %q: %w", imageName, err)
	}
	defer pullStream.Close()

	fd := os.Stdout.Fd()
	isTerm := isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
	if err := jsonmessage.DisplayJSONMessagesStream(pullStream, os.Stdout, fd, isTerm, nil); err != nil {
		return fmt.Errorf("stream pull progress for %q: %w", imageName, err)
	}

	return nil
}

func (r *DockerRunner) waitContainer(ctx context.Context, containerID string) (int64, error) {
	statusCh, errCh := r.cli.ContainerWait(ctx, containerID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return -1, err
		}
		return 0, nil
	case status := <-statusCh:
		if status.Error != nil {
			return status.StatusCode, fmt.Errorf("container exit error: %s", status.Error.Message)
		}
		return status.StatusCode, nil
	case <-ctx.Done():
		return -1, ctx.Err()
	}
}

func (r *DockerRunner) readAndPrintLogs(containerID string) (string, error) {
	logCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	reader, err := r.cli.ContainerLogs(logCtx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
	})
	if err != nil {
		return "", err
	}
	defer reader.Close()

	var combined bytes.Buffer
	stdoutWriter := io.MultiWriter(os.Stdout, &combined)
	stderrWriter := io.MultiWriter(os.Stderr, &combined)

	if _, err := stdcopy.StdCopy(stdoutWriter, stderrWriter, reader); err != nil {
		return combined.String(), err
	}
	return combined.String(), nil
}

func (r *DockerRunner) removeContainer(containerID string) {
	rmCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := r.cli.ContainerRemove(rmCtx, containerID, container.RemoveOptions{Force: true}); err != nil {
		fmt.Fprintf(os.Stderr, "[runner] Warning: failed to remove container %s: %v\n", containerID, err)
	}
}

func formatEnv(env map[string]string) []string {
	if len(env) == 0 {
		return nil
	}
	keys := make([]string, 0, len(env))
	for k := range env {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	formatted := make([]string, 0, len(keys))
	for _, k := range keys {
		formatted = append(formatted, fmt.Sprintf("%s=%s", k, env[k]))
	}
	return formatted
}
