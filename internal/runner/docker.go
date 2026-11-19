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

// RuntimeSpec defines the base image, script extension, and container command for a task execution.
type RuntimeSpec struct {
	Image     string
	Extension string
	Command   func(containerScriptPath string) []string
}

func resolveRuntimeSpec(task workflow.Task) (RuntimeSpec, error) {
	hasDeps := len(task.Dependencies) > 0

	switch task.Runtime {
	case workflow.RuntimePython:
		img := task.Image
		if img == "" {
			img = defaultPythonImage
		}
		return RuntimeSpec{
			Image:     img,
			Extension: ".py",
			Command: func(scriptPath string) []string {
				if hasDeps {
					return []string{pythonVenvPython, scriptPath}
				}
				if task.Image == "" {
					return []string{"uv", "run", scriptPath}
				}
				return []string{
					"sh", "-c",
					`if command -v uv >/dev/null 2>&1; then exec uv run "$1"; elif command -v python3 >/dev/null 2>&1; then exec python3 "$1"; else exec python "$1"; fi`,
					"sh", scriptPath,
				}
			},
		}, nil

	case workflow.RuntimeJS:
		img := task.Image
		if img == "" {
			img = defaultJSImage
		}
		return RuntimeSpec{
			Image:     img,
			Extension: ".ts",
			Command: func(scriptPath string) []string {
				if task.Image == "" {
					return []string{"bun", "run", scriptPath}
				}
				return []string{
					"sh", "-c",
					`if command -v bun >/dev/null 2>&1; then exec bun run "$1"; elif command -v deno >/dev/null 2>&1; then exec deno run --allow-all "$1"; else exec node "$1"; fi`,
					"sh", scriptPath,
				}
			},
		}, nil

	case workflow.RuntimeShell, "":
		if task.Image == "" {
			return RuntimeSpec{}, fmt.Errorf("task %q with runtime %q requires an image", task.ID, task.Runtime)
		}
		if hasDeps {
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
	cli     *client.Client
	noCache bool
}

// NewDockerRunner initializes a Docker SDK client using environment settings and API negotiation.
func NewDockerRunner() (*DockerRunner, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}
	return &DockerRunner{cli: cli}, nil
}

// SetNoCache configures whether dependency image caching should be bypassed and rebuilt.
func (r *DockerRunner) SetNoCache(noCache bool) {
	r.noCache = noCache
}

// Close releases resources held by the Docker client.
func (r *DockerRunner) Close() error {
	if r.cli == nil {
		return nil
	}
	return r.cli.Close()
}

// RunTask writes the task script to the shared workspace, ensures the runtime/cache image exists,
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

	baseInfo, err := r.ensureImage(ctx, spec.Image)
	if err != nil {
		return nil, err
	}

	runImage := spec.Image
	if len(task.Dependencies) > 0 {
		runImage, err = r.ensureDependencyImage(ctx, spec.Image, baseInfo.ID, task)
		if err != nil {
			return nil, err
		}
	}

	containerScriptPath := containerWorkspaceDir + "/" + scriptsSubdir + "/" + scriptFilename
	cfg := &container.Config{
		Image:      runImage,
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

func (r *DockerRunner) ensureImage(ctx context.Context, imageName string) (image.InspectResponse, error) {
	info, err := r.cli.ImageInspect(ctx, imageName)
	if err == nil {
		return info, nil
	}
	if !client.IsErrNotFound(err) {
		return image.InspectResponse{}, fmt.Errorf("inspect image %q: %w", imageName, err)
	}

	fmt.Printf("[runner] Image %s not found locally, pulling...\n", imageName)
	pullStream, err := r.cli.ImagePull(ctx, imageName, image.PullOptions{})
	if err != nil {
		return image.InspectResponse{}, fmt.Errorf("pull image %q: %w", imageName, err)
	}
	defer pullStream.Close()

	fd := os.Stdout.Fd()
	isTerm := isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
	if err := jsonmessage.DisplayJSONMessagesStream(pullStream, os.Stdout, fd, isTerm, nil); err != nil {
		return image.InspectResponse{}, fmt.Errorf("stream pull progress for %q: %w", imageName, err)
	}

	info, err = r.cli.ImageInspect(ctx, imageName)
	if err != nil {
		return image.InspectResponse{}, fmt.Errorf("inspect pulled image %q: %w", imageName, err)
	}
	return info, nil
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
