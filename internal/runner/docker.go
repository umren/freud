package runner

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
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
	containerIncludesDir  = "/includes"
	scriptsSubdir         = ".scripts"
	defaultIncludesSubdir = "includes"
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
	imageMu sync.Mutex
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

type liveBuffer struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	onLog func(string)
}

func (lb *liveBuffer) Write(p []byte) (int, error) {
	lb.mu.Lock()
	n, err := lb.buf.Write(p)
	snapshot := lb.buf.String()
	cb := lb.onLog
	lb.mu.Unlock()

	if cb != nil {
		cb(snapshot)
	}
	return n, err
}

func (lb *liveBuffer) String() string {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	return lb.buf.String()
}

// RunTask writes the task script to the shared workspace, ensures the runtime/cache image exists,
// executes the container, captures logs, and removes the container afterward.
func (r *DockerRunner) RunTask(ctx context.Context, runID, hostWorkspace string, task workflow.Task) (*Result, error) {
	return r.RunTaskStream(ctx, runID, hostWorkspace, task, nil)
}

// RunTaskStream executes a task container and invokes onLog with accumulated output in real time.
func (r *DockerRunner) RunTaskStream(ctx context.Context, runID, hostWorkspace string, task workflow.Task, onLog func(string)) (*Result, error) {
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

	liveBuf := &liveBuffer{onLog: onLog}

	runImage, err := r.prepareRunImage(ctx, spec.Image, task, liveBuf)
	if err != nil {
		return &Result{ExitCode: -1, Logs: liveBuf.String()}, err
	}

	includeMounts, err := resolveIncludeMounts(task)
	if err != nil {
		return &Result{ExitCode: -1, Logs: liveBuf.String()}, err
	}

	mounts := make([]mount.Mount, 0, 1+len(includeMounts))
	mounts = append(mounts, mount.Mount{
		Type:   mount.TypeBind,
		Source: absWorkspace,
		Target: containerWorkspaceDir,
	})
	mounts = append(mounts, includeMounts...)

	containerScriptPath := containerWorkspaceDir + "/" + scriptsSubdir + "/" + scriptFilename
	cfg := &container.Config{
		Image:      runImage,
		Cmd:        spec.Command(containerScriptPath),
		Env:        formatEnv(buildTaskEnv(task)),
		WorkingDir: containerWorkspaceDir,
		Tty:        false,
	}
	hostCfg := &container.HostConfig{
		AutoRemove: false,
		Mounts:     mounts,
	}

	containerName := fmt.Sprintf("orch-%s-%s", runID, task.ID)
	created, err := r.cli.ContainerCreate(ctx, cfg, hostCfg, nil, nil, containerName)
	if err != nil {
		return &Result{ExitCode: -1, Logs: liveBuf.String()}, fmt.Errorf("create container for task %q: %w", task.ID, err)
	}

	// Ensure the container is removed only after logs have been collected or on setup failure.
	defer r.removeContainer(created.ID)

	if err := r.cli.ContainerStart(ctx, created.ID, container.StartOptions{}); err != nil {
		return &Result{ExitCode: -1, Logs: liveBuf.String()}, fmt.Errorf("start container %q for task %q: %w", created.ID, task.ID, err)
	}

	logErrCh := r.streamContainerLogs(ctx, created.ID, liveBuf)

	exitCode, waitErr := r.waitContainer(ctx, created.ID)
	logErr := <-logErrCh
	logs := liveBuf.String()

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

func (r *DockerRunner) prepareRunImage(ctx context.Context, baseImage string, task workflow.Task, logOut io.Writer) (string, error) {
	r.imageMu.Lock()
	defer r.imageMu.Unlock()

	baseInfo, err := r.ensureImage(ctx, baseImage, logOut)
	if err != nil {
		return "", err
	}
	if len(task.Dependencies) == 0 {
		return baseImage, nil
	}
	return r.ensureDependencyImage(ctx, baseImage, baseInfo.ID, task, logOut)
}

func (r *DockerRunner) ensureImage(ctx context.Context, imageName string, logOut io.Writer) (image.InspectResponse, error) {
	info, err := r.cli.ImageInspect(ctx, imageName)
	if err == nil {
		return info, nil
	}
	if !client.IsErrNotFound(err) {
		return image.InspectResponse{}, fmt.Errorf("inspect image %q: %w", imageName, err)
	}

	msg := fmt.Sprintf("[runner] Image %s not found locally, pulling...\n", imageName)
	fmt.Print(msg)
	if logOut != nil {
		_, _ = io.WriteString(logOut, msg)
	}
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

func (r *DockerRunner) streamContainerLogs(ctx context.Context, containerID string, liveBuf *liveBuffer) <-chan error {
	errCh := make(chan error, 1)

	reader, err := r.cli.ContainerLogs(ctx, containerID, container.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})
	if err != nil {
		errCh <- err
		return errCh
	}

	stdoutWriter := io.MultiWriter(os.Stdout, liveBuf)
	stderrWriter := io.MultiWriter(os.Stderr, liveBuf)

	go func() {
		defer reader.Close()
		_, copyErr := stdcopy.StdCopy(stdoutWriter, stderrWriter, reader)
		errCh <- copyErr
	}()

	return errCh
}

func (r *DockerRunner) removeContainer(containerID string) {
	rmCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := r.cli.ContainerRemove(rmCtx, containerID, container.RemoveOptions{Force: true}); err != nil {
		fmt.Fprintf(os.Stderr, "[runner] Warning: failed to remove container %s: %v\n", containerID, err)
	}
}

func buildTaskEnv(task workflow.Task) map[string]string {
	merged := make(map[string]string, len(task.Env)+1)
	for k, v := range task.Env {
		merged[k] = v
	}
	if task.Runtime == workflow.RuntimePython {
		if _, exists := merged["PYTHONPATH"]; !exists {
			merged["PYTHONPATH"] = containerWorkspaceDir + ":" + containerIncludesDir
		}
	}
	return merged
}

func resolveIncludeMounts(task workflow.Task) ([]mount.Mount, error) {
	baseDir := task.BaseDir
	if baseDir == "" {
		baseDir = "."
	}
	absBase, err := filepath.Abs(baseDir)
	if err != nil {
		return nil, fmt.Errorf("resolve workflow base directory %q: %w", baseDir, err)
	}
	roots := includeSearchRoots(absBase)

	if len(task.Include) == 0 {
		for _, root := range roots {
			defaultDir := filepath.Join(root, defaultIncludesSubdir)
			info, err := os.Stat(defaultDir)
			if err != nil {
				if os.IsNotExist(err) {
					continue
				}
				return nil, fmt.Errorf("inspect default includes directory %q: %w", defaultDir, err)
			}
			if !info.IsDir() {
				continue
			}
			return []mount.Mount{
				{
					Type:     mount.TypeBind,
					Source:   defaultDir,
					Target:   containerIncludesDir,
					ReadOnly: true,
				},
			}, nil
		}
		return nil, nil
	}

	type candidate struct {
		source string
		target string
		isDir  bool
	}

	candidates := make([]candidate, 0, len(task.Include))
	for _, rel := range task.Include {
		cleanRel := filepath.ToSlash(filepath.Clean(strings.TrimSpace(rel)))
		if filepath.IsAbs(cleanRel) || cleanRel == "." || cleanRel == ".." || strings.HasPrefix(cleanRel, "../") {
			return nil, fmt.Errorf("invalid include path %q for task %q", rel, task.ID)
		}

		absSource, info, err := locateIncludeSource(roots, cleanRel, rel, task.ID)
		if err != nil {
			return nil, err
		}

		target := containerIncludesDir + "/" + cleanRel
		if cleanRel == defaultIncludesSubdir {
			target = containerIncludesDir
		} else if strings.HasPrefix(cleanRel, defaultIncludesSubdir+"/") {
			target = containerIncludesDir + "/" + strings.TrimPrefix(cleanRel, defaultIncludesSubdir+"/")
		}

		candidates = append(candidates, candidate{
			source: absSource,
			target: path.Clean(target),
			isDir:  info.IsDir(),
		})
	}

	// Sort shorter parent paths first so directory mounts suppress overlapping child mounts.
	sort.SliceStable(candidates, func(i, j int) bool {
		return len(candidates[i].target) < len(candidates[j].target)
	})

	mounts := make([]mount.Mount, 0, len(candidates))
	seenTargets := make(map[string]struct{}, len(candidates))
	mountedDirs := make(map[string]struct{}, len(candidates))

	for _, c := range candidates {
		if _, dup := seenTargets[c.target]; dup {
			continue
		}
		if isCoveredByMountedDir(c.target, mountedDirs) {
			continue
		}
		seenTargets[c.target] = struct{}{}
		if c.isDir {
			mountedDirs[c.target] = struct{}{}
		}
		mounts = append(mounts, mount.Mount{
			Type:     mount.TypeBind,
			Source:   c.source,
			Target:   c.target,
			ReadOnly: true,
		})
	}

	return mounts, nil
}

func includeSearchRoots(absBase string) []string {
	roots := []string{absBase}
	if filepath.Base(absBase) == "flows" {
		roots = append(roots, filepath.Dir(absBase))
	}
	if wd, err := os.Getwd(); err == nil {
		if absWD, err := filepath.Abs(wd); err == nil {
			already := false
			for _, r := range roots {
				if r == absWD {
					already = true
					break
				}
			}
			if !already {
				roots = append(roots, absWD)
			}
		}
	}
	return roots
}

func locateIncludeSource(roots []string, cleanRel, rawRel, taskID string) (string, os.FileInfo, error) {
	for _, root := range roots {
		absSource := filepath.Join(root, filepath.FromSlash(cleanRel))
		relCheck, err := filepath.Rel(root, absSource)
		if err != nil || relCheck == ".." || strings.HasPrefix(relCheck, ".."+string(filepath.Separator)) {
			continue
		}
		info, err := os.Stat(absSource)
		if err == nil {
			return absSource, info, nil
		}
		if !os.IsNotExist(err) {
			return "", nil, fmt.Errorf("stat include path %q: %w", rawRel, err)
		}
	}
	return "", nil, fmt.Errorf("include path %q for task %q does not exist", rawRel, taskID)
}

func isCoveredByMountedDir(target string, mountedDirs map[string]struct{}) bool {
	if len(mountedDirs) == 0 {
		return false
	}
	curr := path.Dir(target)
	for curr != "/" && curr != "." {
		if _, ok := mountedDirs[curr]; ok {
			return true
		}
		curr = path.Dir(curr)
	}
	return false
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
