package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/docker/docker/api/types/build"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/jsonmessage"
	"github.com/mattn/go-isatty"

	"freud/internal/workflow"
)

const (
	cacheRepo           = "orchestrator-cache"
	cacheLabelKey       = "orchestrator.cache"
	cacheBaseImageLabel = "orchestrator.base_image"
	cacheRuntimeLabel   = "orchestrator.runtime"
	cacheDepsLabel      = "orchestrator.dependencies"

	pythonVenvPython = "/opt/orch-venv/bin/python"
	jsDepsDir        = "/opt/orch-deps"
)

// CachedImage describes a locally cached dependency layer image managed by the orchestrator.
type CachedImage struct {
	ID        string
	Tag       string
	BaseImage string
	Runtime   string
	Deps      string
	Size      int64
	CreatedAt time.Time
}

func computeCacheTag(baseImageID, runtime string, deps []string) string {
	sorted := make([]string, len(deps))
	copy(sorted, deps)
	sort.Strings(sorted)

	h := sha256.New()
	h.Write([]byte(baseImageID))
	h.Write([]byte{0})
	h.Write([]byte(runtime))
	h.Write([]byte{0})
	for _, dep := range sorted {
		h.Write([]byte(dep))
		h.Write([]byte{0})
	}
	sum := hex.EncodeToString(h.Sum(nil))
	return cacheRepo + ":" + sum[:16]
}

func buildCacheDockerfile(baseImage, runtime string, deps []string) (string, error) {
	if len(deps) == 0 {
		return "", fmt.Errorf("cannot build dependency cache image with empty dependencies")
	}

	quotedDeps := make([]string, len(deps))
	for i, dep := range deps {
		quotedDeps[i] = shellQuote(dep)
	}
	depArgs := strings.Join(quotedDeps, " ")

	switch runtime {
	case workflow.RuntimePython:
		return fmt.Sprintf(`FROM %s
RUN if command -v uv >/dev/null 2>&1; then \
        uv venv /opt/orch-venv && uv pip install --python %s %s; \
    elif command -v python3 >/dev/null 2>&1; then \
        python3 -m venv /opt/orch-venv && PIP_ROOT_USER_ACTION=ignore /opt/orch-venv/bin/pip install --no-cache-dir --disable-pip-version-check %s; \
    else \
        python -m venv /opt/orch-venv && PIP_ROOT_USER_ACTION=ignore /opt/orch-venv/bin/pip install --no-cache-dir --disable-pip-version-check %s; \
    fi
`, baseImage, pythonVenvPython, depArgs, depArgs, depArgs), nil

	case workflow.RuntimeJS:
		return fmt.Sprintf(`FROM %s
ENV NODE_PATH=%s/node_modules
RUN mkdir -p %s && cd %s && \
    if command -v bun >/dev/null 2>&1; then \
        bun add %s; \
    elif command -v npm >/dev/null 2>&1; then \
        npm install --no-save %s; \
    else \
        echo "No supported JS package manager (bun or npm) found in image" >&2; exit 1; \
    fi
`, baseImage, jsDepsDir, jsDepsDir, jsDepsDir, depArgs, depArgs), nil

	default:
		return "", fmt.Errorf("dependency caching is not supported for runtime %q", runtime)
	}
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func createDockerfileTarContext(dockerfile string) (io.Reader, error) {
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	data := []byte(dockerfile)
	hdr := &tar.Header{
		Name: "Dockerfile",
		Mode: 0o644,
		Size: int64(len(data)),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, fmt.Errorf("write tar header: %w", err)
	}
	if _, err := tw.Write(data); err != nil {
		return nil, fmt.Errorf("write tar body: %w", err)
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("close tar writer: %w", err)
	}
	return &buf, nil
}

func (r *DockerRunner) ensureDependencyImage(ctx context.Context, baseImage, baseImageID string, task workflow.Task) (string, error) {
	cacheTag := computeCacheTag(baseImageID, task.Runtime, task.Dependencies)

	if !r.noCache {
		if _, err := r.cli.ImageInspect(ctx, cacheTag); err == nil {
			fmt.Printf("[runner] Using cached dependency image %s\n", cacheTag)
			return cacheTag, nil
		} else if !client.IsErrNotFound(err) {
			return "", fmt.Errorf("inspect cached image %q: %w", cacheTag, err)
		}
	} else {
		// Remove stale tag before rebuilding so old cache layers do not become dangling.
		if _, err := r.cli.ImageRemove(ctx, cacheTag, image.RemoveOptions{PruneChildren: true}); err != nil && !client.IsErrNotFound(err) {
			return "", fmt.Errorf("remove existing cached image %q: %w", cacheTag, err)
		}
	}

	dockerfile, err := buildCacheDockerfile(baseImage, task.Runtime, task.Dependencies)
	if err != nil {
		return "", err
	}

	tarCtx, err := createDockerfileTarContext(dockerfile)
	if err != nil {
		return "", fmt.Errorf("create build context for %q: %w", cacheTag, err)
	}

	sortedDeps := make([]string, len(task.Dependencies))
	copy(sortedDeps, task.Dependencies)
	sort.Strings(sortedDeps)

	fmt.Printf("[runner] Building cached dependency image %s (from %s)...\n", cacheTag, baseImage)
	buildResp, err := r.cli.ImageBuild(ctx, tarCtx, build.ImageBuildOptions{
		Tags:        []string{cacheTag},
		Dockerfile:  "Dockerfile",
		Remove:      true,
		ForceRemove: true,
		NoCache:     r.noCache,
		Labels: map[string]string{
			cacheLabelKey:       "true",
			cacheBaseImageLabel: baseImage,
			cacheRuntimeLabel:   task.Runtime,
			cacheDepsLabel:      strings.Join(sortedDeps, ","),
		},
	})
	if err != nil {
		return "", fmt.Errorf("build cached image %q: %w", cacheTag, err)
	}
	defer buildResp.Body.Close()

	fd := os.Stdout.Fd()
	isTerm := isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
	if err := jsonmessage.DisplayJSONMessagesStream(buildResp.Body, os.Stdout, fd, isTerm, nil); err != nil {
		return "", fmt.Errorf("build cached image %q: %w", cacheTag, err)
	}

	return cacheTag, nil
}

// ListCachedImages returns all locally stored dependency cache images created by the orchestrator.
func (r *DockerRunner) ListCachedImages(ctx context.Context) ([]CachedImage, error) {
	summaries, err := r.cli.ImageList(ctx, image.ListOptions{
		Filters: filters.NewArgs(filters.Arg("label", cacheLabelKey+"=true")),
	})
	if err != nil {
		return nil, fmt.Errorf("list cached images: %w", err)
	}

	result := make([]CachedImage, 0, len(summaries))
	for _, s := range summaries {
		tag := "<untagged>"
		if len(s.RepoTags) > 0 {
			tag = s.RepoTags[0]
		}
		result = append(result, CachedImage{
			ID:        s.ID,
			Tag:       tag,
			BaseImage: s.Labels[cacheBaseImageLabel],
			Runtime:   s.Labels[cacheRuntimeLabel],
			Deps:      s.Labels[cacheDepsLabel],
			Size:      s.Size,
			CreatedAt: time.Unix(s.Created, 0).UTC(),
		})
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})
	return result, nil
}

// ClearCachedImages removes all orchestrator dependency cache images and returns count and reclaimed bytes.
func (r *DockerRunner) ClearCachedImages(ctx context.Context) (int, int64, error) {
	cached, err := r.ListCachedImages(ctx)
	if err != nil {
		return 0, 0, err
	}

	var removed int
	var reclaimed int64
	for _, img := range cached {
		if _, err := r.cli.ImageRemove(ctx, img.ID, image.RemoveOptions{
			Force:         true,
			PruneChildren: true,
		}); err != nil {
			return removed, reclaimed, fmt.Errorf("remove cached image %s (%s): %w", img.Tag, img.ID, err)
		}
		removed++
		reclaimed += img.Size
	}
	return removed, reclaimed, nil
}
