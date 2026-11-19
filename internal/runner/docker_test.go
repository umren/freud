package runner

import (
	"archive/tar"
	"io"
	"strings"
	"testing"

	"freud/internal/workflow"
)

func TestResolveRuntimeSpec(t *testing.T) {
	t.Run("python default image without deps", func(t *testing.T) {
		spec, err := resolveRuntimeSpec(workflow.Task{
			ID:      "t1",
			Runtime: workflow.RuntimePython,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if spec.Image != defaultPythonImage {
			t.Fatalf("expected %q, got %q", defaultPythonImage, spec.Image)
		}
		cmd := spec.Command("/workspace/.scripts/t1.py")
		if len(cmd) != 3 || cmd[0] != "uv" || cmd[1] != "run" {
			t.Fatalf("unexpected command: %v", cmd)
		}
	})

	t.Run("python custom image without deps", func(t *testing.T) {
		spec, err := resolveRuntimeSpec(workflow.Task{
			ID:      "t1",
			Runtime: workflow.RuntimePython,
			Image:   "python:3.13-slim",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if spec.Image != "python:3.13-slim" {
			t.Fatalf("expected python:3.13-slim, got %q", spec.Image)
		}
		cmd := spec.Command("/workspace/.scripts/t1.py")
		if len(cmd) != 5 || cmd[0] != "sh" || cmd[1] != "-c" || !strings.Contains(cmd[2], "python3") {
			t.Fatalf("unexpected command: %v", cmd)
		}
	})

	t.Run("python with deps uses cached venv python", func(t *testing.T) {
		for _, img := range []string{"", "python:3.13-slim"} {
			spec, err := resolveRuntimeSpec(workflow.Task{
				ID:           "t4",
				Runtime:      workflow.RuntimePython,
				Image:        img,
				Dependencies: []string{"pydantic", "pyyaml"},
			})
			if err != nil {
				t.Fatalf("unexpected error for image %q: %v", img, err)
			}
			cmd := spec.Command("/workspace/.scripts/t4.py")
			if len(cmd) != 2 || cmd[0] != pythonVenvPython || cmd[1] != "/workspace/.scripts/t4.py" {
				t.Fatalf("unexpected command for image %q: %v", img, cmd)
			}
		}
	})

	t.Run("js custom image", func(t *testing.T) {
		spec, err := resolveRuntimeSpec(workflow.Task{
			ID:      "t2",
			Runtime: workflow.RuntimeJS,
			Image:   "node:22-alpine",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if spec.Image != "node:22-alpine" {
			t.Fatalf("expected node:22-alpine, got %q", spec.Image)
		}
		cmd := spec.Command("/workspace/.scripts/t2.ts")
		if len(cmd) != 5 || cmd[0] != "sh" || !strings.Contains(cmd[2], "node") {
			t.Fatalf("unexpected command: %v", cmd)
		}
	})

	t.Run("arbitrary image without runtime", func(t *testing.T) {
		spec, err := resolveRuntimeSpec(workflow.Task{
			ID:    "t3",
			Image: "alpine:3.20",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if spec.Image != "alpine:3.20" {
			t.Fatalf("expected alpine:3.20, got %q", spec.Image)
		}
		cmd := spec.Command("/workspace/.scripts/t3.sh")
		if len(cmd) != 2 || cmd[0] != "sh" || cmd[1] != "/workspace/.scripts/t3.sh" {
			t.Fatalf("unexpected command: %v", cmd)
		}
	})
}

func TestComputeCacheTagOrderIndependentAndBaseSensitive(t *testing.T) {
	tagA := computeCacheTag("sha256:111", workflow.RuntimePython, []string{"pyyaml", "pydantic>=2.0"})
	tagB := computeCacheTag("sha256:111", workflow.RuntimePython, []string{"pydantic>=2.0", "pyyaml"})
	tagDiffBase := computeCacheTag("sha256:222", workflow.RuntimePython, []string{"pyyaml", "pydantic>=2.0"})

	if !strings.HasPrefix(tagA, cacheRepo+":") {
		t.Fatalf("unexpected cache tag prefix: %q", tagA)
	}
	if tagA != tagB {
		t.Fatalf("expected order-independent cache tag, got %q vs %q", tagA, tagB)
	}
	if tagA == tagDiffBase {
		t.Fatalf("expected different base image IDs to produce different tags")
	}
}

func TestBuildCacheDockerfileAndTarContext(t *testing.T) {
	pyDF, err := buildCacheDockerfile("python:3.13-slim", workflow.RuntimePython, []string{"pydantic>=2.0,<3.0", "pyyaml"})
	if err != nil {
		t.Fatalf("buildCacheDockerfile python failed: %v", err)
	}
	if !strings.Contains(pyDF, "FROM python:3.13-slim") || !strings.Contains(pyDF, "'pydantic>=2.0,<3.0' 'pyyaml'") {
		t.Fatalf("unexpected python Dockerfile:\n%s", pyDF)
	}

	jsDF, err := buildCacheDockerfile("oven/bun:latest", workflow.RuntimeJS, []string{"lodash-es", "ms"})
	if err != nil {
		t.Fatalf("buildCacheDockerfile js failed: %v", err)
	}
	if !strings.Contains(jsDF, "ENV NODE_PATH=/opt/orch-deps/node_modules") || !strings.Contains(jsDF, "'lodash-es' 'ms'") {
		t.Fatalf("unexpected js Dockerfile:\n%s", jsDF)
	}

	tarReader, err := createDockerfileTarContext(pyDF)
	if err != nil {
		t.Fatalf("createDockerfileTarContext failed: %v", err)
	}
	tr := tar.NewReader(tarReader)
	hdr, err := tr.Next()
	if err != nil {
		t.Fatalf("read tar header: %v", err)
	}
	if hdr.Name != "Dockerfile" {
		t.Fatalf("expected Dockerfile in tar, got %q", hdr.Name)
	}
	content, err := io.ReadAll(tr)
	if err != nil {
		t.Fatalf("read tar body: %v", err)
	}
	if string(content) != pyDF {
		t.Fatalf("tar Dockerfile content mismatch")
	}
}
