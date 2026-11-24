package runner

import (
	"archive/tar"
	"io"
	"os"
	"path/filepath"
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

func TestResolveIncludeMounts(t *testing.T) {
	baseDir := t.TempDir()
	includesDir := filepath.Join(baseDir, "includes")
	configsDir := filepath.Join(baseDir, "configs")
	if err := os.MkdirAll(includesDir, 0o755); err != nil {
		t.Fatalf("mkdir includes: %v", err)
	}
	if err := os.MkdirAll(configsDir, 0o755); err != nil {
		t.Fatalf("mkdir configs: %v", err)
	}
	helperFile := filepath.Join(includesDir, "helpers.py")
	cfgFile := filepath.Join(configsDir, "app.json")
	if err := os.WriteFile(helperFile, []byte("x = 1\n"), 0o644); err != nil {
		t.Fatalf("write helper: %v", err)
	}
	if err := os.WriteFile(cfgFile, []byte("{}"), 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	t.Run("auto-mounts includes directory when include is empty", func(t *testing.T) {
		mounts, err := resolveIncludeMounts(workflow.Task{
			ID:      "t1",
			BaseDir: baseDir,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(mounts) != 1 || mounts[0].Target != "/includes" || !mounts[0].ReadOnly {
			t.Fatalf("unexpected mounts: %+v", mounts)
		}
	})

	t.Run("explicit includes map cleanly and stay read-only", func(t *testing.T) {
		mounts, err := resolveIncludeMounts(workflow.Task{
			ID:      "t2",
			BaseDir: baseDir,
			Include: []string{"includes/helpers.py", "configs/app.json"},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(mounts) != 2 {
			t.Fatalf("expected 2 mounts, got %d: %+v", len(mounts), mounts)
		}
		if mounts[0].Target != "/includes/configs/app.json" && mounts[1].Target != "/includes/configs/app.json" {
			t.Fatalf("expected /includes/configs/app.json mount, got %+v", mounts)
		}
		if mounts[0].Target != "/includes/helpers.py" && mounts[1].Target != "/includes/helpers.py" {
			t.Fatalf("expected /includes/helpers.py mount, got %+v", mounts)
		}
		for _, m := range mounts {
			if !m.ReadOnly {
				t.Fatalf("expected ReadOnly mount, got %+v", m)
			}
		}
	})

	t.Run("parent dir mount suppresses overlapping child mount", func(t *testing.T) {
		mounts, err := resolveIncludeMounts(workflow.Task{
			ID:      "t3",
			BaseDir: baseDir,
			Include: []string{"includes/helpers.py", "includes"},
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if len(mounts) != 1 || mounts[0].Target != "/includes" {
			t.Fatalf("expected single /includes mount, got %+v", mounts)
		}
	})

	t.Run("missing include returns error", func(t *testing.T) {
		_, err := resolveIncludeMounts(workflow.Task{
			ID:      "t4",
			BaseDir: baseDir,
			Include: []string{"includes/missing.py"},
		})
		if err == nil || !strings.Contains(err.Error(), "does not exist") {
			t.Fatalf("expected does not exist error, got %v", err)
		}
	})

	t.Run("configured custom includesDir resolves default and explicit includes", func(t *testing.T) {
		customInc := filepath.Join(baseDir, "custom_shared")
		if err := os.MkdirAll(customInc, 0o755); err != nil {
			t.Fatalf("mkdir custom_shared: %v", err)
		}
		customFile := filepath.Join(customInc, "shared.py")
		if err := os.WriteFile(customFile, []byte("ok = True\n"), 0o644); err != nil {
			t.Fatalf("write shared.py: %v", err)
		}

		autoMounts, err := resolveIncludeMountsWithDir(workflow.Task{ID: "t5", BaseDir: baseDir}, customInc)
		if err != nil || len(autoMounts) != 1 || autoMounts[0].Source != customInc || autoMounts[0].Target != "/includes" {
			t.Fatalf("expected auto-mount of custom_shared, got %+v, err=%v", autoMounts, err)
		}

		explicitMounts, err := resolveIncludeMountsWithDir(workflow.Task{
			ID:      "t6",
			BaseDir: baseDir,
			Include: []string{"includes/shared.py"},
		}, customInc)
		if err != nil || len(explicitMounts) != 1 || explicitMounts[0].Source != customFile || explicitMounts[0].Target != "/includes/shared.py" {
			t.Fatalf("expected explicit mount from custom_shared, got %+v, err=%v", explicitMounts, err)
		}
	})
}

func TestBuildTaskEnvPythonPath(t *testing.T) {
	pyEnv := buildTaskEnv(workflow.Task{
		Runtime: workflow.RuntimePython,
		Env:     map[string]string{"FOO": "bar"},
	})
	if pyEnv["PYTHONPATH"] != "/workspace:/includes" {
		t.Fatalf("expected PYTHONPATH=/workspace:/includes, got %q", pyEnv["PYTHONPATH"])
	}

	customPyEnv := buildTaskEnv(workflow.Task{
		Runtime: workflow.RuntimePython,
		Env:     map[string]string{"PYTHONPATH": "/custom/lib"},
	})
	if customPyEnv["PYTHONPATH"] != "/custom/lib" {
		t.Fatalf("expected user PYTHONPATH preserved, got %q", customPyEnv["PYTHONPATH"])
	}

	jsEnv := buildTaskEnv(workflow.Task{
		Runtime: workflow.RuntimeJS,
	})
	if _, exists := jsEnv["PYTHONPATH"]; exists {
		t.Fatalf("did not expect PYTHONPATH for JS task")
	}
}
