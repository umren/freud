package runner

import (
	"strings"
	"testing"

	"freud/internal/workflow"
)

func TestResolveRuntimeSpec(t *testing.T) {
	t.Run("python default image", func(t *testing.T) {
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

	t.Run("python custom image", func(t *testing.T) {
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
		if spec.Extension != ".py" {
			t.Fatalf("expected .py extension, got %q", spec.Extension)
		}
		cmd := spec.Command("/workspace/.scripts/t1.py")
		if len(cmd) != 5 || cmd[0] != "sh" || cmd[1] != "-c" || !strings.Contains(cmd[2], "python3") {
			t.Fatalf("unexpected command: %v", cmd)
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
		if spec.Extension != ".ts" {
			t.Fatalf("expected .ts extension, got %q", spec.Extension)
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
		if spec.Extension != ".sh" {
			t.Fatalf("expected .sh extension, got %q", spec.Extension)
		}
		cmd := spec.Command("/workspace/.scripts/t3.sh")
		if len(cmd) != 2 || cmd[0] != "sh" || cmd[1] != "/workspace/.scripts/t3.sh" {
			t.Fatalf("unexpected command: %v", cmd)
		}
	})
}
