package workflow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseAndTopologicalOrder(t *testing.T) {
	yamlData := `
name: test-pipeline
tasks:
  - id: second
    runtime: js
    depends_on: [first]
    script: console.log("second")
  - id: first
    runtime: python
    script: print("first")
  - id: third
    runtime: python
    depends_on: [first, second]
    script: print("third")
`
	wf, err := Parse([]byte(yamlData))
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}

	ordered, err := wf.TopologicalOrder()
	if err != nil {
		t.Fatalf("unexpected topological order error: %v", err)
	}

	if len(ordered) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(ordered))
	}
	if ordered[0].ID != "first" || ordered[1].ID != "second" || ordered[2].ID != "third" {
		t.Fatalf("unexpected order: %s, %s, %s", ordered[0].ID, ordered[1].ID, ordered[2].ID)
	}
}

func TestParseValidationErrors(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "empty name",
			yaml:    "name: \"\"\ntasks:\n  - id: a\n    runtime: python\n    script: print(1)\n",
			wantErr: "workflow name",
		},
		{
			name:    "no tasks",
			yaml:    "name: empty\ntasks: []\n",
			wantErr: "at least one task",
		},
		{
			name:    "duplicate task id",
			yaml:    "name: dup\ntasks:\n  - id: a\n    runtime: python\n    script: print(1)\n  - id: a\n    runtime: js\n    script: console.log(1)\n",
			wantErr: "duplicate task id",
		},
		{
			name:    "unsupported runtime",
			yaml:    "name: bad-rt\ntasks:\n  - id: a\n    runtime: ruby\n    script: puts 1\n",
			wantErr: "unsupported runtime",
		},
		{
			name:    "unknown dependency",
			yaml:    "name: missing-dep\ntasks:\n  - id: a\n    runtime: python\n    depends_on: [missing]\n    script: print(1)\n",
			wantErr: "unknown task",
		},
		{
			name:    "self dependency",
			yaml:    "name: self-dep\ntasks:\n  - id: a\n    runtime: python\n    depends_on: [a]\n    script: print(1)\n",
			wantErr: "cannot depend on itself",
		},
		{
			name:    "missing runtime and image",
			yaml:    "name: missing-both\ntasks:\n  - id: a\n    script: echo 1\n",
			wantErr: "must specify either runtime or image",
		},
		{
			name:    "shell runtime without image",
			yaml:    "name: shell-no-img\ntasks:\n  - id: a\n    runtime: shell\n    script: echo 1\n",
			wantErr: "must specify an image",
		},
		{
			name:    "image with whitespace",
			yaml:    "name: bad-img\ntasks:\n  - id: a\n    runtime: python\n    image: \"python:3.13 slim\"\n    script: print(1)\n",
			wantErr: "must not contain whitespace",
		},
		{
			name:    "shell runtime with dependencies",
			yaml:    "name: shell-deps\ntasks:\n  - id: a\n    runtime: shell\n    image: alpine:3.20\n    dependencies: [curl]\n    script: echo 1\n",
			wantErr: "does not support dependencies",
		},
		{
			name:    "empty dependency entry",
			yaml:    "name: empty-dep\ntasks:\n  - id: a\n    runtime: python\n    dependencies: [\"   \"]\n    script: print(1)\n",
			wantErr: "dependency at index 0 is invalid",
		},
		{
			name:    "absolute include path",
			yaml:    "name: abs-inc\ninclude: [/etc/passwd]\ntasks:\n  - id: a\n    runtime: python\n    script: print(1)\n",
			wantErr: "must be relative",
		},
		{
			name:    "traversal include path",
			yaml:    "name: trav-inc\ntasks:\n  - id: a\n    runtime: python\n    include: [../secret.py]\n    script: print(1)\n",
			wantErr: "must stay inside",
		},
		{
			name:    "empty include path",
			yaml:    "name: empty-inc\ntasks:\n  - id: a\n    runtime: python\n    include: [\"  \"]\n    script: print(1)\n",
			wantErr: "include path at index 0 is empty",
		},
		{
			name: "cycle detection",
			yaml: `name: cycle
tasks:
  - id: a
    runtime: python
    depends_on: [b]
    script: print("a")
  - id: b
    runtime: js
    depends_on: [a]
    script: console.log("b")
`,
			wantErr: "dependency cycle detected",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tc.wantErr, err.Error())
			}
		})
	}
}

func TestParseCustomImages(t *testing.T) {
	yamlData := `
name: custom-images
tasks:
  - id: py
    runtime: python
    image: python:3.13-slim
    script: print("ok")
  - id: sh_only
    image: alpine:3.20
    depends_on: [py]
    script: echo "ok"
`
	wf, err := Parse([]byte(yamlData))
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if wf.Tasks[0].Image != "python:3.13-slim" || wf.Tasks[0].Runtime != RuntimePython {
		t.Fatalf("unexpected first task: %+v", wf.Tasks[0])
	}
	if wf.Tasks[1].Image != "alpine:3.20" || wf.Tasks[1].Runtime != RuntimeShell {
		t.Fatalf("unexpected second task: %+v", wf.Tasks[1])
	}
}

func TestParseExampleFile(t *testing.T) {
	examplePath := filepath.Join("..", "..", "example.yaml")
	if _, err := os.Stat(examplePath); err != nil {
		t.Skipf("example.yaml not found at %s", examplePath)
	}
	wf, err := ParseFile(examplePath)
	if err != nil {
		t.Fatalf("failed to parse example.yaml: %v", err)
	}
	if len(wf.Tasks) != 2 {
		t.Fatalf("expected 2 tasks in example.yaml, got %d", len(wf.Tasks))
	}
}
