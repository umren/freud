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
