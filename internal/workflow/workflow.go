package workflow

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	RuntimePython = "python"
	RuntimeJS     = "js"
	RuntimeShell  = "shell"
)

// Workflow represents a parsed YAML pipeline definition.
type Workflow struct {
	Name    string   `yaml:"name"`
	Include []string `yaml:"include,omitempty"`
	Tasks   []Task   `yaml:"tasks"`
	BaseDir string   `yaml:"-"`
}

// Task represents a single unit of execution within a workflow.
type Task struct {
	ID           string            `yaml:"id"`
	Runtime      string            `yaml:"runtime,omitempty"`
	Image        string            `yaml:"image,omitempty"`
	Dependencies []string          `yaml:"dependencies,omitempty"`
	Include      []string          `yaml:"include,omitempty"`
	DependsOn    []string          `yaml:"depends_on,omitempty"`
	Env          map[string]string `yaml:"env,omitempty"`
	Script       string            `yaml:"script"`
	BaseDir      string            `yaml:"-"`
}

// ParseFile reads and validates a workflow YAML file from disk.
func ParseFile(path string) (*Workflow, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve workflow path %q: %w", path, err)
	}
	data, err := os.ReadFile(absPath)
	if err != nil {
		return nil, fmt.Errorf("read workflow file %q: %w", path, err)
	}
	wf, err := Parse(data)
	if err != nil {
		return nil, err
	}
	wf.BaseDir = filepath.Dir(absPath)
	return wf, nil
}

// Parse unmarshals YAML bytes into a Workflow and validates its structure and DAG.
func Parse(data []byte) (*Workflow, error) {
	var wf Workflow
	if err := yaml.Unmarshal(data, &wf); err != nil {
		return nil, fmt.Errorf("parse workflow YAML: %w", err)
	}
	if wf.BaseDir == "" {
		wf.BaseDir = "."
	}
	if err := wf.Validate(); err != nil {
		return nil, err
	}
	return &wf, nil
}

// Validate checks workflow fields, task definitions, references, and dependency cycles.
func (w *Workflow) Validate() error {
	if strings.TrimSpace(w.Name) == "" {
		return fmt.Errorf("workflow name must not be empty")
	}
	if len(w.Tasks) == 0 {
		return fmt.Errorf("workflow %q must define at least one task", w.Name)
	}
	if err := validateIncludePaths(w.Include); err != nil {
		return fmt.Errorf("workflow %q: %w", w.Name, err)
	}

	taskByID := make(map[string]Task, len(w.Tasks))
	for i := range w.Tasks {
		t := &w.Tasks[i]
		if err := validateTaskID(t.ID); err != nil {
			return fmt.Errorf("task at index %d: %w", i, err)
		}
		if _, exists := taskByID[t.ID]; exists {
			return fmt.Errorf("duplicate task id %q", t.ID)
		}

		t.Image = strings.TrimSpace(t.Image)
		if strings.ContainsAny(t.Image, " \t\r\n") {
			return fmt.Errorf("task %q image %q must not contain whitespace", t.ID, t.Image)
		}

		if t.Runtime == "" {
			if t.Image == "" {
				return fmt.Errorf("task %q must specify either runtime or image", t.ID)
			}
			t.Runtime = RuntimeShell
		}

		switch t.Runtime {
		case RuntimePython, RuntimeJS:
		case RuntimeShell:
			if t.Image == "" {
				return fmt.Errorf("task %q with runtime %q must specify an image", t.ID, t.Runtime)
			}
			if len(t.Dependencies) > 0 {
				return fmt.Errorf("task %q with runtime %q does not support dependencies", t.ID, t.Runtime)
			}
		default:
			return fmt.Errorf(
				"task %q has unsupported runtime %q (supported: %q, %q, %q)",
				t.ID, t.Runtime, RuntimePython, RuntimeJS, RuntimeShell,
			)
		}

		for j, pkg := range t.Dependencies {
			trimmed := strings.TrimSpace(pkg)
			if trimmed == "" || strings.ContainsAny(trimmed, "\r\n") {
				return fmt.Errorf("task %q dependency at index %d is invalid", t.ID, j)
			}
			t.Dependencies[j] = trimmed
		}

		if err := validateIncludePaths(t.Include); err != nil {
			return fmt.Errorf("task %q: %w", t.ID, err)
		}

		if strings.TrimSpace(t.Script) == "" {
			return fmt.Errorf("task %q script must not be empty", t.ID)
		}
		taskByID[t.ID] = *t
	}

	for _, t := range w.Tasks {
		for _, dep := range t.DependsOn {
			if dep == t.ID {
				return fmt.Errorf("task %q cannot depend on itself", t.ID)
			}
			if _, ok := taskByID[dep]; !ok {
				return fmt.Errorf("task %q depends on unknown task %q", t.ID, dep)
			}
		}
	}

	if _, err := w.TopologicalOrder(); err != nil {
		return err
	}
	return nil
}

// TopologicalOrder returns tasks ordered such that every dependency runs before its dependents.
// It returns an error if a cycle exists in the dependency graph.
func (w *Workflow) TopologicalOrder() ([]Task, error) {
	taskByID := make(map[string]Task, len(w.Tasks))
	inDegree := make(map[string]int, len(w.Tasks))
	dependents := make(map[string][]string, len(w.Tasks))

	for _, t := range w.Tasks {
		taskByID[t.ID] = t
		inDegree[t.ID] = 0
	}

	for _, t := range w.Tasks {
		seenDeps := make(map[string]struct{}, len(t.DependsOn))
		for _, dep := range t.DependsOn {
			if _, dup := seenDeps[dep]; dup {
				continue
			}
			seenDeps[dep] = struct{}{}
			inDegree[t.ID]++
			dependents[dep] = append(dependents[dep], t.ID)
		}
	}

	queue := make([]string, 0, len(w.Tasks))
	for _, t := range w.Tasks {
		if inDegree[t.ID] == 0 {
			queue = append(queue, t.ID)
		}
	}

	ordered := make([]Task, 0, len(w.Tasks))
	for len(queue) > 0 {
		currID := queue[0]
		queue = queue[1:]
		ordered = append(ordered, taskByID[currID])

		for _, nextID := range dependents[currID] {
			inDegree[nextID]--
			if inDegree[nextID] == 0 {
				queue = append(queue, nextID)
			}
		}
	}

	if len(ordered) != len(w.Tasks) {
		var cyclic []string
		for _, t := range w.Tasks {
			if inDegree[t.ID] > 0 {
				cyclic = append(cyclic, t.ID)
			}
		}
		return nil, fmt.Errorf("dependency cycle detected involving tasks: %s", strings.Join(cyclic, ", "))
	}

	return ordered, nil
}

func validateTaskID(id string) error {
	if strings.TrimSpace(id) == "" {
		return fmt.Errorf("task id must not be empty")
	}
	if id == "." || id == ".." {
		return fmt.Errorf("invalid task id %q", id)
	}
	if strings.ContainsAny(id, "/\\ \t\r\n") {
		return fmt.Errorf("task id %q must not contain whitespace or path separators", id)
	}
	return nil
}

func validateIncludePaths(paths []string) error {
	for i, raw := range paths {
		trimmed := strings.TrimSpace(raw)
		if trimmed == "" || strings.ContainsAny(trimmed, "\r\n") {
			return fmt.Errorf("include path at index %d is empty", i)
		}
		if filepath.IsAbs(trimmed) || strings.HasPrefix(trimmed, "/") || strings.HasPrefix(trimmed, "\\") {
			return fmt.Errorf("include path %q must be relative", raw)
		}
		clean := filepath.ToSlash(filepath.Clean(trimmed))
		if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
			return fmt.Errorf("include path %q must stay inside the workflow directory", raw)
		}
		paths[i] = clean
	}
	return nil
}
