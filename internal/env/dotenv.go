package env

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ParseFile reads a .env file from disk and returns its key-value pairs.
func ParseFile(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open env file %q: %w", path, err)
	}
	defer f.Close()

	vars := make(map[string]string)
	scanner := bufio.NewScanner(f)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "export ") || strings.HasPrefix(line, "export\t") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export"))
		}

		eqIdx := strings.IndexByte(line, '=')
		if eqIdx <= 0 {
			return nil, fmt.Errorf("env file %q line %d: expected KEY=VALUE", path, lineNum)
		}

		key := strings.TrimSpace(line[:eqIdx])
		if key == "" || strings.ContainsAny(key, " \t") {
			return nil, fmt.Errorf("env file %q line %d: invalid variable name %q", path, lineNum, key)
		}

		rawVal := strings.TrimSpace(line[eqIdx+1:])
		val, err := parseValue(rawVal)
		if err != nil {
			return nil, fmt.Errorf("env file %q line %d (%s): %w", path, lineNum, key, err)
		}

		vars[key] = val
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read env file %q: %w", path, err)
	}

	return vars, nil
}

// LoadDefault discovers and loads .env files from the workflow directory and current working directory.
// Variables in the working directory .env override those in the workflow directory .env.
// Missing .env files are silently ignored. Returns loaded variables and the list of loaded file paths.
func LoadDefault(workflowPath string) (map[string]string, []string, error) {
	candidates := make([]string, 0, 2)
	if workflowPath != "" {
		wfDirEnv := filepath.Join(filepath.Dir(workflowPath), ".env")
		candidates = append(candidates, wfDirEnv)
	}
	candidates = append(candidates, ".env")

	merged := make(map[string]string)
	var loadedPaths []string
	var loadedInfos []os.FileInfo

	for _, candidate := range candidates {
		info, err := os.Stat(candidate)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, nil, fmt.Errorf("stat env file %q: %w", candidate, err)
		}
		if info.IsDir() {
			continue
		}

		duplicate := false
		for _, prev := range loadedInfos {
			if os.SameFile(prev, info) {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}

		vars, err := ParseFile(candidate)
		if err != nil {
			return nil, nil, err
		}
		for k, v := range vars {
			merged[k] = v
		}
		loadedPaths = append(loadedPaths, candidate)
		loadedInfos = append(loadedInfos, info)
	}

	return merged, loadedPaths, nil
}

// MergeTaskEnv combines base environment variables (e.g. from .env) with task-specific env entries.
// Task entries override base entries and may reference base entries via ${VAR}.
func MergeTaskEnv(baseEnv, taskEnv map[string]string) map[string]string {
	if len(baseEnv) == 0 && len(taskEnv) == 0 {
		return nil
	}

	merged := make(map[string]string, len(baseEnv)+len(taskEnv))
	for k, v := range baseEnv {
		merged[k] = v
	}
	for k, v := range taskEnv {
		merged[k] = expandBracedVars(v, baseEnv)
	}
	return merged
}

func expandBracedVars(value string, vars map[string]string) string {
	if len(vars) == 0 || !strings.Contains(value, "${") {
		return value
	}

	var out strings.Builder
	for i := 0; i < len(value); {
		if i+2 < len(value) && value[i] == '$' && value[i+1] == '{' {
			end := strings.IndexByte(value[i+2:], '}')
			if end > 0 {
				name := value[i+2 : i+2+end]
				if replacement, ok := vars[name]; ok {
					out.WriteString(replacement)
					i = i + 2 + end + 1
					continue
				}
			}
		}
		out.WriteByte(value[i])
		i++
	}
	return out.String()
}

func parseValue(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}

	switch raw[0] {
	case '\'':
		end := strings.IndexByte(raw[1:], '\'')
		if end == -1 {
			return "", fmt.Errorf("unterminated single-quoted value")
		}
		rest := strings.TrimSpace(raw[1+end+1:])
		if rest != "" && !strings.HasPrefix(rest, "#") {
			return "", fmt.Errorf("unexpected trailing characters after single-quoted value")
		}
		return raw[1 : 1+end], nil

	case '"':
		var b strings.Builder
		escaped := false
		for i := 1; i < len(raw); i++ {
			ch := raw[i]
			if escaped {
				switch ch {
				case 'n':
					b.WriteByte('\n')
				case 'r':
					b.WriteByte('\r')
				case 't':
					b.WriteByte('\t')
				case '"', '\\':
					b.WriteByte(ch)
				default:
					b.WriteByte('\\')
					b.WriteByte(ch)
				}
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				rest := strings.TrimSpace(raw[i+1:])
				if rest != "" && !strings.HasPrefix(rest, "#") {
					return "", fmt.Errorf("unexpected trailing characters after double-quoted value")
				}
				return b.String(), nil
			}
			b.WriteByte(ch)
		}
		return "", fmt.Errorf("unterminated double-quoted value")

	default:
		// Strip inline comments preceded by whitespace in unquoted values.
		for i := 0; i < len(raw); i++ {
			if raw[i] == '#' && i > 0 && (raw[i-1] == ' ' || raw[i-1] == '\t') {
				return strings.TrimSpace(raw[:i]), nil
			}
		}
		return strings.TrimSpace(raw), nil
	}
}
