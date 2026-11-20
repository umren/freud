package ui

import (
	"html"
	"html/template"
	"path/filepath"
	"strings"
	"unicode"
)

type langSpec struct {
	lineComment  string
	hashComment  bool
	blockComment bool
	tripleQuote  bool
	backtickStr  bool
	dollarVars   bool
	decorators   bool
	keywords     map[string]struct{}
	builtins     map[string]struct{}
}

var (
	pythonLang = langSpec{
		hashComment: true,
		tripleQuote: true,
		decorators:  true,
		keywords: setOf(
			"and", "as", "assert", "async", "await", "break", "class", "continue",
			"def", "del", "elif", "else", "except", "False", "finally", "for",
			"from", "global", "if", "import", "in", "is", "lambda", "None",
			"nonlocal", "not", "or", "pass", "raise", "return", "True", "try",
			"while", "with", "yield",
		),
		builtins: setOf(
			"abs", "all", "any", "bool", "dict", "enumerate", "float", "int",
			"isinstance", "len", "list", "map", "max", "min", "open", "print",
			"range", "set", "sorted", "str", "sum", "tuple", "type", "zip",
		),
	}

	jsLang = langSpec{
		lineComment:  "//",
		blockComment: true,
		backtickStr:  true,
		keywords: setOf(
			"as", "async", "await", "break", "case", "catch", "class", "const",
			"continue", "default", "delete", "do", "else", "export", "extends",
			"false", "finally", "for", "from", "function", "if", "implements",
			"import", "in", "instanceof", "interface", "let", "new", "null",
			"of", "return", "static", "switch", "this", "throw", "true", "try",
			"type", "typeof", "undefined", "var", "void", "while", "yield",
		),
		builtins: setOf(
			"Array", "Boolean", "console", "Date", "Error", "JSON", "Map",
			"Math", "Number", "Object", "Promise", "RegExp", "Set", "String",
			"boolean", "never", "number", "process", "string", "unknown",
		),
	}

	shellLang = langSpec{
		hashComment: true,
		backtickStr: true,
		dollarVars:  true,
		keywords: setOf(
			"case", "do", "done", "elif", "else", "esac", "exec", "exit",
			"export", "fi", "for", "function", "if", "in", "local", "return",
			"set", "shift", "then", "until", "while",
		),
		builtins: setOf(
			"awk", "cat", "cd", "chmod", "cp", "curl", "echo", "env", "grep",
			"head", "ls", "mkdir", "mv", "printf", "pwd", "rm", "sed", "sort",
			"tail", "test", "touch", "tr", "wc",
		),
	}

	sqlLang = langSpec{
		lineComment:  "--",
		blockComment: true,
		keywords: setOf(
			"ALL", "AND", "AS", "ASC", "BETWEEN", "BY", "CASE", "CREATE", "DELETE",
			"DESC", "DISTINCT", "DROP", "ELSE", "END", "EXISTS", "FROM", "GROUP",
			"HAVING", "IN", "INDEX", "INNER", "INSERT", "INTO", "IS", "JOIN",
			"LEFT", "LIMIT", "NOT", "NULL", "ON", "OR", "ORDER", "OUTER", "RIGHT",
			"SELECT", "SET", "TABLE", "THEN", "UNION", "UPDATE", "VALUES", "WHEN",
			"WHERE",
		),
		builtins: setOf(
			"AVG", "COALESCE", "COUNT", "MAX", "MIN", "SUM",
		),
	}

	jsonLang = langSpec{
		keywords: setOf("true", "false", "null"),
	}
)

func setOf(items ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, item := range items {
		m[item] = struct{}{}
	}
	return m
}

// HighlightWorkflowYAML renders workflow YAML with syntax highlighting,
// including embedded Python/JS/Shell highlighting inside `script: |` blocks.
func HighlightWorkflowYAML(raw string) []codeLine {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	rawLines := strings.Split(normalized, "\n")

	scriptLangs := detectScriptBlockLanguages(rawLines)
	lines := make([]codeLine, len(rawLines))

	i := 0
	for i < len(rawLines) {
		if info, ok := scriptLangs[i]; ok {
			if !info.isBlock {
				lines[i] = codeLine{
					Number: i + 1,
					HTML:   highlightInlineScriptYAMLLine(rawLines[i], info.spec),
				}
				i++
				continue
			}

			// Highlight the `script: |` header line as YAML, then collect the indented script block.
			lines[i] = codeLine{Number: i + 1, HTML: highlightYAMLLine(rawLines[i])}
			headerIndent := leadingSpaces(rawLines[i])
			start := i + 1
			end := start
			for end < len(rawLines) {
				line := rawLines[end]
				if strings.TrimSpace(line) == "" {
					end++
					continue
				}
				if leadingSpaces(line) <= headerIndent {
					break
				}
				end++
			}
			if end > start {
				blockHTML := highlightCodeLines(rawLines[start:end], info.spec)
				for idx, htmlLine := range blockHTML {
					lines[start+idx] = codeLine{
						Number: start + idx + 1,
						HTML:   htmlLine,
					}
				}
			}
			i = end
			continue
		}

		lines[i] = codeLine{
			Number: i + 1,
			HTML:   highlightYAMLLine(rawLines[i]),
		}
		i++
	}

	return lines
}

// HighlightByFilename renders any file with syntax highlighting inferred from its extension.
func HighlightByFilename(filename, raw string) []codeLine {
	ext := strings.ToLower(filepath.Ext(filename))
	switch ext {
	case ".yaml", ".yml":
		return HighlightWorkflowYAML(raw)
	case ".py":
		return toCodeLines(highlightCodeLines(splitRawLines(raw), pythonLang))
	case ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs":
		return toCodeLines(highlightCodeLines(splitRawLines(raw), jsLang))
	case ".sh", ".bash", ".zsh":
		return toCodeLines(highlightCodeLines(splitRawLines(raw), shellLang))
	case ".sql":
		return toCodeLines(highlightCodeLines(splitRawLines(raw), sqlLang))
	case ".json":
		return toCodeLines(highlightJSONLines(splitRawLines(raw)))
	default:
		rawLines := splitRawLines(raw)
		out := make([]codeLine, len(rawLines))
		for i, line := range rawLines {
			out[i] = codeLine{
				Number: i + 1,
				HTML:   template.HTML(html.EscapeString(line)),
			}
		}
		return out
	}
}

func splitRawLines(raw string) []string {
	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	return strings.Split(normalized, "\n")
}

func toCodeLines(htmlLines []template.HTML) []codeLine {
	out := make([]codeLine, len(htmlLines))
	for i, h := range htmlLines {
		out[i] = codeLine{Number: i + 1, HTML: h}
	}
	return out
}

type scriptLineInfo struct {
	spec    langSpec
	isBlock bool
}

// detectScriptBlockLanguages pre-scans YAML task blocks so `script: |` and inline `script:`
// highlight correctly regardless of whether `runtime:` appears before or after `script:`.
func detectScriptBlockLanguages(lines []string) map[int]scriptLineInfo {
	result := make(map[int]scriptLineInfo)

	type scriptRef struct {
		lineIdx int
		isBlock bool
	}
	type taskMeta struct {
		runtime     string
		hasImage    bool
		scriptLines []scriptRef
	}

	var tasks []taskMeta
	currentIdx := -1
	inTasks := false
	tasksIndent := -1
	taskItemIndent := -1
	inScriptBlock := false
	scriptHeaderIndent := 0

	flushTask := func(idx int) {
		if idx < 0 || idx >= len(tasks) {
			return
		}
		tm := tasks[idx]
		var spec langSpec
		switch tm.runtime {
		case "python":
			spec = pythonLang
		case "js":
			spec = jsLang
		case "shell":
			spec = shellLang
		case "":
			if tm.hasImage {
				spec = shellLang
			} else {
				return
			}
		default:
			return
		}
		for _, ref := range tm.scriptLines {
			result[ref.lineIdx] = scriptLineInfo{spec: spec, isBlock: ref.isBlock}
		}
	}

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		indent := leadingSpaces(line)

		if inScriptBlock {
			if indent > scriptHeaderIndent {
				continue
			}
			inScriptBlock = false
		}

		if !inTasks {
			if strings.HasPrefix(trimmed, "tasks:") {
				inTasks = true
				tasksIndent = indent
			}
			continue
		}

		if indent <= tasksIndent && !strings.HasPrefix(trimmed, "-") {
			inTasks = false
			continue
		}

		if strings.HasPrefix(trimmed, "- ") && (taskItemIndent == -1 || indent <= taskItemIndent) {
			taskItemIndent = indent
			tasks = append(tasks, taskMeta{})
			currentIdx = len(tasks) - 1
			trimmed = strings.TrimSpace(trimmed[2:])
		}
		if currentIdx < 0 {
			continue
		}

		if k, v, ok := splitYAMLKV(trimmed); ok {
			switch k {
			case "runtime":
				tasks[currentIdx].runtime = strings.Trim(strings.TrimSpace(v), `"'`)
			case "image":
				if strings.TrimSpace(v) != "" {
					tasks[currentIdx].hasImage = true
				}
			case "script":
				val := strings.TrimSpace(v)
				if strings.HasPrefix(val, "|") || strings.HasPrefix(val, ">") {
					tasks[currentIdx].scriptLines = append(tasks[currentIdx].scriptLines, scriptRef{lineIdx: i, isBlock: true})
					inScriptBlock = true
					scriptHeaderIndent = indent
				} else if val != "" {
					tasks[currentIdx].scriptLines = append(tasks[currentIdx].scriptLines, scriptRef{lineIdx: i, isBlock: false})
				}
			}
		}
	}

	for i := range tasks {
		flushTask(i)
	}
	return result
}

func leadingSpaces(s string) int {
	n := 0
	for n < len(s) && (s[n] == ' ' || s[n] == '\t') {
		n++
	}
	return n
}

func splitYAMLKV(trimmed string) (string, string, bool) {
	if strings.HasPrefix(trimmed, "#") {
		return "", "", false
	}
	colon := strings.IndexByte(trimmed, ':')
	if colon <= 0 {
		return "", "", false
	}
	if colon+1 < len(trimmed) && trimmed[colon+1] != ' ' && trimmed[colon+1] != '\t' {
		return "", "", false
	}
	key := strings.TrimSpace(trimmed[:colon])
	val := ""
	if colon+1 < len(trimmed) {
		val = trimmed[colon+1:]
	}
	return key, val, true
}

func highlightInlineScriptYAMLLine(line string, spec langSpec) template.HTML {
	colon := strings.IndexByte(line, ':')
	if colon <= 0 {
		return highlightYAMLLine(line)
	}
	var b strings.Builder
	b.WriteString(string(highlightYAMLLine(line[:colon+1])))
	after := line[colon+1:]
	j := 0
	for j < len(after) && (after[j] == ' ' || after[j] == '\t') {
		b.WriteByte(after[j])
		j++
	}
	if j < len(after) {
		codeHTML := highlightCodeLines([]string{after[j:]}, spec)
		b.WriteString(string(codeHTML[0]))
	}
	return template.HTML(b.String())
}

func highlightYAMLLine(line string) template.HTML {
	var b strings.Builder
	i := 0
	for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
		b.WriteByte(line[i])
		i++
	}
	if i >= len(line) {
		return template.HTML(b.String())
	}

	if line[i] == '#' {
		writeSpan(&b, "tok-com", line[i:])
		return template.HTML(b.String())
	}

	if strings.HasPrefix(line[i:], "- ") || line[i:] == "-" {
		writeSpan(&b, "tok-op", "-")
		i++
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			b.WriteByte(line[i])
			i++
		}
	}

	rest := line[i:]
	if k, v, ok := splitYAMLKV(rest); ok {
		writeSpan(&b, "tok-key", k)
		writeSpan(&b, "tok-op", ":")
		b.WriteString(string(highlightYAMLValue(v)))
		return template.HTML(b.String())
	}

	b.WriteString(string(highlightYAMLValue(rest)))
	return template.HTML(b.String())
}

func highlightYAMLValue(val string) template.HTML {
	var b strings.Builder
	i := 0
	for i < len(val) && (val[i] == ' ' || val[i] == '\t') {
		b.WriteByte(val[i])
		i++
	}
	if i >= len(val) {
		return template.HTML(b.String())
	}

	rest := val[i:]
	switch {
	case strings.HasPrefix(rest, "#"):
		writeSpan(&b, "tok-com", rest)
	case rest[0] == '|' || rest[0] == '>':
		writeSpan(&b, "tok-op", rest)
	case rest[0] == '"' || rest[0] == '\'':
		quote := rest[0]
		end := findClosingQuote(rest, quote)
		writeYAMLStringWithVars(&b, rest[:end])
		if end < len(rest) {
			b.WriteString(string(highlightYAMLValue(rest[end:])))
		}
	case strings.HasPrefix(rest, "[") && strings.HasSuffix(strings.TrimSpace(rest), "]"):
		writeSpan(&b, "tok-str", rest)
	default:
		// Split optional inline comment ` #`.
		body := rest
		comment := ""
		if idx := strings.Index(rest, " #"); idx != -1 {
			body = rest[:idx]
			comment = rest[idx:]
		}
		trimBody := strings.TrimSpace(body)
		switch {
		case trimBody == "true" || trimBody == "false" || trimBody == "null" || isNumericLiteral(trimBody):
			writeSpan(&b, "tok-num", body)
		default:
			writeYAMLStringWithVars(&b, body)
		}
		if comment != "" {
			writeSpan(&b, "tok-com", comment)
		}
	}

	return template.HTML(b.String())
}

func writeYAMLStringWithVars(b *strings.Builder, s string) {
	i := 0
	for i < len(s) {
		idx := strings.Index(s[i:], "${")
		if idx == -1 {
			writeSpan(b, "tok-str", s[i:])
			return
		}
		if idx > 0 {
			writeSpan(b, "tok-str", s[i:i+idx])
		}
		end := strings.IndexByte(s[i+idx+2:], '}')
		if end == -1 {
			writeSpan(b, "tok-str", s[i+idx:])
			return
		}
		writeSpan(b, "tok-var", s[i+idx:i+idx+2+end+1])
		i = i + idx + 2 + end + 1
	}
}

func highlightJSONLines(lines []string) []template.HTML {
	out := make([]template.HTML, len(lines))
	for idx, line := range lines {
		var b strings.Builder
		i := 0
		for i < len(line) {
			ch := line[i]
			if ch == '"' {
				end := findClosingQuote(line[i:], '"')
				tok := line[i : i+end]
				// Look ahead for `:` to distinguish JSON object keys from string values.
				j := i + end
				for j < len(line) && (line[j] == ' ' || line[j] == '\t') {
					j++
				}
				if j < len(line) && line[j] == ':' {
					writeSpan(&b, "tok-key", tok)
				} else {
					writeSpan(&b, "tok-str", tok)
				}
				i += end
				continue
			}
			if isNumStart(line, i) {
				end := scanNumber(line, i)
				writeSpan(&b, "tok-num", line[i:end])
				i = end
				continue
			}
			if isIdentStart(ch) {
				end := scanIdent(line, i)
				word := line[i:end]
				if _, ok := jsonLang.keywords[word]; ok {
					writeSpan(&b, "tok-num", word)
				} else {
					b.WriteString(html.EscapeString(word))
				}
				i = end
				continue
			}
			b.WriteString(html.EscapeString(line[i : i+1]))
			i++
		}
		out[idx] = template.HTML(b.String())
	}
	return out
}

func highlightCodeLines(lines []string, spec langSpec) []template.HTML {
	out := make([]template.HTML, len(lines))
	inBlockComment := false
	tripleDelim := ""

	for lineIdx, line := range lines {
		var b strings.Builder
		i := 0

		for i < len(line) {
			if inBlockComment {
				end := strings.Index(line[i:], "*/")
				if end == -1 {
					writeSpan(&b, "tok-com", line[i:])
					i = len(line)
				} else {
					writeSpan(&b, "tok-com", line[i:i+end+2])
					i += end + 2
					inBlockComment = false
				}
				continue
			}

			if tripleDelim != "" {
				end := strings.Index(line[i:], tripleDelim)
				if end == -1 {
					writeSpan(&b, "tok-str", line[i:])
					i = len(line)
				} else {
					writeSpan(&b, "tok-str", line[i:i+end+3])
					i += end + 3
					tripleDelim = ""
				}
				continue
			}

			if spec.blockComment && strings.HasPrefix(line[i:], "/*") {
				end := strings.Index(line[i+2:], "*/")
				if end == -1 {
					writeSpan(&b, "tok-com", line[i:])
					inBlockComment = true
					i = len(line)
				} else {
					writeSpan(&b, "tok-com", line[i:i+2+end+2])
					i += 2 + end + 2
				}
				continue
			}

			if spec.lineComment != "" && strings.HasPrefix(line[i:], spec.lineComment) {
				writeSpan(&b, "tok-com", line[i:])
				break
			}

			if spec.hashComment && line[i] == '#' {
				writeSpan(&b, "tok-com", line[i:])
				break
			}

			if spec.tripleQuote && (strings.HasPrefix(line[i:], `"""`) || strings.HasPrefix(line[i:], `'''`)) {
				delim := line[i : i+3]
				end := strings.Index(line[i+3:], delim)
				if end == -1 {
					writeSpan(&b, "tok-str", line[i:])
					tripleDelim = delim
					i = len(line)
				} else {
					writeSpan(&b, "tok-str", line[i:i+3+end+3])
					i += 3 + end + 3
				}
				continue
			}

			if line[i] == '"' || line[i] == '\'' || (spec.backtickStr && line[i] == '`') {
				end := findClosingQuote(line[i:], line[i])
				writeSpan(&b, "tok-str", line[i:i+end])
				i += end
				continue
			}

			if spec.decorators && line[i] == '@' && i+1 < len(line) && isIdentStart(line[i+1]) {
				end := scanIdent(line, i+1)
				writeSpan(&b, "tok-var", line[i:end])
				i = end
				continue
			}

			if spec.dollarVars && line[i] == '$' && i+1 < len(line) {
				if line[i+1] == '{' {
					if end := strings.IndexByte(line[i+2:], '}'); end != -1 {
						writeSpan(&b, "tok-var", line[i:i+2+end+1])
						i = i + 2 + end + 1
						continue
					}
				} else if isIdentStart(line[i+1]) {
					end := scanIdent(line, i+1)
					writeSpan(&b, "tok-var", line[i:end])
					i = end
					continue
				}
			}

			if isNumStart(line, i) {
				end := scanNumber(line, i)
				writeSpan(&b, "tok-num", line[i:end])
				i = end
				continue
			}

			if isIdentStart(line[i]) {
				end := scanIdent(line, i)
				word := line[i:end]
				lookup := word
				if spec.lineComment == "--" {
					lookup = strings.ToUpper(word)
				}
				if _, ok := spec.keywords[lookup]; ok {
					writeSpan(&b, "tok-kw", word)
				} else if _, ok := spec.builtins[lookup]; ok {
					writeSpan(&b, "tok-type", word)
				} else {
					b.WriteString(html.EscapeString(word))
				}
				i = end
				continue
			}

			b.WriteString(html.EscapeString(line[i : i+1]))
			i++
		}

		out[lineIdx] = template.HTML(b.String())
	}

	return out
}

func writeSpan(b *strings.Builder, className, text string) {
	b.WriteString(`<span class="`)
	b.WriteString(className)
	b.WriteString(`">`)
	b.WriteString(html.EscapeString(text))
	b.WriteString(`</span>`)
}

func findClosingQuote(s string, quote byte) int {
	escaped := false
	for i := 1; i < len(s); i++ {
		if escaped {
			escaped = false
			continue
		}
		if s[i] == '\\' {
			escaped = true
			continue
		}
		if s[i] == quote {
			return i + 1
		}
	}
	return len(s)
}

func isIdentStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func scanIdent(s string, start int) int {
	i := start
	for i < len(s) {
		ch := s[i]
		if isIdentStart(ch) || (ch >= '0' && ch <= '9') {
			i++
		} else {
			break
		}
	}
	return i
}

func isNumStart(s string, i int) bool {
	if s[i] < '0' || s[i] > '9' {
		return false
	}
	if i > 0 && (isIdentStart(s[i-1]) || (s[i-1] >= '0' && s[i-1] <= '9')) {
		return false
	}
	return true
}

func scanNumber(s string, start int) int {
	i := start
	for i < len(s) {
		ch := s[i]
		if (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == 'x' || ch == 'X' ||
			(ch >= 'a' && ch <= 'f') || (ch >= 'A' && ch <= 'F') {
			i++
		} else {
			break
		}
	}
	return i
}

func isNumericLiteral(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) && r != '.' && r != '-' && r != '+' {
			return false
		}
	}
	return true
}
