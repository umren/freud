package ui

import (
	"strings"
	"testing"
)

func TestHighlightWorkflowYAMLWithEmbeddedScripts(t *testing.T) {
	yamlSrc := `name: sample
cron: "0 */6 * * *"
tasks:
  - id: py_task
    runtime: python
    env:
      TOKEN: "Bearer ${API_TOKEN}"
    script: |
      # python comment
      import os
      def greet(name: str):
          print(f"hi {name}")
  - id: js_task
    runtime: js
    script: |
      // js comment
      const x: number = 42;
      console.log(` + "`val=${x}`" + `);
`

	lines := HighlightWorkflowYAML(yamlSrc)
	joined := joinLineHTML(lines)

	checks := []string{
		`<span class="tok-key">name</span>`,
		`<span class="tok-var">${API_TOKEN}</span>`,
		`<span class="tok-com"># python comment</span>`,
		`<span class="tok-kw">import</span>`,
		`<span class="tok-kw">def</span>`,
		`<span class="tok-type">print</span>`,
		`<span class="tok-com">// js comment</span>`,
		`<span class="tok-kw">const</span>`,
		`<span class="tok-type">number</span>`,
		`<span class="tok-num">42</span>`,
		`<span class="tok-type">console</span>`,
	}
	for _, want := range checks {
		if !strings.Contains(joined, want) {
			t.Fatalf("expected highlighted output to contain %s\ngot:\n%s", want, joined)
		}
	}
}

func TestHighlightByFilenameXSSSafe(t *testing.T) {
	pySrc := `x = "<script>alert(1)</script>"`
	lines := HighlightByFilename("evil.py", pySrc)
	joined := joinLineHTML(lines)
	if strings.Contains(joined, "<script>") {
		t.Fatalf("expected escaped script tag, got: %s", joined)
	}
	if !strings.Contains(joined, "&lt;script&gt;") {
		t.Fatalf("expected HTML-escaped content, got: %s", joined)
	}

	jsonLines := HighlightByFilename("cfg.json", `{"enabled": true, "count": 3}`)
	jsonHTML := joinLineHTML(jsonLines)
	if !strings.Contains(jsonHTML, `<span class="tok-key">&#34;enabled&#34;</span>`) || !strings.Contains(jsonHTML, `<span class="tok-num">true</span>`) {
		t.Fatalf("unexpected json highlight: %s", jsonHTML)
	}
}

func joinLineHTML(lines []codeLine) string {
	parts := make([]string, len(lines))
	for i, l := range lines {
		parts[i] = string(l.HTML)
	}
	return strings.Join(parts, "\n")
}
