package ui

import (
	"net/http"
	"strings"

	"freud/internal/service"
)

type includeFileEntry struct {
	RelPath       string
	Size          int64
	SizeFormatted string
	SearchKey     string
}

type includesViewData struct {
	basePageData
	IncludesDir  string
	Files        []includeFileEntry
	SelectedRel  string
	SelectedSize string
	RawContent   string
	Lines        []codeLine
	ReadErr      string
}

func (s *Server) handleIncludes(w http.ResponseWriter, r *http.Request) {
	stats, err := s.store.GetRunStats(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	files, err := listIncludeFiles(s.cfg.IncludesDir)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	selected := strings.TrimSpace(r.URL.Query().Get("file"))
	if selected == "" && len(files) > 0 {
		selected = files[0].RelPath
	}

	data := includesViewData{
		basePageData: s.newBaseData(r, "Includes", "includes", stats.Running),
		IncludesDir:  s.cfg.IncludesDir,
		Files:        files,
		SelectedRel:  selected,
	}

	if selected != "" {
		content, size, readErr := readIncludeFileSafe(s.cfg.IncludesDir, selected)
		if readErr != nil {
			data.ReadErr = readErr.Error()
		} else {
			data.SelectedSize = formatHumanBytes(size)
			data.RawContent = content
			data.Lines = HighlightByFilename(selected, content)
		}
	}

	s.renderPage(w, "includes", data)
}

func listIncludeFiles(dir string) ([]includeFileEntry, error) {
	raw, err := service.ListIncludes(dir)
	if err != nil {
		return nil, err
	}
	files := make([]includeFileEntry, 0, len(raw))
	for _, f := range raw {
		files = append(files, includeFileEntry{
			RelPath:       f.Path,
			Size:          f.Size,
			SizeFormatted: formatHumanBytes(f.Size),
			SearchKey:     strings.ToLower(f.Path),
		})
	}
	return files, nil
}

func readIncludeFileSafe(dir, rel string) (string, int64, error) {
	inc, err := service.ReadInclude(dir, rel)
	if err != nil {
		return "", 0, err
	}
	return inc.Content, inc.Size, nil
}
