package ui

import (
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const maxPreviewBytes = 512 * 1024 // 512 KiB preview limit in web UI

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
	absRoot, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(absRoot)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	if !info.IsDir() {
		return nil, nil
	}

	var files []includeFileEntry
	err = filepath.WalkDir(absRoot, func(p string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(absRoot, p)
		if err != nil {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		relSlash := filepath.ToSlash(rel)
		files = append(files, includeFileEntry{
			RelPath:       relSlash,
			Size:          fi.Size(),
			SizeFormatted: formatHumanBytes(fi.Size()),
			SearchKey:     strings.ToLower(relSlash),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].RelPath < files[j].RelPath
	})
	return files, nil
}

func readIncludeFileSafe(dir, rel string) (string, int64, error) {
	absRoot, err := filepath.Abs(dir)
	if err != nil {
		return "", 0, err
	}
	cleanRel := filepath.Clean(filepath.FromSlash(rel))
	if filepath.IsAbs(cleanRel) || cleanRel == "." || cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(filepath.Separator)) {
		return "", 0, fmt.Errorf("invalid file path %q", rel)
	}
	absTarget := filepath.Join(absRoot, cleanRel)
	checkRel, err := filepath.Rel(absRoot, absTarget)
	if err != nil || checkRel == ".." || strings.HasPrefix(checkRel, ".."+string(filepath.Separator)) {
		return "", 0, fmt.Errorf("file path %q escapes includes directory", rel)
	}

	info, err := os.Stat(absTarget)
	if err != nil {
		return "", 0, fmt.Errorf("file %q not found", rel)
	}
	if info.IsDir() {
		return "", 0, fmt.Errorf("%q is a directory", rel)
	}
	if info.Size() > maxPreviewBytes {
		return "", info.Size(), fmt.Errorf("file %q (%s) exceeds %s preview limit", rel, formatHumanBytes(info.Size()), formatHumanBytes(maxPreviewBytes))
	}

	raw, err := os.ReadFile(absTarget)
	if err != nil {
		return "", info.Size(), err
	}
	return string(raw), info.Size(), nil
}
