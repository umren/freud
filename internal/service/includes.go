package service

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// MaxIncludePreviewBytes bounds in-memory file reads when inspecting includes via UI, API, or MCP.
const MaxIncludePreviewBytes int64 = 512 * 1024 // 512 KiB

var (
	// ErrIncludeNotFound indicates the requested include path does not exist.
	ErrIncludeNotFound = errors.New("include file not found")
	// ErrInvalidInclude indicates a path traversal, directory target, or oversized file.
	ErrInvalidInclude = errors.New("invalid include file")
)

// IncludeFile describes an entry under the includes directory.
type IncludeFile struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
}

// IncludeContent carries an include file's metadata and UTF-8 text content.
type IncludeContent struct {
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	Content string `json:"content"`
}

// ListIncludes walks includesDir recursively and returns relative forward-slash paths sorted ascending.
func ListIncludes(includesDir string) ([]IncludeFile, error) {
	absRoot, err := filepath.Abs(includesDir)
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

	var files []IncludeFile
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
		files = append(files, IncludeFile{
			Path: filepath.ToSlash(rel),
			Size: fi.Size(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	return files, nil
}

// ReadInclude validates rel against path traversal and returns its content up to MaxIncludePreviewBytes.
func ReadInclude(includesDir, rel string) (*IncludeContent, error) {
	absRoot, err := filepath.Abs(includesDir)
	if err != nil {
		return nil, err
	}
	trimmed := strings.TrimSpace(rel)
	tryPaths := []string{trimmed}
	if withoutPrefix := strings.TrimPrefix(filepath.ToSlash(trimmed), "includes/"); withoutPrefix != trimmed && withoutPrefix != "" {
		tryPaths = append(tryPaths, withoutPrefix)
	}

	var absTarget string
	var checkRel string
	var info os.FileInfo
	for _, candidate := range tryPaths {
		cleanRel := filepath.Clean(filepath.FromSlash(candidate))
		if candidate == "" || filepath.IsAbs(cleanRel) || cleanRel == "." || cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("%w: invalid file path %q", ErrInvalidInclude, rel)
		}
		target := filepath.Join(absRoot, cleanRel)
		relCheck, relErr := filepath.Rel(absRoot, target)
		if relErr != nil || relCheck == ".." || strings.HasPrefix(relCheck, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("%w: file path %q escapes includes directory", ErrInvalidInclude, rel)
		}
		if err := ensureNoSymlinksUnderRoot(absRoot, filepath.ToSlash(relCheck), ErrInvalidInclude); err != nil {
			return nil, err
		}
		st, statErr := os.Lstat(target)
		if statErr == nil {
			absTarget = target
			checkRel = relCheck
			info = st
			break
		}
		if !os.IsNotExist(statErr) {
			return nil, statErr
		}
	}
	if info == nil {
		return nil, fmt.Errorf("%w: %q", ErrIncludeNotFound, rel)
	}
	if info.IsDir() {
		return nil, fmt.Errorf("%w: %q is a directory", ErrInvalidInclude, rel)
	}
	if info.Size() > MaxIncludePreviewBytes {
		return nil, fmt.Errorf("%w: %q (%d bytes) exceeds %d byte limit", ErrInvalidInclude, rel, info.Size(), MaxIncludePreviewBytes)
	}

	raw, err := os.ReadFile(absTarget)
	if err != nil {
		return nil, err
	}
	return &IncludeContent{
		Path:    filepath.ToSlash(checkRel),
		Size:    info.Size(),
		Content: string(raw),
	}, nil
}
