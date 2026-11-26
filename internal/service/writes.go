package service

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"freud/internal/workflow"
)

func validateFlatFlowFileName(fileName string) (string, error) {
	trimmed := strings.TrimSpace(fileName)
	if trimmed == "" || strings.HasPrefix(trimmed, ".") || strings.ContainsAny(trimmed, "/\\ \t\r\n") {
		return "", fmt.Errorf("%w: invalid flow filename %q", ErrInvalidFlow, fileName)
	}
	ext := strings.ToLower(filepath.Ext(trimmed))
	if ext != ".yaml" && ext != ".yml" {
		return "", fmt.Errorf("%w: flow filename %q must end with .yaml or .yml", ErrInvalidFlow, fileName)
	}
	return trimmed, nil
}

func writeFileAtomic(dst string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".freud-write-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, dst)
}

// SaveFlow pre-validates rawYAML via workflow.Parse and atomically writes flowsDir/fileName.
func SaveFlow(flowsDir, fileName, rawYAML string) (created bool, cleanName string, wf *workflow.Workflow, err error) {
	cleanName, err = validateFlatFlowFileName(fileName)
	if err != nil {
		return false, "", nil, err
	}
	if strings.TrimSpace(rawYAML) == "" {
		return false, "", nil, fmt.Errorf("%w: yaml content is required", ErrInvalidFlow)
	}
	wf, err = workflow.Parse([]byte(rawYAML))
	if err != nil {
		return false, "", nil, fmt.Errorf("%w: %v", ErrInvalidFlow, err)
	}

	target := filepath.Join(flowsDir, cleanName)
	if info, statErr := os.Lstat(target); os.IsNotExist(statErr) {
		created = true
	} else if statErr != nil {
		return false, "", nil, statErr
	} else if info.Mode()&os.ModeSymlink != 0 {
		return false, "", nil, fmt.Errorf("%w: symlinks are not allowed (%q)", ErrInvalidFlow, cleanName)
	} else if info.IsDir() {
		return false, "", nil, fmt.Errorf("%w: %q is a directory", ErrInvalidFlow, cleanName)
	}

	if err := writeFileAtomic(target, []byte(rawYAML)); err != nil {
		return false, "", nil, err
	}
	return created, cleanName, wf, nil
}

// DeleteFlow safely removes a workflow YAML file from flowsDir.
func DeleteFlow(flowsDir, fileName string) (string, error) {
	cleanName, err := validateFlatFlowFileName(fileName)
	if err != nil {
		return "", err
	}
	target := filepath.Join(flowsDir, cleanName)
	info, err := os.Lstat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("%w: %q", ErrFlowNotFound, cleanName)
		}
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: symlinks are not allowed (%q)", ErrInvalidFlow, cleanName)
	}
	if info.IsDir() {
		return "", fmt.Errorf("%w: %q is a directory", ErrInvalidFlow, cleanName)
	}
	if err := os.Remove(target); err != nil {
		return "", err
	}
	return cleanName, nil
}

func resolveSafeIncludeWritePath(includesDir, rel string) (absRoot, absTarget, cleanSlash string, err error) {
	absRoot, err = filepath.Abs(includesDir)
	if err != nil {
		return "", "", "", err
	}
	trimmed := strings.TrimSpace(rel)
	if strings.ContainsAny(trimmed, "\\\x00") {
		return "", "", "", fmt.Errorf("%w: invalid file path %q", ErrInvalidInclude, rel)
	}
	trimmed = strings.TrimPrefix(filepath.ToSlash(trimmed), "includes/")
	cleanRel := filepath.Clean(filepath.FromSlash(trimmed))
	if trimmed == "" || filepath.IsAbs(cleanRel) || cleanRel == "." || cleanRel == ".." || strings.HasPrefix(cleanRel, ".."+string(filepath.Separator)) {
		return "", "", "", fmt.Errorf("%w: invalid file path %q", ErrInvalidInclude, rel)
	}
	cleanSlash = filepath.ToSlash(cleanRel)
	for _, seg := range strings.Split(cleanSlash, "/") {
		if seg == "" || strings.HasPrefix(seg, ".") {
			return "", "", "", fmt.Errorf("%w: hidden or dot paths (%q) are not allowed", ErrInvalidInclude, rel)
		}
	}

	absTarget = filepath.Join(absRoot, cleanRel)
	checkRel, err := filepath.Rel(absRoot, absTarget)
	if err != nil || checkRel == ".." || strings.HasPrefix(checkRel, ".."+string(filepath.Separator)) {
		return "", "", "", fmt.Errorf("%w: file path %q escapes includes directory", ErrInvalidInclude, rel)
	}
	cleanSlash = filepath.ToSlash(checkRel)
	if err := ensureNoSymlinksUnderRoot(absRoot, cleanSlash, ErrInvalidInclude); err != nil {
		return "", "", "", err
	}
	return absRoot, absTarget, cleanSlash, nil
}

func ensureNoSymlinksUnderRoot(absRoot, cleanSlash string, invalidSentinel error) error {
	realRoot, rootErr := filepath.EvalSymlinks(absRoot)
	cur := absRoot
	deepestExisting := ""
	if rootErr == nil {
		deepestExisting = realRoot
	}

	segs := strings.Split(cleanSlash, "/")
	for i, seg := range segs {
		cur = filepath.Join(cur, seg)
		fi, err := os.Lstat(cur)
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("%w: symlinks are not allowed (%q)", invalidSentinel, cleanSlash)
		}
		if i < len(segs)-1 && !fi.IsDir() {
			return fmt.Errorf("%w: parent path %q is not a directory", invalidSentinel, filepath.ToSlash(strings.Join(segs[:i+1], "/")))
		}
		if fi.IsDir() && rootErr == nil {
			resolved, evalErr := filepath.EvalSymlinks(cur)
			if evalErr != nil {
				return evalErr
			}
			deepestExisting = resolved
		}
	}

	if rootErr == nil && deepestExisting != "" && deepestExisting != realRoot && !strings.HasPrefix(deepestExisting, realRoot+string(filepath.Separator)) {
		return fmt.Errorf("%w: path %q escapes root directory via symlink", invalidSentinel, cleanSlash)
	}
	return nil
}

// SaveInclude atomically writes content under includesDir up to MaxIncludePreviewBytes.
func SaveInclude(includesDir, relPath, content string) (created bool, cleanPath string, size int64, err error) {
	if int64(len(content)) > MaxIncludePreviewBytes {
		return false, "", 0, fmt.Errorf("%w: content exceeds %d byte limit", ErrInvalidInclude, MaxIncludePreviewBytes)
	}
	_, target, cleanPath, err := resolveSafeIncludeWritePath(includesDir, relPath)
	if err != nil {
		return false, "", 0, err
	}
	if info, statErr := os.Lstat(target); os.IsNotExist(statErr) {
		created = true
	} else if statErr != nil {
		return false, "", 0, statErr
	} else if info.Mode()&os.ModeSymlink != 0 {
		return false, "", 0, fmt.Errorf("%w: symlinks are not allowed (%q)", ErrInvalidInclude, relPath)
	} else if info.IsDir() {
		return false, "", 0, fmt.Errorf("%w: %q is a directory", ErrInvalidInclude, relPath)
	}

	if err := writeFileAtomic(target, []byte(content)); err != nil {
		return false, "", 0, err
	}
	return created, cleanPath, int64(len(content)), nil
}

// DeleteInclude safely removes a shared file inside includesDir.
func DeleteInclude(includesDir, relPath string) (string, error) {
	_, target, cleanPath, err := resolveSafeIncludeWritePath(includesDir, relPath)
	if err != nil {
		return "", err
	}
	info, err := os.Lstat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("%w: %q", ErrIncludeNotFound, relPath)
		}
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: symlinks are not allowed (%q)", ErrInvalidInclude, relPath)
	}
	if info.IsDir() {
		return "", fmt.Errorf("%w: %q is a directory", ErrInvalidInclude, relPath)
	}
	if err := os.Remove(target); err != nil {
		return "", err
	}
	return cleanPath, nil
}
