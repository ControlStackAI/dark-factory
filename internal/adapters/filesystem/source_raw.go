//go:build linux

package filesystem

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

type treeEntry struct {
	mode string
	oid  string
}

type rawEntry struct {
	mode string
	oid  string
	size int64
	data []byte
}

type rawWorktree struct {
	head    map[string]treeEntry
	entries map[string]rawEntry
}

func (r rawWorktree) changedPaths() []string {
	seen := make(map[string]bool, len(r.head)+len(r.entries))
	for path := range r.head {
		seen[path] = true
	}
	for path := range r.entries {
		seen[path] = true
	}
	changed := make([]string, 0, len(seen))
	for path := range seen {
		head, inHead := r.head[path]
		entry, present := r.entries[path]
		if !inHead || !present || head.mode != entry.mode || head.oid != entry.oid {
			changed = append(changed, path)
		}
	}
	sort.Strings(changed)
	return changed
}

func (r rawWorktree) equal(other rawWorktree) bool {
	if len(r.head) != len(other.head) || len(r.entries) != len(other.entries) {
		return false
	}
	for path, entry := range r.head {
		if other.head[path] != entry {
			return false
		}
	}
	for path, entry := range r.entries {
		candidate, ok := other.entries[path]
		if !ok || entry.mode != candidate.mode || entry.oid != candidate.oid || entry.size != candidate.size {
			return false
		}
	}
	return true
}

func captureRawWorktree(ctx context.Context, workspace, commit, objectFormat string, maxBytes int64, retainChanged bool) (rawWorktree, error) {
	headOutput, err := gitOutputLimit(ctx, workspace, false, maxGitMetadataBytes, nil, nil, "ls-tree", "-rz", "--full-tree", commit)
	if err != nil {
		return rawWorktree{}, fmt.Errorf("list HEAD tree: %w", err)
	}
	head, err := parseTreeEntries(headOutput, objectFormat)
	if err != nil {
		return rawWorktree{}, err
	}
	indexOutput, err := gitOutputLimit(ctx, workspace, false, maxGitMetadataBytes, nil, nil, "ls-files", "-s", "-z", "--")
	if err != nil {
		return rawWorktree{}, fmt.Errorf("list index entries: %w", err)
	}
	index, err := parseIndexEntries(indexOutput, objectFormat)
	if err != nil {
		return rawWorktree{}, err
	}
	tags, err := gitOutputLimit(ctx, workspace, false, maxGitMetadataBytes, nil, nil, "ls-files", "-t", "-z", "--")
	if err != nil {
		return rawWorktree{}, fmt.Errorf("inspect index flags: %w", err)
	}
	if err := rejectUnusualIndexFlags(tags); err != nil {
		return rawWorktree{}, err
	}
	untrackedOutput, err := gitOutputLimit(ctx, workspace, false, maxGitMetadataBytes, nil, nil, "ls-files", "--others", "-z", "--")
	if err != nil {
		return rawWorktree{}, fmt.Errorf("list untracked paths: %w", err)
	}
	tracked := make(map[string]bool, len(head)+len(index))
	for path := range head {
		tracked[path] = true
	}
	for path := range index {
		tracked[path] = true
	}
	walkedUntracked, err := enumerateRawUntracked(workspace, tracked)
	if err != nil {
		return rawWorktree{}, err
	}
	untracked, err := mergeNULPaths(untrackedOutput)
	if err != nil {
		return rawWorktree{}, err
	}
	untracked, err = mergePathLists(untracked, walkedUntracked)
	if err != nil {
		return rawWorktree{}, err
	}
	paths := make(map[string]bool, len(head)+len(index)+len(untracked))
	for path := range head {
		paths[path] = true
	}
	for path := range index {
		paths[path] = true
	}
	for _, path := range untracked {
		paths[path] = true
	}
	if len(paths) > maxSourcePaths {
		return rawWorktree{}, fmt.Errorf("raw source path count exceeds %d", maxSourcePaths)
	}
	ordered := make([]string, 0, len(paths))
	for path := range paths {
		if !safeRelativePath(path) {
			return rawWorktree{}, fmt.Errorf("Git returned unsafe source path %q", path)
		}
		ordered = append(ordered, path)
	}
	sort.Strings(ordered)
	rootFD, err := openDirectoryNoSymlinks(workspace)
	if err != nil {
		return rawWorktree{}, fmt.Errorf("anchor workspace: %w", err)
	}
	defer unix.Close(rootFD)
	entries := make(map[string]rawEntry, len(ordered))
	var changedBytes int64
	var scannedBytes int64
	maxScanned := maxBytes * 16
	if maxScanned < maxBytes || maxScanned > 1<<30 {
		maxScanned = 1 << 30
	}
	untrackedSet := make(map[string]bool, len(untracked))
	for _, path := range untracked {
		untrackedSet[path] = true
	}
	for _, path := range ordered {
		entry, present, readErr := readRawPath(rootFD, path, maxBytes, !untrackedSet[path])
		if readErr != nil {
			return rawWorktree{}, readErr
		}
		if !present {
			continue
		}
		entry.oid, err = hashGitBlob(objectFormat, entry.data)
		if err != nil {
			return rawWorktree{}, err
		}
		scannedBytes += entry.size
		if scannedBytes < 0 || scannedBytes > maxScanned {
			return rawWorktree{}, fmt.Errorf("raw tracked scan exceeds %d bytes", maxScanned)
		}
		baseline, inHead := head[path]
		if !inHead || baseline.mode != entry.mode || baseline.oid != entry.oid {
			changedBytes += entry.size
			if changedBytes < 0 || changedBytes > maxBytes {
				return rawWorktree{}, fmt.Errorf("raw changed payloads exceed %d bytes", maxBytes)
			}
			if !retainChanged {
				entry.data = nil
			}
		} else {
			entry.data = nil
		}
		entries[path] = entry
	}
	return rawWorktree{head: head, entries: entries}, nil
}

func enumerateRawUntracked(workspace string, tracked map[string]bool) ([]string, error) {
	paths := make([]string, 0)
	err := filepath.WalkDir(workspace, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return fmt.Errorf("walk raw source: %w", walkErr)
		}
		if path == workspace {
			return nil
		}
		relative, err := filepath.Rel(workspace, path)
		if err != nil {
			return err
		}
		relative = filepath.ToSlash(relative)
		if relative == ".git" {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !safeRelativePath(relative) {
			return fmt.Errorf("filesystem returned unsafe source path %q", relative)
		}
		if filepath.Base(path) == ".git" {
			return fmt.Errorf("untracked nested repository metadata %q is unsupported", relative)
		}
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("stat raw source path %q: %w", relative, err)
		}
		if info.IsDir() {
			return nil
		}
		if tracked[relative] {
			return nil
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("untracked path %q is nonregular; symlinks, FIFOs, devices, sockets, and other unusual entries are unsupported", relative)
		}
		paths = append(paths, relative)
		if len(paths) > maxSourcePaths {
			return fmt.Errorf("raw source path count exceeds %d", maxSourcePaths)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func mergePathLists(groups ...[]string) ([]string, error) {
	seen := map[string]bool{}
	for _, group := range groups {
		for _, path := range group {
			if !safeRelativePath(path) {
				return nil, fmt.Errorf("unsafe raw source path %q", path)
			}
			seen[path] = true
		}
	}
	paths := make([]string, 0, len(seen))
	for path := range seen {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, nil
}

func parseTreeEntries(data []byte, objectFormat string) (map[string]treeEntry, error) {
	entries := map[string]treeEntry{}
	for _, record := range splitNUL(data) {
		metadata, path, ok := strings.Cut(record, "\t")
		fields := strings.Fields(metadata)
		if !ok || len(fields) != 3 || !safeRelativePath(path) {
			return nil, errors.New("Git returned a malformed HEAD tree entry")
		}
		mode, kind, oid := fields[0], fields[1], fields[2]
		if mode == "160000" || kind == "commit" {
			return nil, fmt.Errorf("tracked submodule %q is unsupported by raw source evidence", path)
		}
		if (mode != "100644" && mode != "100755" && mode != "120000") || kind != "blob" {
			return nil, fmt.Errorf("tracked entry %q has unsupported mode/type %s/%s", path, mode, kind)
		}
		if err := validateObjectIdentity(objectFormat, oid); err != nil {
			return nil, fmt.Errorf("HEAD entry %q: %w", path, err)
		}
		if _, duplicate := entries[path]; duplicate {
			return nil, errors.New("Git returned duplicate HEAD tree entries")
		}
		entries[path] = treeEntry{mode: mode, oid: oid}
	}
	return entries, nil
}

func parseIndexEntries(data []byte, objectFormat string) (map[string]treeEntry, error) {
	entries := map[string]treeEntry{}
	for _, record := range splitNUL(data) {
		metadata, path, ok := strings.Cut(record, "\t")
		fields := strings.Fields(metadata)
		if !ok || len(fields) != 3 || fields[2] != "0" || !safeRelativePath(path) {
			return nil, errors.New("unmerged or malformed index entries are unsupported by raw source evidence")
		}
		mode, oid := fields[0], fields[1]
		if mode == "160000" {
			return nil, fmt.Errorf("indexed submodule %q is unsupported by raw source evidence", path)
		}
		if mode != "100644" && mode != "100755" && mode != "120000" {
			return nil, fmt.Errorf("index entry %q has unsupported mode %s", path, mode)
		}
		if err := validateObjectIdentity(objectFormat, oid); err != nil {
			return nil, fmt.Errorf("index entry %q: %w", path, err)
		}
		if _, duplicate := entries[path]; duplicate {
			return nil, errors.New("Git returned duplicate index entries")
		}
		entries[path] = treeEntry{mode: mode, oid: oid}
	}
	return entries, nil
}

func rejectUnusualIndexFlags(data []byte) error {
	for _, record := range splitNUL(data) {
		if len(record) < 3 || record[1] != ' ' || !safeRelativePath(record[2:]) {
			return errors.New("Git returned malformed index flags")
		}
		if record[0] != 'H' {
			return fmt.Errorf("index flag %q for %q is unsupported; sparse/assume-unchanged entries fail closed", record[0], record[2:])
		}
	}
	return nil
}

func readRawPath(rootFD int, path string, maxBytes int64, allowSymlink bool) (rawEntry, bool, error) {
	parts := strings.Split(path, "/")
	current, err := unix.Dup(rootFD)
	if err != nil {
		return rawEntry{}, false, err
	}
	defer func() { _ = unix.Close(current) }()
	for _, part := range parts[:len(parts)-1] {
		next, openErr := unix.Openat(current, part, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(openErr, unix.ENOENT) {
			return rawEntry{}, false, nil
		}
		if openErr != nil {
			return rawEntry{}, false, fmt.Errorf("raw source path %q has a non-directory or symlink ancestor: %w", path, openErr)
		}
		_ = unix.Close(current)
		current = next
	}
	name := parts[len(parts)-1]
	var before unix.Stat_t
	if err := unix.Fstatat(current, name, &before, unix.AT_SYMLINK_NOFOLLOW); errors.Is(err, unix.ENOENT) {
		return rawEntry{}, false, nil
	} else if err != nil {
		return rawEntry{}, false, fmt.Errorf("stat raw source path %q: %w", path, err)
	}
	switch before.Mode & unix.S_IFMT {
	case unix.S_IFLNK:
		if !allowSymlink {
			return rawEntry{}, false, fmt.Errorf("untracked path %q is a symlink; nonregular untracked paths are unsupported", path)
		}
		buffer := make([]byte, 4097)
		n, err := unix.Readlinkat(current, name, buffer)
		if err != nil {
			return rawEntry{}, false, fmt.Errorf("read tracked symlink %q: %w", path, err)
		}
		if n == len(buffer) {
			return rawEntry{}, false, fmt.Errorf("tracked symlink %q target exceeds limit", path)
		}
		var after unix.Stat_t
		if err := unix.Fstatat(current, name, &after, unix.AT_SYMLINK_NOFOLLOW); err != nil || !sameStat(before, after) {
			return rawEntry{}, false, fmt.Errorf("tracked symlink %q changed while it was read", path)
		}
		data := append([]byte(nil), buffer[:n]...)
		return rawEntry{mode: "120000", size: int64(len(data)), data: data}, true, nil
	case unix.S_IFREG:
		if !allowSymlink && before.Nlink != 1 {
			return rawEntry{}, false, fmt.Errorf("untracked path %q must be a singly linked regular file", path)
		}
		fd, err := unix.Openat(current, name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return rawEntry{}, false, fmt.Errorf("open raw source path %q: %w", path, err)
		}
		file := os.NewFile(uintptr(fd), path)
		defer file.Close()
		var opened unix.Stat_t
		if err := unix.Fstat(fd, &opened); err != nil || !sameStat(before, opened) {
			return rawEntry{}, false, fmt.Errorf("raw source path %q changed before read", path)
		}
		if opened.Size < 0 || opened.Size > maxBytes {
			return rawEntry{}, false, fmt.Errorf("raw source path %q exceeds %d bytes", path, maxBytes)
		}
		data, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
		if err != nil {
			return rawEntry{}, false, fmt.Errorf("read raw source path %q: %w", path, err)
		}
		if int64(len(data)) > maxBytes {
			return rawEntry{}, false, fmt.Errorf("raw source path %q exceeds %d bytes", path, maxBytes)
		}
		var after unix.Stat_t
		if err := unix.Fstat(fd, &after); err != nil || !sameStat(opened, after) || int64(len(data)) != after.Size {
			return rawEntry{}, false, fmt.Errorf("raw source path %q changed while it was read", path)
		}
		mode := "100644"
		if after.Mode&0o111 != 0 {
			mode = "100755"
		}
		return rawEntry{mode: mode, size: int64(len(data)), data: data}, true, nil
	default:
		kind := "nonregular"
		if before.Mode&unix.S_IFMT == unix.S_IFDIR {
			kind = "directory or nested repository"
		}
		return rawEntry{}, false, fmt.Errorf("source path %q is a %s; FIFOs, devices, sockets, directories, and other unusual entries are unsupported", path, kind)
	}
}

func validateObjectIdentity(format, oid string) error {
	want := 0
	switch format {
	case "sha1":
		want = sha1.Size * 2
	case "sha256":
		want = sha256.Size * 2
	default:
		return fmt.Errorf("unsupported Git object format %q; only sha1 and sha256 are supported", format)
	}
	if len(oid) != want {
		return fmt.Errorf("Git object ID length %d does not match %s", len(oid), format)
	}
	for _, char := range oid {
		if !strings.ContainsRune("0123456789abcdef", char) {
			return errors.New("Git returned a non-lowercase-hex object ID")
		}
	}
	return nil
}

func hashGitBlob(format string, data []byte) (string, error) {
	header := []byte("blob " + strconv.Itoa(len(data)) + "\x00")
	switch format {
	case "sha1":
		hash := sha1.New()
		_, _ = hash.Write(header)
		_, _ = hash.Write(data)
		return fmt.Sprintf("%x", hash.Sum(nil)), nil
	case "sha256":
		hash := sha256.New()
		_, _ = hash.Write(header)
		_, _ = hash.Write(data)
		return fmt.Sprintf("%x", hash.Sum(nil)), nil
	default:
		return "", fmt.Errorf("unsupported Git object format %q", format)
	}
}

type rawGitState struct {
	workspace string
	root      string
	index     string
	objects   string
	worktree  string
	env       []string
	commit    string
	format    string
}

func newRawGitState(ctx context.Context, workspace, commit, format string) (*rawGitState, error) {
	root, err := os.MkdirTemp("", "dark-factory-raw-git-")
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		_ = os.RemoveAll(root)
		return nil, err
	}
	state := &rawGitState{workspace: workspace, root: root, index: filepath.Join(root, "index"), objects: filepath.Join(root, "objects"), worktree: filepath.Join(root, "worktree"), commit: commit, format: format}
	for _, directory := range []string{state.objects, filepath.Join(state.objects, "info"), filepath.Join(state.objects, "pack"), state.worktree} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			state.Close()
			return nil, err
		}
	}
	objectPath, err := gitPath(ctx, workspace, "objects")
	if err != nil {
		state.Close()
		return nil, err
	}
	if strings.ContainsAny(objectPath, string(os.PathListSeparator)+"\n\r") {
		state.Close()
		return nil, errors.New("repository object path cannot be represented safely as an alternate")
	}
	state.env = []string{"GIT_INDEX_FILE=" + state.index, "GIT_OBJECT_DIRECTORY=" + state.objects, "GIT_ALTERNATE_OBJECT_DIRECTORIES=" + objectPath, "GIT_WORK_TREE=" + state.worktree}
	return state, nil
}

func (s *rawGitState) Close() error {
	if s == nil || s.root == "" {
		return nil
	}
	err := os.RemoveAll(s.root)
	s.root = ""
	return err
}

func (s *rawGitState) Git(ctx context.Context, maxOutput int64, stdin []byte, args ...string) ([]byte, error) {
	return gitOutputLimit(ctx, s.workspace, false, maxOutput, s.env, stdin, args...)
}

func (s *rawGitState) Materialize(ctx context.Context, snapshot rawWorktree) error {
	if _, err := s.Git(ctx, 4096, nil, "read-tree", s.commit); err != nil {
		return fmt.Errorf("initialize raw source index: %w", err)
	}
	zeroOID := strings.Repeat("0", sha1.Size*2)
	if s.format == "sha256" {
		zeroOID = strings.Repeat("0", sha256.Size*2)
	}
	paths := snapshot.changedPaths()
	var records bytes.Buffer
	for _, path := range paths {
		entry, present := snapshot.entries[path]
		if !present {
			fmt.Fprintf(&records, "0 %s\t%s%c", zeroOID, path, byte(0))
			continue
		}
		oidOutput, err := s.Git(ctx, 256, entry.data, "hash-object", "-w", "--stdin")
		if err != nil {
			return fmt.Errorf("materialize raw source path %q: %w", path, err)
		}
		oid := strings.TrimSpace(string(oidOutput))
		if oid != entry.oid {
			return fmt.Errorf("Git object hash for %q differs from independent %s hash", path, s.format)
		}
		fmt.Fprintf(&records, "%s %s\t%s%c", entry.mode, entry.oid, path, byte(0))
	}
	if records.Len() > int(maxGitMetadataBytes) {
		return errors.New("raw source index update exceeds metadata limit")
	}
	if len(paths) != 0 {
		if _, err := s.Git(ctx, 4096, records.Bytes(), "update-index", "-z", "--index-info"); err != nil {
			return fmt.Errorf("materialize raw source index: %w", err)
		}
	}
	return nil
}
