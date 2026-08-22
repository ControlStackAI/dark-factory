package filesystem

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	defaultSourceMaxBytes = int64(64 << 20)
	maxSourcePaths        = 100_000
	maxGitMetadataBytes   = int64(64 << 20)
)

type SourceState struct {
	Claim SourceClaim
	Diff  []byte
}

// InspectSource derives evidence from a bounded raw snapshot of the worktree. The snapshot
// is materialized in a temporary object database and index before Git is allowed to diff it,
// so clean/process/text filters and live-path races cannot hide or rewrite payload bytes.
func InspectSource(ctx context.Context, workspace string) (SourceState, error) {
	return inspectSourceLimited(ctx, workspace, defaultSourceMaxBytes, nil)
}

func inspectSource(ctx context.Context, workspace string, hook Hook) (SourceState, error) {
	return inspectSourceLimited(ctx, workspace, defaultSourceMaxBytes, hook)
}

func inspectSourceLimited(ctx context.Context, workspace string, maxBytes int64, hook Hook) (SourceState, error) {
	if maxBytes <= 0 || maxBytes > 1<<30 {
		return SourceState{}, errors.New("source evidence byte limit is invalid")
	}
	abs, err := filepath.Abs(workspace)
	if err != nil {
		return SourceState{}, fmt.Errorf("make workspace absolute: %w", err)
	}
	info, err := os.Lstat(abs)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return SourceState{}, errors.New("workspace is not a real directory")
	}
	commitBytes, err := gitOutputLimit(ctx, abs, false, 256, nil, nil, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return SourceState{}, fmt.Errorf("resolve source commit: %w", err)
	}
	commit := strings.TrimSpace(string(commitBytes))
	formatBytes, err := gitOutputLimit(ctx, abs, false, 32, nil, nil, "rev-parse", "--show-object-format")
	if err != nil {
		return SourceState{}, fmt.Errorf("resolve repository object format: %w", err)
	}
	objectFormat := strings.TrimSpace(string(formatBytes))
	if err := validateObjectIdentity(objectFormat, commit); err != nil {
		return SourceState{}, err
	}

	guardPaths := []string{"info/attributes", "config", "config.worktree"}
	guards := make([]*pathMutationGuard, 0, len(guardPaths))
	seenGuard := map[string]bool{}
	for _, name := range guardPaths {
		path, pathErr := gitPath(ctx, abs, name)
		if pathErr != nil {
			closeMutationGuards(guards)
			return SourceState{}, fmt.Errorf("resolve Git %s path: %w", name, pathErr)
		}
		if seenGuard[path] {
			continue
		}
		seenGuard[path] = true
		guard, guardErr := guardMutationPath(path, name == "info/attributes")
		if guardErr != nil {
			closeMutationGuards(guards)
			return SourceState{}, guardErr
		}
		guards = append(guards, guard)
		if name != "info/attributes" {
			if includeErr := rejectConfigIncludes(ctx, abs, path); includeErr != nil {
				closeMutationGuards(guards)
				return SourceState{}, includeErr
			}
		}
	}
	defer closeMutationGuards(guards)

	temporary, err := newRawGitState(ctx, abs, commit, objectFormat)
	if err != nil {
		return SourceState{}, err
	}
	defer temporary.Close()
	first, err := captureRawWorktree(ctx, abs, commit, objectFormat, maxBytes, true)
	if err != nil {
		return SourceState{}, err
	}
	if err := temporary.Materialize(ctx, first); err != nil {
		return SourceState{}, err
	}
	if hook != nil {
		if err := hook("before_diff"); err != nil {
			return SourceState{}, err
		}
	}
	changedBytes, err := temporary.Git(ctx, maxGitMetadataBytes, nil, "diff", "--cached", "--name-only", "-z", "--no-ext-diff", "--no-textconv", "--no-renames", commit, "--")
	if err != nil {
		return SourceState{}, fmt.Errorf("list raw source changes: %w", err)
	}
	changed, err := mergeNULPaths(changedBytes)
	if err != nil {
		return SourceState{}, err
	}
	if len(changed) > maxSourcePaths {
		return SourceState{}, fmt.Errorf("raw source path count exceeds %d", maxSourcePaths)
	}
	diff, err := temporary.Git(ctx, maxBytes, nil, "diff", "--cached", "--binary", "--full-index", "--no-ext-diff", "--no-textconv", "--no-renames", commit, "--")
	if err != nil {
		return SourceState{}, fmt.Errorf("compute raw source diff: %w", err)
	}
	if !equalStrings(changed, first.changedPaths()) {
		return SourceState{}, errors.New("raw source changes do not match emitted diff membership")
	}
	if hook != nil {
		if err := hook("after_diff"); err != nil {
			return SourceState{}, err
		}
	}
	second, err := captureRawWorktree(ctx, abs, commit, objectFormat, maxBytes, false)
	if err != nil {
		return SourceState{}, err
	}
	if !first.equal(second) {
		return SourceState{}, errors.New("raw source changed during source inspection")
	}
	finalCommit, err := gitOutputLimit(ctx, abs, false, 256, nil, nil, "rev-parse", "--verify", "HEAD^{commit}")
	if err != nil || strings.TrimSpace(string(finalCommit)) != commit {
		return SourceState{}, errors.New("HEAD changed during source inspection")
	}
	for _, guard := range guards {
		if err := guard.Verify(); err != nil {
			return SourceState{}, err
		}
	}
	return SourceState{Claim: SourceClaim{Commit: commit, DiffSHA256: digest(diff), ChangedFiles: changed}, Diff: diff}, nil
}

func gitOutput(ctx context.Context, workspace string, allowDiffExit bool, args ...string) ([]byte, error) {
	return gitOutputLimit(ctx, workspace, allowDiffExit, maxGitMetadataBytes, nil, nil, args...)
}

func gitOutputLimit(ctx context.Context, workspace string, allowDiffExit bool, maxBytes int64, extraEnv []string, stdin []byte, args ...string) ([]byte, error) {
	if maxBytes <= 0 {
		return nil, errors.New("invalid Git output limit")
	}
	prefix := []string{
		"-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false",
		"-c", "core.autocrlf=false", "-c", "core.eol=lf", "-c", "core.safecrlf=false",
		"-c", "core.filemode=true", "-c", "diff.external=", "-c", "core.attributesfile=/dev/null",
		"-c", "diff.context=3", "-c", "diff.interHunkContext=0", "-c", "diff.noprefix=false",
		"-c", "diff.mnemonicPrefix=false", "-c", "diff.algorithm=myers", "-c", "core.abbrev=40",
		"-c", "diff.relative=false", "-c", "diff.suppressBlankEmpty=false", "-c", "color.ui=never",
	}
	command := exec.CommandContext(ctx, "git", append(prefix, args...)...)
	command.Dir = workspace
	command.Env = append(sanitizedGitEnvironment(), extraEnv...)
	if stdin != nil {
		command.Stdin = bytes.NewReader(stdin)
	}
	var stdout bytes.Buffer
	var stderr bytes.Buffer
	command.Stdout = &limitedBuffer{buffer: &stdout, remaining: maxBytes}
	command.Stderr = &limitedBuffer{buffer: &stderr, remaining: 1 << 20}
	err := command.Run()
	if errors.Is(err, errOutputLimit) {
		return nil, fmt.Errorf("git %s output exceeds %d bytes", args[0], maxBytes)
	}
	if err != nil {
		var exit *exec.ExitError
		if !(allowDiffExit && errors.As(err, &exit) && exit.ExitCode() == 1) {
			return nil, fmt.Errorf("git %s failed: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
		}
	}
	return stdout.Bytes(), nil
}

var errOutputLimit = errors.New("Git output limit exceeded")

type limitedBuffer struct {
	buffer    *bytes.Buffer
	remaining int64
}

func (w *limitedBuffer) Write(data []byte) (int, error) {
	if int64(len(data)) > w.remaining {
		if w.remaining > 0 {
			_, _ = w.buffer.Write(data[:w.remaining])
			w.remaining = 0
		}
		return 0, errOutputLimit
	}
	w.remaining -= int64(len(data))
	return w.buffer.Write(data)
}

func sanitizedGitEnvironment() []string {
	blocked := map[string]bool{
		"GIT_CONFIG_GLOBAL": true, "GIT_CONFIG_SYSTEM": true, "GIT_CONFIG_NOSYSTEM": true,
		"GIT_EXTERNAL_DIFF": true, "GIT_DIFF_OPTS": true, "GIT_INDEX_FILE": true,
		"GIT_OBJECT_DIRECTORY": true, "GIT_ALTERNATE_OBJECT_DIRECTORIES": true,
		"GIT_WORK_TREE": true, "GIT_DIR": true, "GIT_COMMON_DIR": true,
		"GIT_OPTIONAL_LOCKS": true, "GIT_ATTR_NOSYSTEM": true, "GIT_CEILING_DIRECTORIES": true,
		"GIT_DISCOVERY_ACROSS_FILESYSTEM": true, "GIT_LITERAL_PATHSPECS": true,
		"GIT_GLOB_PATHSPECS": true, "GIT_NOGLOB_PATHSPECS": true, "GIT_ICASE_PATHSPECS": true,
		"GIT_PREFIX": true, "LC_ALL": true,
	}
	result := make([]string, 0, len(os.Environ())+5)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if !blocked[name] && !strings.HasPrefix(name, "GIT_CONFIG_") {
			result = append(result, entry)
		}
	}
	return append(result, "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null", "GIT_CONFIG_NOSYSTEM=1", "GIT_OPTIONAL_LOCKS=0", "GIT_ATTR_NOSYSTEM=1", "LC_ALL=C")
}

func gitPath(ctx context.Context, workspace, path string) (string, error) {
	output, err := gitOutputLimit(ctx, workspace, false, 4096, nil, nil, "rev-parse", "--git-path", path)
	if err != nil {
		return "", err
	}
	resolved := strings.TrimSuffix(strings.TrimSuffix(string(output), "\n"), "\r")
	if resolved == "" {
		return "", errors.New("git returned an empty path")
	}
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(workspace, resolved)
	}
	return filepath.Clean(resolved), nil
}

func rejectConfigIncludes(ctx context.Context, workspace, path string) error {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect Git config includes: %w", err)
	}
	output, err := gitOutputLimit(ctx, workspace, false, 1<<20, nil, nil, "config", "--file", path, "--no-includes", "--name-only", "--list", "-z")
	if err != nil {
		return fmt.Errorf("parse guarded Git config: %w", err)
	}
	for _, key := range splitNUL(output) {
		lower := strings.ToLower(key)
		if lower == "include.path" || strings.HasPrefix(lower, "includeif.") && strings.HasSuffix(lower, ".path") {
			return errors.New("repository-local Git config includes are unsupported because included files cannot be race-guarded")
		}
	}
	return nil
}

type pathMutationGuard struct {
	fd          int
	path        string
	watchedName string
	label       string
	rejectData  bool
}

func guardMutationPath(path string, rejectData bool) (*pathMutationGuard, error) {
	watchRoot := filepath.Dir(path)
	for {
		info, err := os.Lstat(watchRoot)
		if err == nil {
			if !info.IsDir() {
				return nil, errors.New("guarded Git path ancestor is not a directory")
			}
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("inspect guarded Git path ancestor: %w", err)
		}
		parent := filepath.Dir(watchRoot)
		if parent == watchRoot {
			return nil, errors.New("guarded Git path has no existing directory ancestor")
		}
		watchRoot = parent
	}
	fd, err := unix.InotifyInit1(unix.IN_CLOEXEC | unix.IN_NONBLOCK)
	if err != nil {
		return nil, fmt.Errorf("watch guarded Git path: %w", err)
	}
	relative, err := filepath.Rel(watchRoot, path)
	if err != nil || relative == "." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		_ = unix.Close(fd)
		return nil, errors.New("guarded Git path is outside its watched ancestor")
	}
	label := "Git config"
	if rejectData {
		label = "Git info attributes"
	}
	guard := &pathMutationGuard{fd: fd, path: path, watchedName: strings.Split(relative, string(filepath.Separator))[0], label: label, rejectData: rejectData}
	mask := uint32(unix.IN_ATTRIB | unix.IN_CLOSE_WRITE | unix.IN_CREATE | unix.IN_DELETE | unix.IN_DELETE_SELF | unix.IN_MODIFY | unix.IN_MOVE_SELF | unix.IN_MOVED_FROM | unix.IN_MOVED_TO)
	if _, err := unix.InotifyAddWatch(fd, watchRoot, mask); err != nil {
		guard.Close()
		return nil, fmt.Errorf("watch guarded Git path: %w", err)
	}
	if err := guard.checkPath(); err != nil {
		guard.Close()
		return nil, err
	}
	return guard, nil
}

func (g *pathMutationGuard) checkPath() error {
	info, err := os.Lstat(g.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect guarded Git path: %w", err)
	}
	if !info.Mode().IsRegular() {
		if g.rejectData {
			return errors.New("nonempty or nonregular Git info attributes are not supported")
		}
		return errors.New("guarded Git path is nonregular")
	}
	if g.rejectData && info.Size() != 0 {
		return errors.New("nonempty or nonregular Git info attributes are not supported")
	}
	return nil
}

func (g *pathMutationGuard) Verify() error {
	if err := g.checkPath(); err != nil {
		return err
	}
	buffer := make([]byte, 64<<10)
	for {
		n, err := unix.Read(g.fd, buffer)
		if errors.Is(err, unix.EAGAIN) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("read guarded Git path watch: %w", err)
		}
		if n == 0 {
			return errors.New("guarded Git path watch closed unexpectedly")
		}
		for offset := 0; offset < n; {
			if n-offset < unix.SizeofInotifyEvent {
				return errors.New("guarded Git path watch returned a truncated event")
			}
			mask := binary.LittleEndian.Uint32(buffer[offset+4 : offset+8])
			nameLength := int(binary.LittleEndian.Uint32(buffer[offset+12 : offset+16]))
			eventLength := unix.SizeofInotifyEvent + nameLength
			if nameLength < 0 || eventLength < unix.SizeofInotifyEvent || offset+eventLength > n {
				return errors.New("guarded Git path watch returned a truncated event")
			}
			name := string(bytes.TrimRight(buffer[offset+unix.SizeofInotifyEvent:offset+eventLength], "\x00"))
			if mask&unix.IN_Q_OVERFLOW != 0 || name == "" || name == g.watchedName {
				return fmt.Errorf("%s changed during source inspection", g.label)
			}
			offset += eventLength
		}
	}
}

func (g *pathMutationGuard) Close() error {
	if g == nil || g.fd < 0 {
		return nil
	}
	err := unix.Close(g.fd)
	g.fd = -1
	return err
}

func closeMutationGuards(guards []*pathMutationGuard) {
	for _, guard := range guards {
		_ = guard.Close()
	}
}

func mergeNULPaths(groups ...[]byte) ([]string, error) {
	seen := map[string]bool{}
	for _, group := range groups {
		for _, path := range splitNUL(group) {
			if !safeRelativePath(path) {
				return nil, fmt.Errorf("git returned unsafe changed path %q", path)
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

func splitNUL(data []byte) []string {
	if len(data) == 0 {
		return nil
	}
	parts := bytes.Split(data, []byte{0})
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) != 0 {
			result = append(result, string(part))
		}
	}
	return result
}
