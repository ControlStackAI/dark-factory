// Package openclaw implements the bounded argv-only OpenClaw CLI adapter.
package openclaw

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"golang.org/x/sys/unix"
)

const resultVersion = 1

type PromptBuilder func(domain.TurnRequest) (string, error)

type Options struct {
	Executable       string
	Agent            string
	SessionPrefix    string
	Timeout          time.Duration
	ShutdownTimeout  time.Duration
	PromptRoot       string
	ArtifactRoot     string
	MaxOutputBytes   int64
	MaxArtifactBytes int64
	MaxArtifacts     int
	PromptBuilder    PromptBuilder
	StripEnvironment []string
}

type Executor struct {
	options Options
}

// ExecutionError contains only bounded, redacted diagnostics. It never includes stdout,
// the prompt, an environment value, or the prompt file path.
type ExecutionError struct {
	Kind       string
	Diagnostic string
	Truncated  bool
	ExitCode   int
	cause      error
}

func (e *ExecutionError) Error() string {
	message := "OpenClaw " + e.Kind
	if e.ExitCode != 0 {
		message += fmt.Sprintf(" (exit %d)", e.ExitCode)
	}
	if e.Diagnostic != "" {
		message += ": " + e.Diagnostic
	}
	return message
}
func (e *ExecutionError) Unwrap() error { return e.cause }

func New(options Options) (*Executor, error) {
	if strings.TrimSpace(options.Executable) == "" || strings.ContainsAny(options.Executable, "\r\n\x00") ||
		strings.TrimSpace(options.Agent) == "" || strings.ContainsAny(options.Agent, "\r\n\x00") ||
		strings.TrimSpace(options.SessionPrefix) == "" || strings.ContainsAny(options.SessionPrefix, " \t\r\n\x00") {
		return nil, errors.New("OpenClaw executable, agent, or session prefix is invalid")
	}
	if strings.Contains(options.Executable, string(filepath.Separator)) && !filepath.IsAbs(options.Executable) {
		return nil, errors.New("OpenClaw executable path must be absolute")
	}
	for _, forbidden := range []string{"--deliver", "--reply-to", "--reply-channel", "--reply-account", "--channel", "--to"} {
		if options.Executable == forbidden || options.Agent == forbidden {
			return nil, errors.New("OpenClaw delivery and routing flags are forbidden")
		}
	}
	if options.Timeout <= 0 || options.Timeout > 24*time.Hour || options.ShutdownTimeout <= 0 || options.ShutdownTimeout > time.Minute {
		return nil, errors.New("OpenClaw timeout bounds are invalid")
	}
	if options.MaxOutputBytes < 1024 || options.MaxOutputBytes > 16<<20 || options.MaxArtifactBytes < options.MaxOutputBytes || options.MaxArtifactBytes > 1<<30 || options.MaxArtifacts < 1 || options.MaxArtifacts > 10000 {
		return nil, errors.New("OpenClaw output/artifact bounds are invalid")
	}
	if options.PromptBuilder == nil {
		options.PromptBuilder = defaultPrompt
	}
	for _, name := range options.StripEnvironment {
		if name == "" || strings.ContainsAny(name, "=\x00") {
			return nil, errors.New("invalid environment name to strip")
		}
	}
	for _, root := range []string{options.PromptRoot, options.ArtifactRoot} {
		if !filepath.IsAbs(root) {
			return nil, errors.New("OpenClaw roots must be absolute")
		}
		anchored, err := openPrivateRoot(root)
		if err != nil {
			return nil, err
		}
		_ = anchored.Close()
	}
	return &Executor{options: options}, nil
}

func (e *Executor) ExecuteTurn(ctx context.Context, request domain.TurnRequest) (domain.TurnResult, error) {
	if request.RunID == "" || request.ProjectID == "" || request.IssueID == "" || request.Attempt < 1 || request.Fence == 0 || request.LeaseUntil.IsZero() {
		return domain.TurnResult{}, errors.New("invalid OpenClaw turn request")
	}
	prompt, err := e.options.PromptBuilder(request)
	if err != nil || strings.TrimSpace(prompt) == "" {
		return domain.TurnResult{}, errors.New("build OpenClaw prompt failed")
	}
	promptRoot, err := openPrivateRoot(e.options.PromptRoot)
	if err != nil {
		return domain.TurnResult{}, fmt.Errorf("anchor prompt root: %w", err)
	}
	defer promptRoot.Close()
	promptName, err := createPrivateFile(promptRoot, ".openclaw-prompt-", []byte(prompt))
	if err != nil {
		return domain.TurnResult{}, fmt.Errorf("create private prompt: %w", err)
	}
	defer func() { _ = unix.Unlinkat(int(promptRoot.Fd()), promptName, 0) }()

	sessionKey := sessionKey(e.options.SessionPrefix, request)
	effectiveTimeout, err := e.timeoutWithinLease(request.LeaseUntil)
	if err != nil {
		return domain.TurnResult{}, err
	}
	timeoutSeconds := int(math.Ceil(effectiveTimeout.Seconds()))
	args := []string{"agent", "--agent", e.options.Agent, "--session-key", sessionKey,
		"--message-file", "/proc/self/fd/3/" + promptName, "--json", "--timeout", strconv.Itoa(timeoutSeconds)}
	command := exec.Command(e.options.Executable, args...)
	command.Env = strippedEnvironment(os.Environ(), e.options.StripEnvironment)
	command.ExtraFiles = []*os.File{promptRoot}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout := newBoundedCapture(e.options.MaxOutputBytes)
	stderr := newBoundedCapture(e.options.MaxOutputBytes)
	command.Stdout = stdout
	command.Stderr = stderr
	if err := command.Start(); err != nil {
		return domain.TurnResult{}, executionFailure("failed to start", err, stderr)
	}

	processCtx, cancel := context.WithTimeout(ctx, effectiveTimeout)
	defer cancel()
	wait := make(chan error, 1)
	go func() { wait <- command.Wait() }()
	waitErr, timedOut := waitProcess(processCtx, command.Process.Pid, wait, e.options.ShutdownTimeout)
	// The adapter owns the process group. Remove any descendant that outlived the CLI.
	_ = unix.Kill(-command.Process.Pid, unix.SIGKILL)
	if timedOut {
		return domain.TurnResult{}, executionFailure("timed out or was canceled", processCtx.Err(), stderr)
	}
	if waitErr != nil {
		failure := executionFailure("process failed", waitErr, stderr)
		var exitError *exec.ExitError
		if errors.As(waitErr, &exitError) {
			failure.ExitCode = exitError.ExitCode()
		}
		return domain.TurnResult{}, failure
	}
	if stdout.Truncated() {
		return domain.TurnResult{}, &ExecutionError{Kind: "stdout exceeded configured bound", Truncated: true}
	}

	response := stdout.Bytes()
	responseRef, responseDigest, err := snapshotResponse(e.options.ArtifactRoot, request, sessionKey, response, e.options.MaxArtifactBytes, e.options.MaxArtifacts)
	if err != nil {
		return domain.TurnResult{}, fmt.Errorf("snapshot OpenClaw response: %w", err)
	}
	parsed, err := parseResponse(response)
	if err != nil {
		return domain.TurnResult{}, &ExecutionError{Kind: "returned an invalid versioned result", cause: err}
	}
	return domain.TurnResult{Step: parsed.Step, Evidence: parsed.Evidence, SessionKey: sessionKey, ResponseRef: responseRef, ResponseSHA256: responseDigest}, nil
}

func (e *Executor) timeoutWithinLease(leaseUntil time.Time) (time.Duration, error) {
	remaining := time.Until(leaseUntil)
	margin := remaining / 100
	if margin < time.Millisecond {
		margin = time.Millisecond
	}
	if margin > time.Second {
		margin = time.Second
	}
	available := remaining - e.options.ShutdownTimeout - margin
	if available <= 0 {
		return 0, &ExecutionError{Kind: "remaining lease cannot safely contain an OpenClaw process and bounded shutdown"}
	}
	if available < e.options.Timeout {
		return available, nil
	}
	return e.options.Timeout, nil
}

func defaultPrompt(request domain.TurnRequest) (string, error) {
	return fmt.Sprintf(`Dark Factory controller turn.
Run: %s
Project: %s
Issue: %s
Attempt: %d
Fence: %d

Perform one bounded unit of work. Do not deliver or send messages. Your final response must be exactly one JSON object with no Markdown fences and this schema:
{"result_version":1,"step":"concrete completed step","evidence":"concrete artifact, test, commit, or observed result"}
`, request.RunID, request.ProjectID, request.IssueID, request.Attempt, request.Fence), nil
}

type versionedResult struct {
	ResultVersion int    `json:"result_version"`
	Step          string `json:"step"`
	Evidence      string `json:"evidence"`
}

func parseResponse(raw []byte) (versionedResult, error) {
	var envelope struct {
		Status string `json:"status"`
		Result struct {
			Payloads []struct {
				Text string `json:"text"`
			} `json:"payloads"`
		} `json:"result"`
	}
	if err := decodeOne(raw, &envelope, false); err != nil || envelope.Status != "ok" || len(envelope.Result.Payloads) != 1 || envelope.Result.Payloads[0].Text == "" {
		return versionedResult{}, errors.New("invalid OpenClaw JSON envelope")
	}
	var result versionedResult
	if err := decodeOne([]byte(envelope.Result.Payloads[0].Text), &result, true); err != nil {
		return versionedResult{}, err
	}
	if result.ResultVersion != resultVersion || strings.TrimSpace(result.Step) != result.Step || strings.TrimSpace(result.Evidence) != result.Evidence || len(result.Step) < 2 || len(result.Evidence) < 8 {
		return versionedResult{}, errors.New("invalid OpenClaw result fields or version")
	}
	return result, nil
}

func decodeOne(raw []byte, target any, strict bool) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	if strict {
		decoder.DisallowUnknownFields()
	}
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values")
	}
	return nil
}

func waitProcess(ctx context.Context, pid int, wait <-chan error, grace time.Duration) (error, bool) {
	select {
	case err := <-wait:
		return err, false
	case <-ctx.Done():
		// Resolve success-at-deadline deterministically when Wait has already completed.
		select {
		case err := <-wait:
			return err, false
		default:
		}
		_ = unix.Kill(-pid, unix.SIGTERM)
		timer := time.NewTimer(grace)
		defer timer.Stop()
		select {
		case <-wait:
			return ctx.Err(), true
		case <-timer.C:
			_ = unix.Kill(-pid, unix.SIGKILL)
			<-wait
			return ctx.Err(), true
		}
	}
}

type boundedCapture struct {
	maximum   int64
	contents  []byte
	written   int64
	truncated bool
}

func newBoundedCapture(maximum int64) *boundedCapture { return &boundedCapture{maximum: maximum} }
func (b *boundedCapture) Write(p []byte) (int, error) {
	b.written += int64(len(p))
	remaining := b.maximum - int64(len(b.contents))
	if remaining > 0 {
		keep := int64(len(p))
		if keep > remaining {
			keep = remaining
		}
		b.contents = append(b.contents, p[:keep]...)
	}
	if b.written > b.maximum {
		b.truncated = true
	}
	return len(p), nil
}
func (b *boundedCapture) Bytes() []byte   { return append([]byte(nil), b.contents...) }
func (b *boundedCapture) Truncated() bool { return b.truncated }

func executionFailure(kind string, cause error, stderr *boundedCapture) *ExecutionError {
	diagnostic := ""
	if stderr.written > 0 {
		diagnostic = fmt.Sprintf("stderr captured (%d bytes; content withheld)", stderr.written)
	}
	return &ExecutionError{Kind: kind, Diagnostic: diagnostic, Truncated: stderr.Truncated(), cause: cause}
}

func sessionKey(prefix string, request domain.TurnRequest) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%d", prefix, request.RunID, request.Attempt, request.Fence)))
	return prefix + "-" + hex.EncodeToString(digest[:16])
}

func strippedEnvironment(environment, names []string) []string {
	strip := make(map[string]bool, len(names))
	for _, name := range names {
		strip[name] = true
	}
	result := make([]string, 0, len(environment))
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if !strip[name] {
			result = append(result, entry)
		}
	}
	return result
}

func openPrivateRoot(path string) (*os.File, error) {
	before, err := os.Lstat(path)
	if err != nil || !before.IsDir() || before.Mode().Perm() != 0o700 || before.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("root %q is not a private 0700 directory", path)
	}
	how := &unix.OpenHow{Flags: unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_DIRECTORY, Resolve: unix.RESOLVE_NO_SYMLINKS}
	fd, err := unix.Openat2(unix.AT_FDCWD, path, how)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	after, err := file.Stat()
	stat, statOK := after.Sys().(*syscall.Stat_t)
	if err != nil || !os.SameFile(before, after) || !statOK || int(stat.Uid) != os.Geteuid() {
		_ = file.Close()
		return nil, errors.New("private root changed while being anchored")
	}
	return file, nil
}

func createPrivateFile(root *os.File, prefix string, contents []byte) (string, error) {
	for range 100 {
		var random [16]byte
		if _, err := rand.Read(random[:]); err != nil {
			return "", err
		}
		name := prefix + hex.EncodeToString(random[:])
		fd, err := unix.Openat(int(root.Fd()), name, unix.O_WRONLY|unix.O_CREAT|unix.O_EXCL|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
		if errors.Is(err, syscall.EEXIST) {
			continue
		}
		if err != nil {
			return "", err
		}
		file := os.NewFile(uintptr(fd), name)
		_, writeErr := file.Write(contents)
		if writeErr == nil {
			writeErr = file.Sync()
		}
		if closeErr := file.Close(); writeErr == nil {
			writeErr = closeErr
		}
		if writeErr != nil {
			_ = unix.Unlinkat(int(root.Fd()), name, 0)
			return "", writeErr
		}
		return name, nil
	}
	return "", errors.New("could not allocate a private file")
}

func snapshotResponse(rootPath string, request domain.TurnRequest, sessionKey string, contents []byte, maximum int64, maxArtifacts int) (string, string, error) {
	if int64(len(contents)) > maximum {
		return "", "", errors.New("response exceeds artifact bound")
	}
	root, err := openPrivateRoot(rootPath)
	if err != nil {
		return "", "", err
	}
	defer root.Close()
	digest := fmt.Sprintf("%x", sha256.Sum256(contents))
	identity := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%s", request.RunID, request.Attempt, sessionKey)))
	finalName := fmt.Sprintf("openclaw-%x-%s.json", identity[:12], digest)
	entries, err := os.ReadDir(rootPath)
	if err != nil {
		return "", "", err
	}
	count := 0
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "openclaw-") && strings.HasSuffix(entry.Name(), ".json") {
			count++
		}
	}
	if count >= maxArtifacts {
		if _, err := readRegularAt(root, finalName, maximum); err != nil {
			return "", "", errors.New("response artifact count bound exhausted")
		}
	}
	tempName, err := createPrivateFile(root, ".openclaw-response-", contents)
	if err != nil {
		return "", "", err
	}
	defer func() { _ = unix.Unlinkat(int(root.Fd()), tempName, 0) }()
	if err := unix.Renameat2(int(root.Fd()), tempName, int(root.Fd()), finalName, unix.RENAME_NOREPLACE); err != nil {
		if !errors.Is(err, syscall.EEXIST) {
			return "", "", err
		}
		existing, readErr := readRegularAt(root, finalName, maximum)
		if readErr != nil || !bytes.Equal(existing, contents) {
			return "", "", errors.New("conflicting response artifact already exists")
		}
	}
	if err := unix.Fsync(int(root.Fd())); err != nil {
		return "", "", err
	}
	after, err := os.Lstat(rootPath)
	if err != nil {
		return "", "", err
	}
	anchored, err := root.Stat()
	if err != nil || !os.SameFile(after, anchored) {
		return "", "", errors.New("artifact root changed during snapshot")
	}
	return filepath.Join(rootPath, finalName), digest, nil
}

func readRegularAt(root *os.File, name string, maximum int64) ([]byte, error) {
	fd, err := unix.Openat(int(root.Fd()), name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("existing response artifact is not a private regular file")
	}
	contents, err := io.ReadAll(io.LimitReader(file, maximum+1))
	if err != nil || int64(len(contents)) > maximum {
		return nil, errors.New("existing response artifact exceeds bound")
	}
	return contents, nil
}
