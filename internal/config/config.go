package config

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const Version = 1

type Config struct {
	ConfigVersion int       `json:"config_version"`
	Mode          string    `json:"mode"`
	Paths         Paths     `json:"paths"`
	Scope         Scope     `json:"scope"`
	Linear        Linear    `json:"linear"`
	OpenClaw      OpenClaw  `json:"openclaw"`
	Budgets       Budgets   `json:"budgets"`
	Lifecycle     Lifecycle `json:"lifecycle"`
	Limits        Limits    `json:"limits"`
}

type Paths struct {
	StateDB       string   `json:"state_db"`
	StateRoot     string   `json:"state_root"`
	ArtifactRoot  string   `json:"artifact_root"`
	ReviewRoot    string   `json:"review_root"`
	WorkspaceRoot string   `json:"workspace_root"`
	AllowedRoots  []string `json:"allowed_roots"`
}

type Scope struct {
	TeamID         string   `json:"team_id"`
	ProjectID      string   `json:"project_id"`
	IssueID        string   `json:"issue_id"`
	IssueAllowlist []string `json:"issue_allowlist"`
}

type Linear struct {
	Endpoint string `json:"endpoint"`
	APIKey   string `json:"api_key"`
}

type OpenClaw struct {
	Executable    string `json:"executable"`
	Agent         string `json:"agent"`
	SessionPrefix string `json:"session_prefix"`
	Model         string `json:"model"`
	Timeout       string `json:"timeout"`
	Delivery      bool   `json:"delivery"`
}

type Budgets struct {
	LeaseDuration          string `json:"lease_duration"`
	MaxAttempts            int    `json:"max_attempts"`
	MaxConsecutiveFailures int    `json:"max_consecutive_failures"`
	MaxRunDuration         string `json:"max_run_duration"`
	PollInterval           string `json:"poll_interval"`
	InitialBackoff         string `json:"initial_backoff"`
	MaxBackoff             string `json:"max_backoff"`
	ShutdownTimeout        string `json:"shutdown_timeout"`
}

type Lifecycle struct {
	Ready      string `json:"ready"`
	InProgress string `json:"in_progress"`
	Done       string `json:"done"`
}

type Limits struct {
	MaxOutputBytes   int64 `json:"max_output_bytes"`
	MaxArtifactBytes int64 `json:"max_artifact_bytes"`
	MaxArtifacts     int   `json:"max_artifacts"`
}

type LoadOptions struct {
	RequireSecret bool
}

func Default(configPath string) (Config, error) {
	path, err := filepath.Abs(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("make config path absolute: %w", err)
	}
	base := filepath.Dir(path)
	workspace, err := os.Getwd()
	if err != nil {
		return Config{}, fmt.Errorf("get workspace: %w", err)
	}
	workspace, err = filepath.Abs(workspace)
	if err != nil {
		return Config{}, err
	}
	state := filepath.Join(base, "state")
	allowedRoots := []string{base}
	if workspace != base {
		allowedRoots = append(allowedRoots, workspace)
	}
	return Config{
		ConfigVersion: Version,
		Mode:          "dry-run",
		Paths: Paths{
			StateDB: filepath.Join(state, "factory.db"), StateRoot: state,
			ArtifactRoot: filepath.Join(base, "artifacts"), ReviewRoot: filepath.Join(base, "reviews"),
			WorkspaceRoot: workspace, AllowedRoots: allowedRoots,
		},
		Scope:     Scope{TeamID: "TEAM_ID", ProjectID: "PROJECT_ID", IssueID: "ISSUE_ID"},
		Linear:    Linear{Endpoint: "https://api.linear.app/graphql", APIKey: "env:LINEAR_API_KEY"},
		OpenClaw:  OpenClaw{Executable: "openclaw", Agent: "main", SessionPrefix: "agent:main:dark-factory", Timeout: "15m", Delivery: false},
		Budgets:   Budgets{LeaseDuration: "2m", MaxAttempts: 8, MaxConsecutiveFailures: 3, MaxRunDuration: "24h", PollInterval: "5s", InitialBackoff: "1s", MaxBackoff: "1m", ShutdownTimeout: "30s"},
		Lifecycle: Lifecycle{Ready: "Ready", InProgress: "In Progress", Done: "Done"},
		Limits:    Limits{MaxOutputBytes: 1 << 20, MaxArtifactBytes: 64 << 20, MaxArtifacts: 256},
	}, nil
}

func Load(path string, opts LoadOptions) (Config, error) {
	absolute, err := filepath.Abs(path)
	if err != nil {
		return Config{}, fmt.Errorf("make config path absolute: %w", err)
	}
	b, info, err := readSecureConfig(absolute)
	if err != nil {
		return Config{}, fmt.Errorf("inspect config %q: %w", absolute, err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return Config{}, fmt.Errorf("config %q has unsafe mode %04o; require 0600 or stricter", absolute, info.Mode().Perm())
	}
	if err := checkConfigDirectory(filepath.Dir(absolute)); err != nil {
		return Config{}, fmt.Errorf("config directory: %w", err)
	}
	cfg, err := decode(b)
	if err != nil {
		return Config{}, fmt.Errorf("decode config %q: %w", absolute, err)
	}
	if err := cfg.Validate(filepath.Dir(absolute)); err != nil {
		return Config{}, err
	}
	if opts.RequireSecret {
		if _, err := ResolveSecret(cfg.Linear.APIKey); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

func decode(b []byte) (Config, error) {
	trimmed := bytes.TrimSpace(b)
	if len(trimmed) == 0 {
		return Config{}, errors.New("configuration is empty")
	}
	if trimmed[0] != '{' {
		converted, err := decodeSimpleYAML(trimmed)
		if err != nil {
			return Config{}, err
		}
		trimmed = converted
	}
	if err := rejectDuplicateJSONKeys(trimmed); err != nil {
		return Config{}, err
	}
	var cfg Config
	d := json.NewDecoder(bytes.NewReader(trimmed))
	d.DisallowUnknownFields()
	if err := d.Decode(&cfg); err != nil {
		return Config{}, err
	}
	if err := d.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple documents")
		}
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) Validate(base string) error {
	var problems []string
	add := func(ok bool, message string) {
		if !ok {
			problems = append(problems, message)
		}
	}
	add(c.ConfigVersion == Version, fmt.Sprintf("config_version must be %d", Version))
	add(c.Mode == "dry-run" || c.Mode == "live", "mode must be dry-run or live")
	for field, value := range map[string]string{
		"scope.team_id": c.Scope.TeamID, "scope.project_id": c.Scope.ProjectID, "scope.issue_id": c.Scope.IssueID,
		"linear.endpoint": c.Linear.Endpoint, "linear.api_key": c.Linear.APIKey,
		"openclaw.executable": c.OpenClaw.Executable, "openclaw.agent": c.OpenClaw.Agent, "openclaw.session_prefix": c.OpenClaw.SessionPrefix,
		"lifecycle.ready": c.Lifecycle.Ready, "lifecycle.in_progress": c.Lifecycle.InProgress, "lifecycle.done": c.Lifecycle.Done,
	} {
		add(strings.TrimSpace(value) != "", field+" is required")
		add(strings.TrimSpace(value) == value, field+" must not have leading or trailing whitespace")
		add(!strings.ContainsRune(value, 0), field+" contains a NUL byte")
	}
	endpoint, err := url.Parse(c.Linear.Endpoint)
	add(err == nil && endpoint.Scheme == "https" && endpoint.Host != "" && endpoint.User == nil, "linear.endpoint must be an HTTPS URL without user information")
	add(validSecretRef(c.Linear.APIKey), "linear.api_key must be an env:NAME reference")
	add(!c.OpenClaw.Delivery, "openclaw.delivery must be false; delivery is disabled")
	add(!strings.ContainsAny(c.OpenClaw.Executable, "\r\n"), "openclaw.executable contains a newline")
	add(!strings.ContainsRune(c.OpenClaw.Executable, filepath.Separator) || filepath.IsAbs(c.OpenClaw.Executable), "openclaw.executable must be a bare command name or absolute path")
	add(!strings.ContainsAny(c.OpenClaw.SessionPrefix, " \t\r\n"), "openclaw.session_prefix must not contain whitespace")
	add(strings.HasPrefix(c.OpenClaw.SessionPrefix, "agent:"+c.OpenClaw.Agent+":"), "openclaw.session_prefix must be scoped to openclaw.agent")
	if c.OpenClaw.Model != "" {
		add(strings.TrimSpace(c.OpenClaw.Model) == c.OpenClaw.Model && !strings.ContainsRune(c.OpenClaw.Model, 0), "openclaw.model is invalid")
	}
	durations := map[string]string{
		"openclaw.timeout": c.OpenClaw.Timeout, "budgets.lease_duration": c.Budgets.LeaseDuration,
		"budgets.max_run_duration": c.Budgets.MaxRunDuration, "budgets.poll_interval": c.Budgets.PollInterval,
		"budgets.initial_backoff": c.Budgets.InitialBackoff, "budgets.max_backoff": c.Budgets.MaxBackoff,
		"budgets.shutdown_timeout": c.Budgets.ShutdownTimeout,
	}
	parsed := map[string]time.Duration{}
	for field, value := range durations {
		d, parseErr := time.ParseDuration(value)
		add(parseErr == nil && d > 0, field+" must be a positive duration")
		parsed[field] = d
	}
	add(parsed["budgets.max_backoff"] >= parsed["budgets.initial_backoff"], "budgets.max_backoff must be at least initial_backoff")
	add(c.Budgets.MaxAttempts >= 1 && c.Budgets.MaxAttempts <= 1000, "budgets.max_attempts must be between 1 and 1000")
	add(c.Budgets.MaxConsecutiveFailures >= 1 && c.Budgets.MaxConsecutiveFailures <= c.Budgets.MaxAttempts, "budgets.max_consecutive_failures must be between 1 and max_attempts")
	add(c.Limits.MaxOutputBytes >= 1024 && c.Limits.MaxOutputBytes <= 16<<20, "limits.max_output_bytes must be between 1024 and 16777216")
	add(c.Limits.MaxArtifactBytes >= 1024 && c.Limits.MaxArtifactBytes <= 1<<30, "limits.max_artifact_bytes must be between 1024 and 1073741824")
	add(c.Limits.MaxArtifacts >= 1 && c.Limits.MaxArtifacts <= 10000, "limits.max_artifacts must be between 1 and 10000")
	seen := map[string]bool{}
	for _, issue := range c.Scope.IssueAllowlist {
		add(strings.TrimSpace(issue) != "", "scope.issue_allowlist contains an empty issue ID")
		add(!seen[issue], "scope.issue_allowlist contains duplicate issue ID "+strconv.Quote(issue))
		seen[issue] = true
	}
	if len(c.Scope.IssueAllowlist) > 0 {
		add(seen[c.Scope.IssueID], "scope.issue_allowlist must contain scope.issue_id")
	}

	pathFields := []struct {
		name  string
		value *string
	}{
		{"paths.state_db", &c.Paths.StateDB}, {"paths.state_root", &c.Paths.StateRoot}, {"paths.artifact_root", &c.Paths.ArtifactRoot},
		{"paths.review_root", &c.Paths.ReviewRoot}, {"paths.workspace_root", &c.Paths.WorkspaceRoot},
	}
	for i := range c.Paths.AllowedRoots {
		pathFields = append(pathFields, struct {
			name  string
			value *string
		}{fmt.Sprintf("paths.allowed_roots[%d]", i), &c.Paths.AllowedRoots[i]})
	}
	add(len(c.Paths.AllowedRoots) > 0, "paths.allowed_roots must not be empty")
	for _, field := range pathFields {
		canonical, pathErr := canonicalPath(base, *field.value)
		if pathErr != nil {
			problems = append(problems, field.name+": "+pathErr.Error())
			continue
		}
		*field.value = canonical
	}
	if len(problems) == 0 {
		resolvedAllowed := make([]string, 0, len(c.Paths.AllowedRoots))
		seenAllowed := map[string]bool{}
		for _, root := range c.Paths.AllowedRoots {
			if seenAllowed[root] {
				problems = append(problems, "paths.allowed_roots contains duplicate "+strconv.Quote(root))
			}
			seenAllowed[root] = true
			resolved, pathErr := resolveExistingPrefix(root)
			if pathErr != nil {
				problems = append(problems, "paths.allowed_roots: "+pathErr.Error())
				continue
			}
			resolvedAllowed = append(resolvedAllowed, resolved)
		}
		for _, target := range []struct{ name, value string }{
			{"paths.state_db", c.Paths.StateDB}, {"paths.state_root", c.Paths.StateRoot}, {"paths.artifact_root", c.Paths.ArtifactRoot},
			{"paths.review_root", c.Paths.ReviewRoot}, {"paths.workspace_root", c.Paths.WorkspaceRoot},
		} {
			resolved, pathErr := resolveExistingPrefix(target.value)
			if pathErr != nil {
				problems = append(problems, target.name+": "+pathErr.Error())
				continue
			}
			if !withinAny(resolved, resolvedAllowed) {
				problems = append(problems, target.name+" is outside paths.allowed_roots")
			}
		}
		if !pathWithin(c.Paths.StateDB, c.Paths.StateRoot) {
			problems = append(problems, "paths.state_db must be inside paths.state_root")
		}
		roots := []string{c.Paths.StateRoot, c.Paths.ArtifactRoot, c.Paths.ReviewRoot}
		for i := range roots {
			for j := i + 1; j < len(roots); j++ {
				if pathWithin(roots[i], roots[j]) || pathWithin(roots[j], roots[i]) {
					problems = append(problems, "state_root, artifact_root, and review_root must be distinct and non-overlapping")
				}
			}
		}
		for _, root := range c.Paths.AllowedRoots {
			if err := checkOwnedPrivateRoot(root); err != nil {
				problems = append(problems, "allowed root "+root+": "+err.Error())
			}
		}
		for _, target := range []struct {
			name, value string
			private     bool
		}{{"state_root", c.Paths.StateRoot, true}, {"artifact_root", c.Paths.ArtifactRoot, true}, {"review_root", c.Paths.ReviewRoot, true}, {"workspace_root", c.Paths.WorkspaceRoot, false}} {
			if err := checkPathFromAllowed(target.value, c.Paths.AllowedRoots); err != nil {
				problems = append(problems, target.name+": "+err.Error())
			}
			if err := checkDirectoryTarget(target.value, target.private); err != nil {
				problems = append(problems, target.name+": "+err.Error())
			}
		}
		if err := checkRegularTarget(c.Paths.StateDB); err != nil {
			problems = append(problems, "state_db: "+err.Error())
		}
	}
	if len(problems) != 0 {
		sort.Strings(problems)
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

func ResolveSecret(ref string) (string, error) {
	if !validSecretRef(ref) {
		return "", errors.New("secret must be an env:NAME reference")
	}
	name := strings.TrimPrefix(ref, "env:")
	value, ok := os.LookupEnv(name)
	if !ok || value == "" {
		return "", fmt.Errorf("required environment variable %s is not set", name)
	}
	return value, nil
}

func validSecretRef(ref string) bool {
	if !strings.HasPrefix(ref, "env:") {
		return false
	}
	name := strings.TrimPrefix(ref, "env:")
	if name == "" {
		return false
	}
	for i, r := range name {
		if !(r == '_' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func canonicalPath(base, raw string) (string, error) {
	if raw == "" {
		return "", errors.New("path is empty")
	}
	if strings.TrimSpace(raw) != raw {
		return "", errors.New("path has leading or trailing whitespace")
	}
	if strings.ContainsRune(raw, 0) {
		return "", errors.New("path contains a NUL byte")
	}
	if strings.HasPrefix(raw, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		if raw == "~" {
			raw = home
		} else if strings.HasPrefix(raw, "~/") {
			raw = filepath.Join(home, raw[2:])
		} else {
			return "", errors.New("only ~/ home expansion is supported")
		}
	}
	var missing string
	raw = os.Expand(raw, func(name string) string {
		value, ok := os.LookupEnv(name)
		if !ok {
			missing = name
		}
		return value
	})
	if missing != "" {
		return "", fmt.Errorf("environment variable %s is not set", missing)
	}
	// Expansion is part of the untrusted input. Reject traversal after expansion so
	// an environment value cannot inject a component that filepath.Clean erases.
	for _, part := range strings.Split(filepath.ToSlash(raw), "/") {
		if part == ".." {
			return "", errors.New("path traversal is not allowed")
		}
	}
	if !filepath.IsAbs(raw) {
		raw = filepath.Join(base, raw)
	}
	return filepath.Clean(raw), nil
}

func resolveExistingPrefix(path string) (string, error) {
	tail := []string{}
	current := path
	for {
		_, err := os.Lstat(current)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("no existing path ancestor")
		}
		tail = append(tail, filepath.Base(current))
		current = parent
	}
	resolved, err := filepath.EvalSymlinks(current)
	if err != nil {
		return "", err
	}
	for i := len(tail) - 1; i >= 0; i-- {
		resolved = filepath.Join(resolved, tail[i])
	}
	return filepath.Clean(resolved), nil
}

func withinAny(path string, roots []string) bool {
	for _, root := range roots {
		if pathWithin(path, root) {
			return true
		}
	}
	return false
}

func checkOwnedPrivateRoot(path string) error {
	if err := ensureNoSymlinkComponents(path); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return errors.New("does not exist")
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("is not a directory")
	}
	return secureInfo(info)
}

func ensureNoSymlinkComponents(path string) error {
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return err
	}
	if filepath.Clean(resolved) != filepath.Clean(path) {
		return errors.New("path contains a symbolic-link component")
	}
	return nil
}

func checkDirectoryTarget(path string, private bool) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("must be a directory and not a symlink")
	}
	if private && info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("mode %04o is not private", info.Mode().Perm())
	}
	return secureInfo(info)
}

func checkRegularTarget(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return errors.New("must be a regular file and not a symlink")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("mode %04o is not private", info.Mode().Perm())
	}
	return secureInfo(info)
}

func checkConfigDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return errors.New("must be a directory and not a symlink")
	}
	if err := secureInfo(info); err != nil {
		return err
	}
	return nil
}

func checkPathFromAllowed(path string, allowed []string) error {
	var root string
	for _, candidate := range allowed {
		if pathWithin(path, candidate) && len(candidate) > len(root) {
			root = candidate
		}
	}
	if root == "" {
		return errors.New("outside allowed roots")
	}
	current := root
	for {
		info, err := os.Stat(current)
		if err == nil {
			if err := secureInfo(info); err != nil {
				return fmt.Errorf("unsafe path %s: %w", current, err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if current == path {
			break
		}
		rel, _ := filepath.Rel(current, path)
		part := strings.Split(rel, string(filepath.Separator))[0]
		current = filepath.Join(current, part)
	}
	return nil
}

func secureInfo(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errors.New("ownership is unavailable")
	}
	if int(stat.Uid) != os.Geteuid() {
		return fmt.Errorf("owner uid %d does not match current uid %d", stat.Uid, os.Geteuid())
	}
	if info.Mode().Perm()&0o022 != 0 {
		return fmt.Errorf("mode %04o is group/world writable", info.Mode().Perm())
	}
	return nil
}

func readSecureConfig(path string) ([]byte, os.FileInfo, error) {
	// O_NONBLOCK lets us inspect and reject FIFOs/devices without hanging in open(2).
	// Regular-file reads are unaffected on Linux.
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		if errors.Is(err, syscall.ELOOP) {
			return nil, nil, errors.New("must be a regular file and not a symlink")
		}
		return nil, nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = syscall.Close(fd)
		return nil, nil, errors.New("open config file")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, nil, errors.New("must be a regular file and not a symlink")
	}
	if err := secureInfo(info); err != nil {
		return nil, nil, err
	}
	if info.Size() > 1<<20 {
		return nil, nil, errors.New("config exceeds 1048576 bytes")
	}
	b, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
	if err != nil {
		return nil, nil, err
	}
	if len(b) > 1<<20 {
		return nil, nil, errors.New("config exceeds 1048576 bytes")
	}
	return b, info, nil
}

func pathWithin(path, root string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}
