package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestDefaultCoversM1FieldsAndValidates(t *testing.T) {
	path, cfg := newConfig(t)
	if cfg.ConfigVersion != 1 || cfg.Mode != "dry-run" || cfg.Paths.StateDB == "" || cfg.Paths.StateRoot == "" || cfg.Paths.ArtifactRoot == "" || cfg.Paths.ReviewRoot == "" || cfg.Paths.WorkspaceRoot == "" || len(cfg.Paths.AllowedRoots) == 0 {
		t.Fatal("path/version/mode defaults are incomplete")
	}
	if cfg.Scope.TeamID == "" || cfg.Scope.ProjectID == "" || cfg.Scope.IssueID == "" || cfg.Linear.Endpoint == "" || cfg.Linear.APIKey != "env:LINEAR_API_KEY" {
		t.Fatal("scope/Linear defaults are incomplete")
	}
	if cfg.OpenClaw.Executable == "" || cfg.OpenClaw.Agent == "" || cfg.OpenClaw.SessionPrefix == "" || cfg.OpenClaw.Timeout == "" || cfg.OpenClaw.Delivery {
		t.Fatal("OpenClaw defaults are incomplete or delivery is enabled")
	}
	if cfg.Budgets.LeaseDuration == "" || cfg.Budgets.MaxAttempts == 0 || cfg.Budgets.MaxConsecutiveFailures == 0 || cfg.Budgets.MaxRunDuration == "" || cfg.Budgets.PollInterval == "" || cfg.Budgets.InitialBackoff == "" || cfg.Budgets.MaxBackoff == "" || cfg.Budgets.ShutdownTimeout == "" {
		t.Fatal("budget defaults are incomplete")
	}
	if cfg.Lifecycle.Ready == "" || cfg.Lifecycle.InProgress == "" || cfg.Lifecycle.Done == "" || cfg.Limits.MaxOutputBytes == 0 || cfg.Limits.MaxArtifactBytes == 0 || cfg.Limits.MaxArtifacts == 0 {
		t.Fatal("lifecycle/limit defaults are incomplete")
	}
	loaded, err := Load(path, LoadOptions{RequireSecret: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range append([]string{loaded.Paths.StateDB, loaded.Paths.StateRoot, loaded.Paths.ArtifactRoot, loaded.Paths.ReviewRoot, loaded.Paths.WorkspaceRoot}, loaded.Paths.AllowedRoots...) {
		if !filepath.IsAbs(value) {
			t.Fatalf("path was not canonicalized: %q", value)
		}
	}
}

func TestStrictDocumentsRejectUnknownDuplicateAndUnsupportedVersions(t *testing.T) {
	path, cfg := newConfig(t)
	base := mustJSON(t, cfg)
	cases := []struct{ name, document, want string }{
		{"unknown", strings.Replace(base, `"mode": "dry-run"`, `"mode": "dry-run", "surprise": true`, 1), "unknown field"},
		{"duplicate-json", strings.Replace(base, `"mode": "dry-run"`, `"mode": "dry-run", "mode": "live"`, 1), `duplicate key "mode"`},
		{"version-zero", strings.Replace(base, `"config_version": 1`, `"config_version": 0`, 1), "config_version must be 1"},
		{"version-two", strings.Replace(base, `"config_version": 1`, `"config_version": 2`, 1), "config_version must be 1"},
		{"duplicate-yaml", "config_version: 1\nconfig_version: 1\n", `duplicate key "config_version"`},
		{"duplicate-yaml-nested", "config_version: 1\npaths:\n  state_db: one\n  state_db: two\n", `duplicate key "state_db"`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			writeDocument(t, path, tc.document)
			_, err := Load(path, LoadOptions{})
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v, want %q", err, tc.want)
			}
		})
	}
}

func TestSimpleYAMLLoadsStrictly(t *testing.T) {
	path, cfg := newConfig(t)
	doc := yamlDocument(cfg)
	writeDocument(t, path, doc)
	loaded, err := Load(path, LoadOptions{RequireSecret: true})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ConfigVersion != 1 || loaded.Scope.TeamID != cfg.Scope.TeamID || len(loaded.Paths.AllowedRoots) != 2 {
		t.Fatalf("unexpected load: %+v", loaded)
	}
}

func TestSecretProviderValidationMissingEnvironmentAndRedaction(t *testing.T) {
	valid := []string{"env:A", "env:LINEAR_API_KEY", "env:A_2"}
	invalid := []string{"", "literal", "env:", "env:lower", "env:2BAD", "file:/secret", "env:A-B"}
	for _, ref := range valid {
		if !validSecretRef(ref) {
			t.Errorf("rejected %q", ref)
		}
	}
	for _, ref := range invalid {
		if validSecretRef(ref) {
			t.Errorf("accepted %q", ref)
		}
	}

	path, _ := newConfig(t)
	t.Setenv("LINEAR_API_KEY", "")
	_, err := Load(path, LoadOptions{RequireSecret: true})
	if err == nil || !strings.Contains(err.Error(), "LINEAR_API_KEY") || strings.Contains(err.Error(), "test-secret-value") {
		t.Fatalf("unexpected missing-secret error: %v", err)
	}
	t.Setenv("LINEAR_API_KEY", "test-secret-value")
	loaded, err := Load(path, LoadOptions{RequireSecret: true})
	if err != nil {
		t.Fatal(err)
	}
	b := mustJSON(t, loaded)
	if strings.Contains(b, "test-secret-value") || !strings.Contains(b, "env:LINEAR_API_KEY") {
		t.Fatal("resolved secret leaked or reference was lost")
	}
}

func TestValidationBoundaries(t *testing.T) {
	_, base := newConfig(t)
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"mode", func(c *Config) { c.Mode = "automatic" }, "mode must"},
		{"empty-team", func(c *Config) { c.Scope.TeamID = "" }, "scope.team_id"},
		{"empty-project", func(c *Config) { c.Scope.ProjectID = "" }, "scope.project_id"},
		{"empty-issue", func(c *Config) { c.Scope.IssueID = "" }, "scope.issue_id"},
		{"allowlist-missing-primary", func(c *Config) { c.Scope.IssueAllowlist = []string{"OTHER"} }, "must contain"},
		{"allowlist-duplicate", func(c *Config) { c.Scope.IssueAllowlist = []string{c.Scope.IssueID, c.Scope.IssueID} }, "duplicate"},
		{"endpoint-http", func(c *Config) { c.Linear.Endpoint = "http://example.test" }, "HTTPS"},
		{"literal-secret", func(c *Config) { c.Linear.APIKey = "secret" }, "env:NAME"},
		{"delivery", func(c *Config) { c.OpenClaw.Delivery = true }, "delivery must be false"},
		{"empty-agent", func(c *Config) { c.OpenClaw.Agent = "" }, "openclaw.agent"},
		{"relative-executable-path", func(c *Config) { c.OpenClaw.Executable = "bin/openclaw" }, "bare command name or absolute path"},
		{"bad-session", func(c *Config) { c.OpenClaw.SessionPrefix = "not isolated" }, "must not contain whitespace"},
		{"session-agent-mismatch", func(c *Config) { c.OpenClaw.SessionPrefix = "agent:other:factory" }, "scoped to openclaw.agent"},
		{"bad-model", func(c *Config) { c.OpenClaw.Model = " model" }, "openclaw.model"},
		{"zero-timeout", func(c *Config) { c.OpenClaw.Timeout = "0s" }, "positive duration"},
		{"lease-duration", func(c *Config) { c.Budgets.LeaseDuration = "0s" }, "budgets.lease_duration"},
		{"max-run-duration", func(c *Config) { c.Budgets.MaxRunDuration = "0s" }, "budgets.max_run_duration"},
		{"poll-interval", func(c *Config) { c.Budgets.PollInterval = "0s" }, "budgets.poll_interval"},
		{"initial-backoff", func(c *Config) { c.Budgets.InitialBackoff = "0s" }, "budgets.initial_backoff"},
		{"max-backoff", func(c *Config) { c.Budgets.MaxBackoff = "0s" }, "budgets.max_backoff"},
		{"shutdown-timeout", func(c *Config) { c.Budgets.ShutdownTimeout = "0s" }, "budgets.shutdown_timeout"},
		{"backoff-order", func(c *Config) { c.Budgets.InitialBackoff = "2m"; c.Budgets.MaxBackoff = "1m" }, "at least initial_backoff"},
		{"attempts-min", func(c *Config) { c.Budgets.MaxAttempts = 0 }, "max_attempts"},
		{"attempts-max", func(c *Config) { c.Budgets.MaxAttempts = 1001 }, "max_attempts"},
		{"failures-min", func(c *Config) { c.Budgets.MaxConsecutiveFailures = 0 }, "max_consecutive_failures"},
		{"failures-max", func(c *Config) { c.Budgets.MaxConsecutiveFailures = c.Budgets.MaxAttempts + 1 }, "max_consecutive_failures"},
		{"output-min", func(c *Config) { c.Limits.MaxOutputBytes = 1023 }, "max_output_bytes"},
		{"output-max", func(c *Config) { c.Limits.MaxOutputBytes = (16 << 20) + 1 }, "max_output_bytes"},
		{"artifact-min", func(c *Config) { c.Limits.MaxArtifactBytes = 1023 }, "max_artifact_bytes"},
		{"artifact-max", func(c *Config) { c.Limits.MaxArtifactBytes = (1 << 30) + 1 }, "max_artifact_bytes"},
		{"artifact-count-min", func(c *Config) { c.Limits.MaxArtifacts = 0 }, "max_artifacts"},
		{"artifact-count-max", func(c *Config) { c.Limits.MaxArtifacts = 10001 }, "max_artifacts"},
		{"lifecycle", func(c *Config) { c.Lifecycle.Done = "" }, "lifecycle.done"},
		{"allowed-roots-empty", func(c *Config) { c.Paths.AllowedRoots = nil }, "allowed_roots must not be empty"},
		{"allowed-roots-duplicate", func(c *Config) { c.Paths.AllowedRoots = append(c.Paths.AllowedRoots, c.Paths.AllowedRoots[0]) }, "duplicate"},
		{"state-db-outside-state-root", func(c *Config) { c.Paths.StateDB = filepath.Join(filepath.Dir(c.Paths.StateRoot), "other.db") }, "must be inside paths.state_root"},
		{"roots-distinct", func(c *Config) { c.Paths.ReviewRoot = c.Paths.ArtifactRoot }, "must be distinct"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.Paths.AllowedRoots = append([]string(nil), base.Paths.AllowedRoots...)
			tc.mutate(&cfg)
			err := cfg.Validate(filepath.Dir(base.Paths.StateRoot))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want %q", err, tc.want)
			}
		})
	}
}

func TestValidationAcceptsExactBoundaries(t *testing.T) {
	_, base := newConfig(t)
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"attempts-min", func(c *Config) { c.Budgets.MaxAttempts = 1; c.Budgets.MaxConsecutiveFailures = 1 }},
		{"attempts-max", func(c *Config) { c.Budgets.MaxAttempts = 1000; c.Budgets.MaxConsecutiveFailures = 1000 }},
		{"output-min", func(c *Config) { c.Limits.MaxOutputBytes = 1024 }},
		{"output-max", func(c *Config) { c.Limits.MaxOutputBytes = 16 << 20 }},
		{"artifact-min", func(c *Config) { c.Limits.MaxArtifactBytes = 1024 }},
		{"artifact-max", func(c *Config) { c.Limits.MaxArtifactBytes = 1 << 30 }},
		{"artifact-count-min", func(c *Config) { c.Limits.MaxArtifacts = 1 }},
		{"artifact-count-max", func(c *Config) { c.Limits.MaxArtifacts = 10000 }},
		{"openclaw-timeout", func(c *Config) { c.OpenClaw.Timeout = "1ns" }},
		{"lease-duration", func(c *Config) { c.Budgets.LeaseDuration = "1ns" }},
		{"max-run-duration", func(c *Config) { c.Budgets.MaxRunDuration = "1ns" }},
		{"poll-interval", func(c *Config) { c.Budgets.PollInterval = "1ns" }},
		{"initial-backoff", func(c *Config) { c.Budgets.InitialBackoff = "1ns" }},
		{"max-backoff", func(c *Config) { c.Budgets.InitialBackoff = "1ns"; c.Budgets.MaxBackoff = "1ns" }},
		{"shutdown-timeout", func(c *Config) { c.Budgets.ShutdownTimeout = "1ns" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.Paths.AllowedRoots = append([]string(nil), base.Paths.AllowedRoots...)
			tc.mutate(&cfg)
			if err := cfg.Validate(filepath.Dir(base.Paths.StateRoot)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPathAttacksAndPermissionsFailClosed(t *testing.T) {
	path, base := newConfig(t)
	outside := t.TempDir()
	attack := filepath.Join(filepath.Dir(path), "escape")
	if err := os.Symlink(outside, attack); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"empty", func(c *Config) { c.Paths.StateRoot = "" }, "path is empty"},
		{"traversal", func(c *Config) {
			c.Paths.StateDB = c.Paths.StateRoot + string(filepath.Separator) + ".." + string(filepath.Separator) + "outside.db"
		}, "traversal"},
		{"outside", func(c *Config) { c.Paths.ArtifactRoot = outside }, "outside paths.allowed_roots"},
		{"symlink-escape", func(c *Config) { c.Paths.ArtifactRoot = filepath.Join(attack, "artifacts") }, "outside paths.allowed_roots"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := base
			cfg.Paths.AllowedRoots = append([]string(nil), base.Paths.AllowedRoots...)
			tc.mutate(&cfg)
			err := cfg.Validate(filepath.Dir(path))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error=%v want %q", err, tc.want)
			}
		})
	}

	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, LoadOptions{}); err == nil || !strings.Contains(err.Error(), "unsafe mode") {
		t.Fatalf("accepted broad config mode: %v", err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(base.Paths.ArtifactRoot, 0o777); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, LoadOptions{}); err == nil || !strings.Contains(err.Error(), "world writable") {
		t.Fatalf("accepted world-writable target: %v", err)
	}
}

func TestWorldWritableAncestryAndUnsafeOwnershipFailClosed(t *testing.T) {
	path, cfg := newConfig(t)
	parent := filepath.Join(filepath.Dir(path), "writable-parent")
	if err := os.Mkdir(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o777); err != nil {
		t.Fatal(err)
	}
	cfg.Paths.ArtifactRoot = filepath.Join(parent, "artifacts")
	if err := cfg.Validate(filepath.Dir(path)); err == nil || !strings.Contains(err.Error(), "world writable") {
		t.Fatalf("accepted unsafe ancestry: %v", err)
	}

	info, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}
	stat := *info.Sys().(*syscall.Stat_t)
	stat.Uid = uint32(os.Geteuid() + 1)
	if err := secureInfo(fakeInfo{FileInfo: info, stat: &stat}); err == nil || !strings.Contains(err.Error(), "owner uid") {
		t.Fatalf("accepted foreign owner: %v", err)
	}
}

func TestPathExpansionAndRelativeCanonicalization(t *testing.T) {
	path, cfg := newConfig(t)
	root := filepath.Dir(path)
	t.Setenv("CFG_ROOT", root)
	cfg.Paths.StateRoot = "${CFG_ROOT}/state"
	cfg.Paths.StateDB = "${CFG_ROOT}/state/factory.db"
	cfg.Paths.ArtifactRoot = "artifacts"
	cfg.Paths.ReviewRoot = "reviews"
	cfg.Paths.WorkspaceRoot = "."
	cfg.Paths.AllowedRoots = []string{"${CFG_ROOT}"}
	if err := cfg.Validate(root); err != nil {
		t.Fatal(err)
	}
	for _, value := range append([]string{cfg.Paths.StateDB, cfg.Paths.StateRoot, cfg.Paths.ArtifactRoot, cfg.Paths.ReviewRoot, cfg.Paths.WorkspaceRoot}, cfg.Paths.AllowedRoots...) {
		if !filepath.IsAbs(value) || strings.Contains(value, "$") {
			t.Fatalf("not expanded/canonical: %q", value)
		}
	}
	cfg.Paths.StateRoot = "${MISSING_ROOT}/state"
	if err := cfg.Validate(root); err == nil || !strings.Contains(err.Error(), "MISSING_ROOT") {
		t.Fatalf("accepted missing expansion: %v", err)
	}
	t.Setenv("TRAVERSING_ROOT", root+string(filepath.Separator)+".."+string(filepath.Separator)+filepath.Base(root))
	cfg.Paths.StateRoot = "${TRAVERSING_ROOT}/state"
	if err := cfg.Validate(root); err == nil || !strings.Contains(err.Error(), "traversal") {
		t.Fatalf("accepted traversal introduced by expansion: %v", err)
	}
}

func TestConfigSymlinkRejected(t *testing.T) {
	path, _ := newConfig(t)
	link := filepath.Join(filepath.Dir(path), "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(link, LoadOptions{}); err == nil || !strings.Contains(err.Error(), "not a symlink") {
		t.Fatalf("accepted symlink: %v", err)
	}
}

func TestConfigFIFORejectedWithoutBlocking(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "factory.json")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := Load(path, LoadOptions{})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "regular file") {
			t.Fatalf("FIFO error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("config validation blocked opening a FIFO")
	}
}

func TestConfigurationNestingIsBounded(t *testing.T) {
	jsonDocument := strings.Repeat(`{"nested":`, maxConfigNesting+1) + "0" + strings.Repeat("}", maxConfigNesting+1)
	if _, err := decode([]byte(jsonDocument)); err == nil || !strings.Contains(err.Error(), "nesting exceeds") {
		t.Fatalf("deep JSON error=%v", err)
	}

	var yaml strings.Builder
	for depth := 0; depth <= maxConfigNesting; depth++ {
		fmt.Fprintf(&yaml, "%snested:\n", strings.Repeat("  ", depth))
	}
	fmt.Fprintf(&yaml, "%svalue: x\n", strings.Repeat("  ", maxConfigNesting+1))
	if _, err := decode([]byte(yaml.String())); err == nil || !strings.Contains(err.Error(), "nesting exceeds") {
		t.Fatalf("deep YAML error=%v", err)
	}
}

func TestWriteNewIsPrivateAndConcurrentNoReplace(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "factory.json")
	cfg, err := Default(path)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 24
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- WriteNew(path, cfg) }()
	}
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		} else if !strings.Contains(err.Error(), "already exists") {
			t.Errorf("unexpected init error: %v", err)
		}
	}
	if wins != 1 {
		t.Fatalf("successful initializers=%d, want 1", wins)
	}
	for _, target := range []struct {
		path string
		mode os.FileMode
	}{{path, 0o600}, {cfg.Paths.StateRoot, 0o700}, {cfg.Paths.ArtifactRoot, 0o700}, {cfg.Paths.ReviewRoot, 0o700}} {
		info, err := os.Stat(target.path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != target.mode {
			t.Errorf("%s mode=%04o", target.path, info.Mode().Perm())
		}
	}
	t.Setenv("LINEAR_API_KEY", "fake")
	if _, err := Load(path, LoadOptions{RequireSecret: true}); err != nil {
		t.Fatal(err)
	}
}

func TestWriteNewDoesNotReplaceDanglingSymlink(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "factory.json")
	if err := os.Symlink(filepath.Join(root, "missing"), path); err != nil {
		t.Fatal(err)
	}
	cfg, err := Default(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteNew(path, cfg); err == nil {
		t.Fatal("replaced final symlink")
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink == 0 {
		t.Fatalf("final path changed: %v %v", info, err)
	}
}

func TestWriteNewAllowsSecureExistingParentWithoutChangingItsMode(t *testing.T) {
	root := t.TempDir()
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "factory.json")
	cfg, err := Default(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("existing parent mode changed to %04o", info.Mode().Perm())
	}
	for _, private := range []string{cfg.Paths.StateRoot, cfg.Paths.ArtifactRoot, cfg.Paths.ReviewRoot} {
		info, err := os.Stat(private)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != 0o700 {
			t.Fatalf("%s mode=%04o", private, info.Mode().Perm())
		}
	}
}

func TestAnchoredRootSurvivesRenameAndSymlinkSwap(t *testing.T) {
	parent := t.TempDir()
	if err := os.Chmod(parent, 0o700); err != nil {
		t.Fatal(err)
	}
	original := filepath.Join(parent, "allowed")
	moved := filepath.Join(parent, "moved")
	outside := filepath.Join(parent, "outside")
	for _, path := range []string{original, outside} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	anchored, err := openAnchoredRoot(original)
	if err != nil {
		t.Fatal(err)
	}
	defer anchored.close()
	if err := os.Rename(original, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, original); err != nil {
		t.Fatal(err)
	}
	if err := mkdirPrivateAnchored(filepath.Join(original, "state"), []*anchoredRoot{anchored}); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(filepath.Join(moved, "state")); err != nil || !info.IsDir() {
		t.Fatalf("anchored target was not created in original directory object: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "state")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("symlink replacement received a write: %v", err)
	}
}

func TestConfigSizeIsBounded(t *testing.T) {
	path, _ := newConfig(t)
	if err := os.WriteFile(path, make([]byte, (1<<20)+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, LoadOptions{}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("accepted oversized config: %v", err)
	}
}

func newConfig(t *testing.T) (string, Config) {
	t.Helper()
	t.Setenv("LINEAR_API_KEY", "fake-test-token")
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "factory.json")
	cfg, err := Default(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	return path, cfg
}

func writeDocument(t *testing.T, path, document string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
		t.Fatal(err)
	}
}
func mustJSON(t *testing.T, value any) string {
	t.Helper()
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func yamlDocument(c Config) string {
	q := func(value string) string { b, _ := json.Marshal(value); return string(b) }
	return "config_version: 1\nmode: dry-run\npaths:\n  state_db: " + q(c.Paths.StateDB) + "\n  state_root: " + q(c.Paths.StateRoot) + "\n  artifact_root: " + q(c.Paths.ArtifactRoot) + "\n  review_root: " + q(c.Paths.ReviewRoot) + "\n  workspace_root: " + q(c.Paths.WorkspaceRoot) + "\n  allowed_roots:\n    - " + q(c.Paths.AllowedRoots[0]) + "\n    - " + q(c.Paths.AllowedRoots[1]) + "\nscope:\n  team_id: TEAM_ID\n  project_id: PROJECT_ID\n  issue_id: ISSUE_ID\n  issue_allowlist: []\nlinear:\n  endpoint: https://api.linear.app/graphql\n  api_key: env:LINEAR_API_KEY\nopenclaw:\n  executable: openclaw\n  agent: main\n  session_prefix: agent:main:dark-factory\n  model: \"\"\n  timeout: 15m\n  delivery: false\nbudgets:\n  lease_duration: 2m\n  max_attempts: 8\n  max_consecutive_failures: 3\n  max_run_duration: 24h\n  poll_interval: 5s\n  initial_backoff: 1s\n  max_backoff: 1m\n  shutdown_timeout: 30s\nlifecycle:\n  ready: Ready\n  in_progress: In Progress\n  done: Done\nlimits:\n  max_output_bytes: 1048576\n  max_artifact_bytes: 67108864\n  max_artifacts: 256\n"
}

type fakeInfo struct {
	os.FileInfo
	stat *syscall.Stat_t
}

func (f fakeInfo) Sys() any           { return f.stat }
func (f fakeInfo) ModTime() time.Time { return f.FileInfo.ModTime() }
