package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	durablesqlite "github.com/ControlStackAI/dark-factory/internal/adapters/sqlite"
	"github.com/ControlStackAI/dark-factory/internal/app"
	"github.com/ControlStackAI/dark-factory/internal/config"
	"github.com/ControlStackAI/dark-factory/internal/domain"
)

func TestDaemonTwoKeyInterlockAndNoAdapterCall(t *testing.T) {
	path, cfg, sentinel := daemonConfig(t)
	original := runLiveSupervisor
	liveCalls := 0
	runLiveSupervisor = func(_ context.Context, got config.Config) (domain.Run, error) {
		liveCalls++
		if got.Mode != "live" {
			t.Fatalf("live composition received mode %q", got.Mode)
		}
		return domain.Run{ID: "live", ProjectID: got.Scope.ProjectID, IssueID: got.Scope.IssueID, Status: domain.RunComplete, Step: "complete"}, nil
	}
	t.Cleanup(func() { runLiveSupervisor = original })
	cases := []struct {
		name, mode string
		args       []string
		want       string
		success    bool
	}{
		{"dry-no-apply", "dry-run", []string{"--once", "--config", path}, "", true},
		{"dry-apply", "dry-run", []string{"--once", "--apply", "--config", path}, "--apply requires config mode live", false},
		{"live-no-apply", "live", []string{"--once", "--config", path}, "config mode live requires --apply", false},
		{"live-apply", "live", []string{"--once", "--apply", "--config", path}, "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg.Mode = tc.mode
			writeDaemonConfig(t, path, cfg)
			var out, stderr bytes.Buffer
			err := run(tc.args, &out, &stderr)
			if tc.success && err != nil {
				t.Fatal(err)
			}
			if !tc.success && (err == nil || !strings.Contains(err.Error(), tc.want)) {
				t.Fatalf("error=%v want %q", err, tc.want)
			}
			if _, statErr := os.Stat(sentinel); !os.IsNotExist(statErr) {
				t.Fatalf("adapter called: %v", statErr)
			}
		})
	}
	if liveCalls != 1 {
		t.Fatalf("live composition calls=%d, want 1", liveCalls)
	}
}

func TestDaemonHelpAndExitContract(t *testing.T) {
	for _, arg := range []string{"-h", "--help"} {
		var out, stderr bytes.Buffer
		if err := run([]string{arg}, &out, &stderr); err != nil || !strings.Contains(out.String(), "usage: factoryd") {
			t.Fatalf("arg=%s out=%q err=%v", arg, out.String(), err)
		}
	}
	for _, args := range [][]string{nil, {"--once", "extra"}, {"--apply"}} {
		var out, stderr bytes.Buffer
		if err := run(args, &out, &stderr); err == nil {
			t.Fatalf("%v unexpectedly succeeded", args)
		}
	}
}

func TestDaemonDryRunJSONGolden(t *testing.T) {
	path, _, _ := daemonConfig(t)
	var out, stderr bytes.Buffer
	if err := run([]string{"--once", "--config", path}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile("testdata/dry-run.golden")
	if err != nil {
		t.Fatal(err)
	}
	if out.String() != string(want) {
		t.Fatalf("golden mismatch\nwant=%s\ngot=%s", want, out.String())
	}
}

func TestDaemonDurableStateCompatibility(t *testing.T) {
	state := filepath.Join(t.TempDir(), "factory.db")
	var first, second domain.Run
	for index, target := range []*domain.Run{&first, &second} {
		var out, stderr bytes.Buffer
		if err := run([]string{"--once", "--state", state}, &out, &stderr); err != nil {
			t.Fatalf("run %d: %v", index+1, err)
		}
		if err := json.Unmarshal(out.Bytes(), target); err != nil {
			t.Fatalf("decode run %d: %v", index+1, err)
		}
	}
	if first.Status != domain.RunComplete || first.Attempts != 1 {
		t.Fatalf("first run status=%s attempts=%d", first.Status, first.Attempts)
	}
	if second.Status != first.Status || second.Attempts != first.Attempts || second.Version != first.Version {
		t.Fatalf("durable replay changed the run: first=%+v second=%+v", first, second)
	}
	for _, args := range [][]string{
		{"--once", "--state", state, "--apply"},
		{"--once", "--state", state, "--config", filepath.Join(t.TempDir(), "factory.json")},
	} {
		var out, stderr bytes.Buffer
		if err := run(args, &out, &stderr); err == nil {
			t.Fatalf("unsafe compatibility combination succeeded: %v", args)
		}
	}
}

func TestForegroundDaemonProcessLockStatusAndSIGKILLRestart(t *testing.T) {
	path, cfg, sentinel := daemonConfig(t)
	cfg.Mode = "live"
	cfg.Budgets.LeaseDuration = "200ms"
	cfg.Budgets.PollInterval = "10ms"
	cfg.Budgets.InitialBackoff = "10ms"
	cfg.Budgets.MaxBackoff = "20ms"
	cfg.Budgets.MaxRunDuration = "1h"
	cfg.Budgets.ShutdownTimeout = "50ms"
	cfg.OpenClaw.Timeout = "50ms"
	cfg.OpenClaw.Executable = "/bin/false"
	writeDaemonConfig(t, path, cfg)
	store, err := durablesqlite.Open(cfg.Paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	policy := domain.Policy{LeaseDuration: 200 * time.Millisecond, MaxRunDuration: time.Hour, MaxAttempts: cfg.Budgets.MaxAttempts, MaxConsecutiveFailures: cfg.Budgets.MaxConsecutiveFailures}
	runID := app.StableRunID(cfg)
	if err := store.Create(context.Background(), domain.Run{ID: runID, ProjectID: cfg.Scope.ProjectID, IssueID: cfg.Scope.IssueID, Status: domain.RunActive, Step: "process fixture", Policy: policy, StartedAt: now, DeadlineAt: now.Add(time.Hour), Version: 1}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(cfg.Paths.StateDB, 0o600); err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()
	daemonBin := filepath.Join(binDir, "factoryd")
	ctlBin := filepath.Join(binDir, "factoryctl")
	for target, pkg := range map[string]string{daemonBin: ".", ctlBin: "../factoryctl"} {
		build := exec.Command("go", "build", "-o", target, pkg)
		build.Dir = "."
		if output, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build %s: %v\n%s", pkg, err, output)
		}
	}

	first := exec.Command(daemonBin, "--apply", "--config", path)
	first.Env = testOnlyLinearEnv()
	var firstOut, firstErr strings.Builder
	first.Stdout, first.Stderr = &firstOut, &firstErr
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	firstFence := waitForFence(t, cfg.Paths.StateDB, runID, 1)

	contender := exec.Command(daemonBin, "--apply", "--config", path)
	contender.Env = testOnlyLinearEnv()
	contenderOutput, contenderErr := contender.CombinedOutput()
	if contenderErr == nil || !strings.Contains(string(contenderOutput), "already owns this state database") {
		t.Fatalf("second daemon error=%v output=%s", contenderErr, contenderOutput)
	}

	status := exec.Command(ctlBin, "status", "--config", path, "--json")
	status.Env = withoutLinearEnv()
	statusOutput, err := status.CombinedOutput()
	if err != nil {
		t.Fatalf("factoryctl status: %v\n%s", err, statusOutput)
	}
	var report struct {
		Status string `json:"status"`
		Run    struct {
			ID       string `json:"id"`
			Attempts int    `json:"attempts"`
			Fence    uint64 `json:"fence"`
		} `json:"run"`
	}
	if err := json.Unmarshal(statusOutput, &report); err != nil || report.Status != "active" || report.Run.ID != runID || report.Run.Fence != firstFence {
		t.Fatalf("status=%s decode=%v report=%+v", statusOutput, err, report)
	}

	if err := first.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if err := first.Wait(); err == nil {
		t.Fatal("SIGKILLed daemon exited successfully")
	}
	second := exec.Command(daemonBin, "--apply", "--config", path)
	second.Env = testOnlyLinearEnv()
	var secondOut, secondErr strings.Builder
	second.Stdout, second.Stderr = &secondOut, &secondErr
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	secondFence := waitForFence(t, cfg.Paths.StateDB, runID, firstFence+1)
	if secondFence <= firstFence {
		t.Fatalf("restart fence=%d first=%d", secondFence, firstFence)
	}
	if err := second.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := second.Wait(); err != nil {
		t.Fatalf("SIGTERM daemon: %v stderr=%s", err, secondErr.String())
	}
	reopened, err := durablesqlite.Open(cfg.Paths.StateDB)
	if err != nil {
		t.Fatalf("reopen after process restart: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Get(context.Background(), runID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("OpenClaw executable unexpectedly ran: %v", err)
	}
}

func waitForFence(t *testing.T, dbPath, runID string, minimum uint64) uint64 {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run, err := durablesqlite.ReadRun(context.Background(), dbPath, runID)
		if err == nil && run.Lease.Fence >= minimum {
			return run.Lease.Fence
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("durable lease fence did not reach %d", minimum)
	return 0
}

func withoutLinearEnv() []string {
	var result []string
	for _, value := range os.Environ() {
		if !strings.HasPrefix(value, "LINEAR_API_KEY=") {
			result = append(result, value)
		}
	}
	return result
}

func testOnlyLinearEnv() []string {
	return append(withoutLinearEnv(), "LINEAR_API_KEY=m3-loopback-test-only")
}

func daemonConfig(t *testing.T) (string, config.Config, string) {
	t.Helper()
	root := t.TempDir()
	_ = os.Chmod(root, 0o700)
	path := filepath.Join(root, "factory.json")
	cfg, err := config.Default(path)
	if err != nil {
		t.Fatal(err)
	}
	sentinel := filepath.Join(root, "CALLED")
	executable := filepath.Join(root, "fake-openclaw")
	if err := os.WriteFile(executable, []byte("#!/bin/sh\nprintf called > \""+sentinel+"\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg.OpenClaw.Executable = executable
	cfg.Linear.Endpoint = "https://127.0.0.1:1/graphql"
	if err := config.WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	return path, cfg, sentinel
}
func writeDaemonConfig(t *testing.T, path string, cfg config.Config) {
	t.Helper()
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}
