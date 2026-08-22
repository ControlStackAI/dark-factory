package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	filesystemadapter "github.com/ControlStackAI/dark-factory/internal/adapters/filesystem"
	"github.com/ControlStackAI/dark-factory/internal/config"
)

type recordingProbe struct {
	calls int
	issue string
	err   error
}

func (p *recordingProbe) Probe(_ context.Context, issue string) error {
	p.calls++
	p.issue = issue
	return p.err
}

func TestInitValidateAndNoOverwrite(t *testing.T) {
	path := filepath.Join(privateTemp(t), "factory.json")
	var out, stderr bytes.Buffer
	if err := run([]string{"init", "--config", path}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("mode=%04o", info.Mode().Perm())
	}
	t.Setenv("LINEAR_API_KEY", "fake")
	out.Reset()
	if err := run([]string{"validate", "--config", path, "--json"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "testdata/validate.golden", out.String())
	if err := run([]string{"init", "--config", path}, &out, &stderr); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second init error=%v", err)
	}
}

func TestDoctorJSONGoldenHasSeparateReadinessAndNoSecret(t *testing.T) {
	path, cfg := initializedConfig(t)
	cfg.OpenClaw.Executable = filepath.Join(filepath.Dir(path), "missing-openclaw")
	rewriteConfig(t, path, cfg)
	t.Setenv("LINEAR_API_KEY", "")
	var out, stderr bytes.Buffer
	if err := run([]string{"doctor", "--config", path, "--json"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "testdata/doctor.golden", out.String())
	if strings.Contains(out.String(), "fake-secret") {
		t.Fatal("secret leaked")
	}
	var report struct {
		Checks []struct{ Name, Status string } `json:"checks"`
	}
	if err := json.Unmarshal(out.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	want := []string{"artifact-root", "config", "linear", "openclaw", "review-root", "service-environment", "state-db"}
	var got []string
	for _, check := range report.Checks {
		got = append(got, check.Name)
		if (check.Name == "linear" || check.Name == "openclaw") && check.Status == "ready" {
			t.Fatalf("%s reported ready", check.Name)
		}
	}
	sort.Strings(got)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("checks=%v", got)
	}
}

func TestDoctorOnlineIsExplicitAndQueryOnlyProbeIsReported(t *testing.T) {
	path, cfg := initializedConfig(t)
	t.Setenv("LINEAR_API_KEY", "test-only")
	probe := &recordingProbe{}
	original := newOnlineLinearProbe
	newOnlineLinearProbe = func(got config.Config) (linearProbe, error) {
		if got.Scope.TeamID != cfg.Scope.TeamID || got.Scope.ProjectID != cfg.Scope.ProjectID {
			t.Fatalf("probe constructed with wrong scope")
		}
		return probe, nil
	}
	t.Cleanup(func() { newOnlineLinearProbe = original })
	var out, stderr bytes.Buffer
	if err := run([]string{"doctor", "--config", path, "--json"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if probe.calls != 0 {
		t.Fatal("default doctor invoked the online probe")
	}
	out.Reset()
	if err := run([]string{"doctor", "--config", path, "--online", "--json"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	if probe.calls != 1 || probe.issue != cfg.Scope.IssueID || !strings.Contains(out.String(), "query-only scope and lifecycle probe succeeded") {
		t.Fatalf("calls=%d issue=%q output=%s", probe.calls, probe.issue, out.String())
	}
}

func TestValidateDoctorStatusAreReadOnlyAndDryRunIsNonDurable(t *testing.T) {
	path, _ := initializedConfig(t)
	t.Setenv("LINEAR_API_KEY", "fake-secret-value")
	before := treeSnapshot(t, filepath.Dir(path))
	commands := [][]string{{"validate", "--config", path, "--json"}, {"doctor", "--config", path, "--json"}, {"status", "--config", path, "--json"}, {"dry-run", "--config", path, "--json"}}
	for _, args := range commands {
		var out, stderr bytes.Buffer
		if err := run(args, &out, &stderr); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if strings.Contains(out.String()+stderr.String(), "fake-secret-value") {
			t.Fatalf("%v leaked secret", args)
		}
	}
	after := treeSnapshot(t, filepath.Dir(path))
	if before != after {
		t.Fatalf("non-live commands mutated filesystem\nbefore=%s\nafter=%s", before, after)
	}
}

func TestDryRunJSONGolden(t *testing.T) {
	path, _ := initializedConfig(t)
	var out, stderr bytes.Buffer
	if err := run([]string{"dry-run", "--config", path, "--json"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "testdata/dry-run.golden", out.String())
}

func TestNoNetworkOrExecutorSentinelForEveryNonLiveCommand(t *testing.T) {
	path, cfg := initializedConfig(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan bool, 1)
	go func() {
		connection, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = connection.Close()
		}
		accepted <- acceptErr == nil
	}()
	sentinel := filepath.Join(filepath.Dir(path), "CALLED")
	executable := filepath.Join(filepath.Dir(path), "fake-openclaw")
	script := "#!/bin/sh\nprintf called > \"" + sentinel + "\"\n"
	if err := os.WriteFile(executable, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	cfg.OpenClaw.Executable = executable
	cfg.Linear.Endpoint = "https://" + listener.Addr().String() + "/graphql"
	rewriteConfig(t, path, cfg)
	t.Setenv("LINEAR_API_KEY", "sentinel-secret")
	for _, args := range [][]string{{"validate", "--config", path}, {"doctor", "--config", path}, {"status", "--config", path}, {"dry-run", "--config", path}} {
		var out, stderr bytes.Buffer
		if err := run(args, &out, &stderr); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		if _, err := os.Stat(sentinel); !os.IsNotExist(err) {
			t.Fatalf("%v contacted an adapter: %v", args, err)
		}
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if <-accepted {
		t.Fatal("a non-live command attempted a Linear network connection")
	}
}

func TestHelpAndExitContract(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"--help"}, {"-h"}} {
		var out, stderr bytes.Buffer
		if err := run(args, &out, &stderr); err != nil || !strings.Contains(out.String(), "usage: factoryctl") {
			t.Fatalf("%v out=%q err=%v", args, out.String(), err)
		}
	}
	for _, args := range [][]string{nil, {"unknown"}, {"version", "extra"}, {"validate", "extra"}} {
		var out, stderr bytes.Buffer
		if err := run(args, &out, &stderr); err == nil {
			t.Fatalf("%v unexpectedly succeeded", args)
		}
	}
}

func TestPacketFinalizeAndIndependentVerifyCommands(t *testing.T) {
	path, cfg := initializedConfig(t)
	workspace := filepath.Join(filepath.Dir(path), "packet-workspace")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	command := func(args ...string) {
		cmd := exec.Command("git", args...)
		cmd.Dir = workspace
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
	}
	command("init", "-q")
	if err := os.WriteFile(filepath.Join(workspace, "base.txt"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	command("add", "base.txt")
	command("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base")
	if err := os.WriteFile(filepath.Join(workspace, "candidate.txt"), []byte("candidate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg.Paths.WorkspaceRoot = workspace
	cfg.Paths.AllowedRoots = append(cfg.Paths.AllowedRoots, workspace)
	rewriteConfig(t, path, cfg)
	state, err := filesystemadapter.InspectSource(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	sourceDigest, err := filesystemadapter.SourceDigest(state.Claim)
	if err != nil {
		t.Fatal(err)
	}
	artifact := []byte("response\n")
	receipt := filesystemadapter.ReviewReceipt{ReceiptVersion: 1, ReviewID: "review:run-cli:ISSUE_ID:1", ProjectID: "PROJECT_ID", IssueID: "ISSUE_ID", RunID: "run-cli", CheckpointSequence: 1,
		SourceCommit: state.Claim.Commit, SourceDigest: sourceDigest, ArtifactPath: "response.json", ArtifactSHA256: fmt.Sprintf("%x", sha256.Sum256(artifact)), Verdict: "approved",
		Checks: []string{"go test ./..."}, Author: filesystemadapter.Identity{Provider: "openai", Model: "author"}, Reviewer: filesystemadapter.Identity{Provider: "google", Model: "reviewer"}}
	reviewBytes, err := filesystemadapter.CanonicalReviewReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string][]byte{"response.json": artifact, "review.json": reviewBytes, "source.diff": state.Diff, "tests.txt": []byte("pass\n")}
	members := []filesystemadapter.Member{{Path: "response.json", Kind: "artifact"}, {Path: "review.json", Kind: "review_receipt"}, {Path: "source.diff", Kind: "source_diff"}, {Path: "tests.txt", Kind: "test_receipt"}}
	for index := range members {
		members[index].Size = int64(len(files[members[index].Path]))
		members[index].SHA256 = fmt.Sprintf("%x", sha256.Sum256(files[members[index].Path]))
	}
	manifest := filesystemadapter.Manifest{PacketVersion: 1, ReviewID: receipt.ReviewID, ProjectID: receipt.ProjectID, IssueID: receipt.IssueID, RunID: receipt.RunID, CheckpointSequence: 1, Source: state.Claim, Members: members}
	manifestBytes, err := filesystemadapter.CanonicalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := os.MkdirTemp(cfg.Paths.ReviewRoot, ".pending-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(pending, 0o700); err != nil {
		t.Fatal(err)
	}
	files[filesystemadapter.ManifestName] = manifestBytes
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(pending, name), contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var out, stderr bytes.Buffer
	if err := run([]string{"packet", "finalize", "--config", path, "--packet", pending, "--json"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	var finalized packetReport
	if err := json.Unmarshal(out.Bytes(), &finalized); err != nil {
		t.Fatal(err)
	}
	if finalized.Status != "verified" || finalized.PacketDigest == "" || finalized.SourceDigest != sourceDigest {
		t.Fatalf("report=%+v", finalized)
	}
	out.Reset()
	if err := run([]string{"packet", "verify", "--config", path, "--packet", finalized.Path, "--json"}, &out, &stderr); err != nil {
		t.Fatal(err)
	}
	var verified packetReport
	if err := json.Unmarshal(out.Bytes(), &verified); err != nil {
		t.Fatal(err)
	}
	if verified.PacketDigest != finalized.PacketDigest || verified.SourceDigest != finalized.SourceDigest {
		t.Fatalf("finalized=%+v verified=%+v", finalized, verified)
	}
}

func initializedConfig(t *testing.T) (string, config.Config) {
	t.Helper()
	path := filepath.Join(privateTemp(t), "factory.json")
	cfg, err := config.Default(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := config.WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	return path, cfg
}
func privateTemp(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	return root
}
func rewriteConfig(t *testing.T, path string, cfg config.Config) {
	t.Helper()
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, append(b, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}
func assertGolden(t *testing.T, path, got string) {
	t.Helper()
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got != string(want) {
		t.Fatalf("golden mismatch\nwant=%s\ngot=%s", want, got)
	}
}
func treeSnapshot(t *testing.T, root string) string {
	t.Helper()
	var entries []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		digest := "-"
		if info.Mode().IsRegular() {
			contents, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			digest = fmt.Sprintf("%x", sha256.Sum256(contents))
		} else if info.Mode()&os.ModeSymlink != 0 {
			target, readErr := os.Readlink(path)
			if readErr != nil {
				return readErr
			}
			digest = fmt.Sprintf("link:%x", sha256.Sum256([]byte(target)))
		}
		entries = append(entries, fmt.Sprintf("%s:%s:%d:%s", rel, info.Mode(), info.Size(), digest))
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(entries)
	return strings.Join(entries, "\n")
}
