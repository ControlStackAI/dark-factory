//go:build linux

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	filesystemadapter "github.com/ControlStackAI/dark-factory/internal/adapters/filesystem"
	durablesqlite "github.com/ControlStackAI/dark-factory/internal/adapters/sqlite"
	"github.com/ControlStackAI/dark-factory/internal/app"
	"github.com/ControlStackAI/dark-factory/internal/config"
	"github.com/ControlStackAI/dark-factory/internal/domain"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

const (
	m5Token      = "m5-loopback-only-token"
	m5Team       = "team-m5"
	m5Project    = "project-m5"
	m5IssueOne   = "issue-m5-1"
	m5IssueTwo   = "issue-m5-2"
	m5Ready      = "state-ready"
	m5Started    = "state-started"
	m5Done       = "state-done"
	m5RunEnv     = "DARK_FACTORY_M5_RUN"
	m5SeedsEnv   = "DARK_FACTORY_M5_SEEDS"
	m5ReceiptEnv = "DARK_FACTORY_M5_RECEIPT"
)

type faultBoundary struct {
	Name      string `json:"name"`
	Before    string `json:"before"`
	After     string `json:"after"`
	Journal   string `json:"journal_phase,omitempty"`
	RemoteKey string `json:"remote_key,omitempty"`
	Artifact  string `json:"artifact,omitempty"`
}

var m5Boundaries = []faultBoundary{
	{Name: "run create", Before: "before_run_created", After: "after_run_created", Journal: "run_created"},
	{Name: "lease acquire", Before: "before_lease_acquired", After: "after_lease_acquired", Journal: "lease_acquired"},
	{Name: "attempt reserve", Before: "before_attempt_reserved", After: "after_attempt_reserved", Journal: "attempt_reserved"},
	{Name: "dispatch start", Before: "before_dispatch_started", After: "after_dispatch_started", Journal: "dispatch_started"},
	{Name: "agent result snapshot", Before: "before_agent_result_snapshot", After: "after_agent_result_snapshot", Artifact: "openclaw"},
	{Name: "checkpoint", Before: "before_checkpoint_committed", After: "after_checkpoint_committed", Journal: "checkpoint_committed"},
	{Name: "review import", Before: "before_snapshot_install", After: "after_review_import", Artifact: "snapshot"},
	{Name: "review consume", Before: "before_review_consumed", After: "after_review_consumed", Journal: "review_consumed"},
	{Name: "review consumption receipt", Before: "before_consumption_receipt", After: "after_consumption_receipt", Artifact: "receipt"},
	{Name: "advancement freeze", Before: "before_advance_frozen", After: "after_advance_frozen", Journal: "advance_frozen"},
	{Name: "Linear claim comment", Before: "before_linear_claim_comment", After: "after_linear_claim_comment", RemoteKey: "comment:" + m5IssueOne + ":started"},
	{Name: "Linear claim state", Before: "before_linear_claim_state", After: "after_linear_claim_state", RemoteKey: "state:" + m5IssueOne + ":" + m5Started},
	{Name: "Linear completion comment", Before: "before_linear_complete_comment", After: "after_linear_complete_comment", RemoteKey: "comment:" + m5IssueOne + ":completed"},
	{Name: "Linear completion state", Before: "before_linear_complete_state", After: "after_linear_complete_state", RemoteKey: "state:" + m5IssueOne + ":" + m5Done},
	{Name: "Linear adoption comment", Before: "before_linear_adopt_comment", After: "after_linear_adopt_comment", RemoteKey: "comment:" + m5IssueTwo + ":started"},
	{Name: "Linear adoption state", Before: "before_linear_adopt_state", After: "after_linear_adopt_state", RemoteKey: "state:" + m5IssueTwo + ":" + m5Started},
	{Name: "Linear remote receipt", Before: "before_linear_remote_receipt", After: "after_linear_remote_receipt"},
	{Name: "local reconcile", Before: "before_advance_reconciled", After: "after_advance_reconciled", Journal: "advance_reconciled"},
	{Name: "completion", Before: "before_run_completed", After: "after_run_completed", Journal: "run_completed"},
}

type matrixSpec struct {
	Boundary faultBoundary `json:"boundary"`
	Side     string        `json:"side"`
	Point    string        `json:"point"`
}

type scenarioReceipt struct {
	Seed                  int64             `json:"seed"`
	Spec                  matrixSpec        `json:"spec"`
	Status                domain.RunStatus  `json:"status"`
	Attempts              int               `json:"attempts"`
	Fences                []uint64          `json:"fences"`
	JournalCounts         map[string]int    `json:"journal_counts"`
	RemoteMutationCounts  map[string]int    `json:"remote_mutation_counts"`
	PacketCount           int               `json:"packet_count"`
	SnapshotCount         int               `json:"snapshot_count"`
	ReceiptCount          int               `json:"receipt_count"`
	OpenClawProcesses     int               `json:"openclaw_processes"`
	QuickCheck            string            `json:"quick_check"`
	FaultPID              int               `json:"fault_pid"`
	FinalDaemonPID        int               `json:"final_daemon_pid"`
	FinalRemoteIssueState map[string]string `json:"final_remote_issue_state"`
	Duration              time.Duration     `json:"duration_ns"`
}

type matrixReceipt struct {
	GeneratedAt   string            `json:"generated_at"`
	Command       string            `json:"command"`
	SeedCount     int               `json:"seed_count"`
	Seeds         []int64           `json:"seeds"`
	BoundaryCount int               `json:"boundary_count"`
	ScenarioCount int               `json:"scenario_count"`
	BeforeCount   int               `json:"before_count"`
	AfterCount    int               `json:"after_count"`
	Complete      int               `json:"complete"`
	Blocked       int               `json:"blocked"`
	Scenarios     []scenarioReceipt `json:"scenarios"`
}

func TestM5ForcedRestartEndToEnd(t *testing.T) {
	if os.Getenv(m5RunEnv) != "1" {
		t.Skip("set DARK_FACTORY_M5_RUN=1 and provide at least 20 seeds for the retained M5 matrix")
	}
	seeds := parseSeeds(t, os.Getenv(m5SeedsEnv))
	if len(seeds) < 20 {
		t.Fatalf("M5 requires at least 20 seeds, got %d", len(seeds))
	}
	receiptPath := os.Getenv(m5ReceiptEnv)
	if !filepath.IsAbs(receiptPath) {
		t.Fatal("DARK_FACTORY_M5_RECEIPT must be an absolute path")
	}
	repo := commandOutput(t, "git", "rev-parse", "--show-toplevel")
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	binRoot := privateDir(t, root, "bin")
	factoryd := filepath.Join(binRoot, "factoryd-m5")
	openclaw := filepath.Join(binRoot, "fake-openclaw-m5")
	runCommand(t, repo, "go", "build", "-tags", "darkfactory_faultinject", "-o", factoryd, "./cmd/factoryd")
	runCommand(t, repo, "go", "build", "-o", openclaw, "./internal/adapters/openclaw/testdata/fake-openclaw")
	workspace := createWorkspace(t, root)
	source, err := filesystemadapter.InspectSource(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}

	specs := allMatrixSpecs()
	rng := rand.New(rand.NewSource(seedDigest(seeds)))
	rng.Shuffle(len(specs), func(i, j int) { specs[i], specs[j] = specs[j], specs[i] })
	for len(specs) < len(seeds)*2 {
		boundary := m5Boundaries[rng.Intn(len(m5Boundaries))]
		side := "before"
		point := boundary.Before
		if rng.Intn(2) == 1 {
			side, point = "after", boundary.After
		}
		specs = append(specs, matrixSpec{Boundary: boundary, Side: side, Point: point})
	}
	receipt := matrixReceipt{GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano), Command: "go test -race ./internal/e2e -run TestM5ForcedRestartEndToEnd -count=1 -v", Seeds: seeds, SeedCount: len(seeds), BoundaryCount: len(m5Boundaries)}
	for index, seed := range seeds {
		for _, spec := range specs[index*2 : index*2+2] {
			t.Logf("M5 scenario seed=%d point=%s", seed, spec.Point)
			scenario := runScenario(t, scenarioOptions{Root: root, Workspace: workspace, Source: source, Factoryd: factoryd, OpenClaw: openclaw, Seed: seed, Index: len(receipt.Scenarios), Spec: spec})
			receipt.Scenarios = append(receipt.Scenarios, scenario)
			if spec.Side == "before" {
				receipt.BeforeCount++
			} else {
				receipt.AfterCount++
			}
			switch scenario.Status {
			case domain.RunComplete:
				receipt.Complete++
			case domain.RunBlocked:
				receipt.Blocked++
			default:
				t.Fatalf("non-terminal scenario: %+v", scenario)
			}
		}
	}
	receipt.ScenarioCount = len(receipt.Scenarios)
	assertMatrixCoverage(t, receipt.Scenarios)
	encoded, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(receiptPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(receiptPath, append(encoded, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("M5_MATRIX_RECEIPT %s", encoded)
}

type scenarioOptions struct {
	Root, Workspace, Factoryd, OpenClaw string
	Source                              filesystemadapter.SourceState
	Seed                                int64
	Index                               int
	Spec                                matrixSpec
}

func runScenario(t *testing.T, options scenarioOptions) scenarioReceipt {
	t.Helper()
	started := time.Now()
	root := privateDir(t, options.Root, fmt.Sprintf("scenario-%03d-%d", options.Index, options.Seed))
	server := startFakeLinear(t, root)
	defer server.stop(t)
	cfgPath, cfg := writeScenarioConfig(t, root, options.Workspace, options.OpenClaw, server.endpoint)
	runID := app.StableRunID(cfg)
	marker := filepath.Join(root, "fault-marker.json")
	transcript := filepath.Join(root, "openclaw-transcript.jsonl")
	pumpErrors := make(chan error, 1)
	createdPackets := map[string]bool{}
	startPump := func() (context.CancelFunc, <-chan struct{}) {
		pumpCtx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			packetPump(pumpCtx, cfg, runID, options.Source, createdPackets, pumpErrors)
		}()
		return cancel, done
	}
	cancelPump, pumpDone := startPump()

	first := startDaemon(t, options.Factoryd, cfgPath, options.Spec.Point, marker, transcript)
	witness := waitForFault(t, first, marker, pumpErrors, 20*time.Second)
	if witness.Phase != options.Spec.Point || witness.PID != first.pid {
		t.Fatalf("fault witness=%+v process=%d point=%s", witness, first.pid, options.Spec.Point)
	}
	cancelPump()
	<-pumpDone
	if err := first.command.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	first.waitKilled(t)
	if err := unix.Kill(witness.PID, 0); !errors.Is(err, unix.ESRCH) {
		t.Fatalf("stale daemon %d survived SIGKILL: %v", witness.PID, err)
	}
	afterKill := inspectScenario(t, cfg, runID, server.auditPath)
	assertFaultSide(t, options.Spec, afterKill)

	second := startDaemon(t, options.Factoryd, cfgPath, options.Spec.Point, marker, transcript)
	// The replacement owns startup recovery before the read-only packet producer
	// resumes. This delay does not place the fault; the fsynced phase witness above does.
	time.Sleep(75 * time.Millisecond)
	cancelPump, pumpDone = startPump()
	second.waitTerminal(t, pumpErrors, 30*time.Second)
	cancelPump()
	<-pumpDone
	if err := unix.Kill(second.pid, 0); !errors.Is(err, unix.ESRCH) {
		t.Fatalf("replacement daemon %d leaked: %v", second.pid, err)
	}
	select {
	case err := <-pumpErrors:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	default:
	}
	final := inspectScenario(t, cfg, runID, server.auditPath)
	assertScenario(t, cfg, runID, transcript, final)
	packetCount, snapshotCount := reverifyPackets(t, cfg)
	return scenarioReceipt{
		Seed: options.Seed, Spec: options.Spec, Status: final.Run.Status, Attempts: final.Run.Attempts,
		Fences: final.Fences, JournalCounts: final.JournalCounts, RemoteMutationCounts: final.Audit.Mutations,
		PacketCount: packetCount, SnapshotCount: snapshotCount, ReceiptCount: final.ReceiptCount,
		OpenClawProcesses: assertOpenClawReaped(t, transcript), QuickCheck: final.QuickCheck,
		FaultPID: witness.PID, FinalDaemonPID: second.pid, FinalRemoteIssueState: final.Audit.States,
		Duration: time.Since(started),
	}
}

func allMatrixSpecs() []matrixSpec {
	result := make([]matrixSpec, 0, len(m5Boundaries)*2)
	for _, boundary := range m5Boundaries {
		result = append(result, matrixSpec{Boundary: boundary, Side: "before", Point: boundary.Before})
		result = append(result, matrixSpec{Boundary: boundary, Side: "after", Point: boundary.After})
	}
	return result
}

func assertMatrixCoverage(t *testing.T, scenarios []scenarioReceipt) {
	t.Helper()
	covered := map[string]bool{}
	for _, scenario := range scenarios {
		covered[scenario.Spec.Point] = true
	}
	for _, boundary := range m5Boundaries {
		for _, point := range []string{boundary.Before, boundary.After} {
			if !covered[point] {
				t.Errorf("matrix did not cover %s", point)
			}
		}
	}
}

type daemonProcess struct {
	command        *exec.Cmd
	pid            int
	stdout, stderr bytes.Buffer
	done           chan error
}

func startDaemon(t *testing.T, binary, configPath, point, marker, transcript string) *daemonProcess {
	t.Helper()
	command := exec.Command(binary, "--apply", "--config", configPath)
	command.Env = append(withoutEnvironment("LINEAR_API_KEY", "DARK_FACTORY_M5_FAULT_POINT", "DARK_FACTORY_M5_FAULT_MARKER", "FAKE_OPENCLAW_TRANSCRIPT", "FAKE_OPENCLAW_TRANSCRIPT_APPEND"),
		"LINEAR_API_KEY="+m5Token, "DARK_FACTORY_M5_FAULT_POINT="+point, "DARK_FACTORY_M5_FAULT_MARKER="+marker,
		"FAKE_OPENCLAW_TRANSCRIPT="+transcript, "FAKE_OPENCLAW_TRANSCRIPT_APPEND=1")
	process := &daemonProcess{command: command, done: make(chan error, 1)}
	command.Stdout, command.Stderr = &process.stdout, &process.stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process.pid = command.Process.Pid
	go func() { process.done <- command.Wait() }()
	return process
}

func (p *daemonProcess) waitKilled(t *testing.T) {
	t.Helper()
	select {
	case err := <-p.done:
		if err == nil {
			t.Fatalf("SIGKILLed daemon %d exited successfully", p.pid)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("SIGKILLed daemon %d was not reaped", p.pid)
	}
}

func (p *daemonProcess) waitTerminal(t *testing.T, pumpErrors <-chan error, timeout time.Duration) {
	t.Helper()
	select {
	case err := <-p.done:
		if err != nil {
			t.Fatalf("replacement daemon failed: %v\nstdout=%s\nstderr=%s", err, p.stdout.String(), p.stderr.String())
		}
	case err := <-pumpErrors:
		t.Fatalf("packet pump failed: %v", err)
	case <-time.After(timeout):
		_ = p.command.Process.Signal(syscall.SIGKILL)
		t.Fatalf("replacement daemon timed out\nstdout=%s\nstderr=%s", p.stdout.String(), p.stderr.String())
	}
}

type faultWitness struct {
	Phase string `json:"phase"`
	PID   int    `json:"pid"`
}

func waitForFault(t *testing.T, process *daemonProcess, marker string, pumpErrors <-chan error, timeout time.Duration) faultWitness {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case err := <-process.done:
			t.Fatalf("daemon exited before fault: %v\nstdout=%s\nstderr=%s", err, process.stdout.String(), process.stderr.String())
		case err := <-pumpErrors:
			t.Fatalf("packet pump failed before fault: %v", err)
		case <-ticker.C:
			raw, err := os.ReadFile(marker)
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				t.Fatal(err)
			}
			var witness faultWitness
			if err := json.Unmarshal(raw, &witness); err != nil {
				t.Fatal(err)
			}
			return witness
		case <-deadline.C:
			_ = process.command.Process.Signal(syscall.SIGKILL)
			t.Fatalf("fault %s was not observed\nstdout=%s\nstderr=%s", os.Getenv("DARK_FACTORY_M5_FAULT_POINT"), process.stdout.String(), process.stderr.String())
		}
	}
}

func withoutEnvironment(names ...string) []string {
	blocked := map[string]bool{}
	for _, name := range names {
		blocked[name] = true
	}
	var result []string
	for _, value := range os.Environ() {
		name, _, _ := strings.Cut(value, "=")
		if !blocked[name] {
			result = append(result, value)
		}
	}
	return result
}

func parseSeeds(t *testing.T, raw string) []int64 {
	t.Helper()
	seen := map[int64]bool{}
	var seeds []int64
	for _, part := range strings.Split(raw, ",") {
		seed, err := strconv.ParseInt(strings.TrimSpace(part), 10, 64)
		if err != nil || seen[seed] {
			t.Fatalf("invalid or duplicate M5 seed %q", part)
		}
		seen[seed] = true
		seeds = append(seeds, seed)
	}
	return seeds
}

func seedDigest(seeds []int64) int64 {
	hash := sha256.New()
	for _, seed := range seeds {
		_, _ = fmt.Fprintf(hash, "%d\x00", seed)
	}
	sum := hash.Sum(nil)
	var value int64
	for _, b := range sum[:8] {
		value = value<<8 | int64(b)
	}
	return value
}

func privateDir(t *testing.T, parent, name string) string {
	t.Helper()
	path := filepath.Join(parent, name)
	if err := os.MkdirAll(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	return path
}

func commandOutput(t *testing.T, name string, args ...string) string {
	t.Helper()
	output, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, output)
	}
	return strings.TrimSpace(string(output))
}

func runCommand(t *testing.T, directory, name string, args ...string) {
	t.Helper()
	command := exec.Command(name, args...)
	command.Dir = directory
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, output)
	}
}

func createWorkspace(t *testing.T, root string) string {
	t.Helper()
	workspace := privateDir(t, root, "workspace")
	if err := os.WriteFile(filepath.Join(workspace, "candidate.txt"), []byte("M5 immutable source fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runCommand(t, workspace, "git", "init", "-q")
	runCommand(t, workspace, "git", "add", "candidate.txt")
	runCommand(t, workspace, "git", "-c", "user.name=M5 Fixture", "-c", "user.email=m5@example.invalid", "commit", "-q", "-m", "fixture")
	return workspace
}

func writeScenarioConfig(t *testing.T, root, workspace, openclaw, endpoint string) (string, config.Config) {
	t.Helper()
	path := filepath.Join(root, "factory.json")
	cfg, err := config.Default(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg.Mode = "live"
	cfg.Paths = config.Paths{
		StateDB: filepath.Join(root, "state", "factory.db"), StateRoot: filepath.Join(root, "state"),
		ArtifactRoot: filepath.Join(root, "artifacts"), ReviewRoot: filepath.Join(root, "reviews"),
		WorkspaceRoot: workspace, AllowedRoots: []string{filepath.Dir(root)},
	}
	cfg.Scope = config.Scope{TeamID: m5Team, ProjectID: m5Project, IssueID: m5IssueOne, IssueAllowlist: []string{m5IssueOne, m5IssueTwo}}
	cfg.Linear = config.Linear{Endpoint: endpoint + "/graphql", APIKey: "env:LINEAR_API_KEY"}
	cfg.OpenClaw = config.OpenClaw{Executable: openclaw, Agent: "main", SessionPrefix: "agent:main:m5", Timeout: "250ms"}
	cfg.Budgets = config.Budgets{LeaseDuration: "500ms", MaxAttempts: 2, MaxConsecutiveFailures: 2, MaxRunDuration: "30s", PollInterval: "10ms", InitialBackoff: "5ms", MaxBackoff: "20ms", ShutdownTimeout: "50ms"}
	cfg.Lifecycle = config.Lifecycle{Ready: "Ready", InProgress: "In Progress", Done: "Done"}
	cfg.Limits = config.Limits{MaxOutputBytes: 8192, MaxArtifactBytes: 1 << 20, MaxPacketBytes: 4 << 20, MaxArtifacts: 32}
	if err := config.WriteNew(path, cfg); err != nil {
		t.Fatal(err)
	}
	return path, cfg
}

func packetPump(ctx context.Context, cfg config.Config, runID string, source filesystemadapter.SourceState, created map[string]bool, failures chan<- error) {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			run, err := readRun(cfg.Paths.StateDB, runID)
			if err != nil || run.Status != domain.RunActive || run.CheckpointSequence == 0 || run.LastTurnArtifact == nil {
				continue
			}
			reviewID := filesystemadapter.ReviewID(run.ID, run.IssueID, run.CheckpointSequence)
			if created[reviewID] {
				continue
			}
			if err := createPacket(ctx, cfg, run, source); err != nil {
				if ctx.Err() != nil {
					return
				}
				select {
				case failures <- err:
				default:
				}
				return
			}
			created[reviewID] = true
		}
	}
}

func readRun(path, runID string) (domain.Run, error) {
	return durablesqlite.ReadRun(context.Background(), path, runID)
}

func createPacket(ctx context.Context, cfg config.Config, run domain.Run, source filesystemadapter.SourceState) error {
	artifact, err := os.ReadFile(run.LastTurnArtifact.ResponseRef)
	if err != nil {
		return err
	}
	if digestBytes(artifact) != run.LastTurnArtifact.ResponseSHA256 {
		return errors.New("OpenClaw artifact changed before review packet")
	}
	reviewID := filesystemadapter.ReviewID(run.ID, run.IssueID, run.CheckpointSequence)
	sourceDigest, err := filesystemadapter.SourceDigest(source.Claim)
	if err != nil {
		return err
	}
	review := filesystemadapter.ReviewReceipt{
		ReceiptVersion: filesystemadapter.ReceiptVersion, ReviewID: reviewID, ProjectID: run.ProjectID, IssueID: run.IssueID,
		RunID: run.ID, CheckpointSequence: run.CheckpointSequence, SourceCommit: source.Claim.Commit, SourceDigest: sourceDigest,
		ArtifactPath: "response.json", ArtifactSHA256: run.LastTurnArtifact.ResponseSHA256, Verdict: "approved",
		Checks:   []string{"M5 process harness verified response"},
		Author:   filesystemadapter.Identity{Provider: "openai", Model: "gpt-m5-author"},
		Reviewer: filesystemadapter.Identity{Provider: "anthropic", Model: "claude-m5-reviewer"},
	}
	reviewBytes, err := filesystemadapter.CanonicalReviewReceipt(review)
	if err != nil {
		return err
	}
	files := map[string][]byte{"response.json": artifact, "review.json": reviewBytes, "source.diff": source.Diff, "tests.txt": []byte("go test -race M5 forced-restart matrix\n")}
	paths := make([]string, 0, len(files))
	for path := range files {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	members := make([]filesystemadapter.Member, 0, len(paths))
	for _, path := range paths {
		kind := "artifact"
		switch path {
		case "review.json":
			kind = "review_receipt"
		case "source.diff":
			kind = "source_diff"
		case "tests.txt":
			kind = "test_receipt"
		}
		members = append(members, filesystemadapter.Member{Path: path, Kind: kind, SHA256: digestBytes(files[path]), Size: int64(len(files[path]))})
	}
	manifest := filesystemadapter.Manifest{PacketVersion: filesystemadapter.PacketVersion, ReviewID: reviewID, ProjectID: run.ProjectID, IssueID: run.IssueID, RunID: run.ID, CheckpointSequence: run.CheckpointSequence, Source: source.Claim, Members: members}
	manifestBytes, err := filesystemadapter.CanonicalManifest(manifest)
	if err != nil {
		return err
	}
	pending, err := os.MkdirTemp(cfg.Paths.ReviewRoot, ".pending-m5-")
	if err != nil {
		return err
	}
	if err := os.Chmod(pending, 0o700); err != nil {
		return err
	}
	files[filesystemadapter.ManifestName] = manifestBytes
	for path, contents := range files {
		if err := os.WriteFile(filepath.Join(pending, path), contents, 0o600); err != nil {
			return err
		}
	}
	_, _, err = filesystemadapter.FinalizePacket(ctx, cfg.Paths.ReviewRoot, pending, cfg.Paths.WorkspaceRoot, filesystemadapter.Limits{MaxMemberBytes: cfg.Limits.MaxArtifactBytes, MaxPacketBytes: cfg.Limits.MaxPacketBytes, MaxMembers: cfg.Limits.MaxArtifacts})
	return err
}

func digestBytes(contents []byte) string { return fmt.Sprintf("%x", sha256.Sum256(contents)) }

type inspectedScenario struct {
	Run                                                                      domain.Run
	RunExists                                                                bool
	QuickCheck                                                               string
	Fences                                                                   []uint64
	JournalCounts                                                            map[string]int
	AttemptRows, DistinctAttempts, ReviewRows, ConsumedReviews, ReceiptCount int
	ArtifactCount, SnapshotCount                                             int
	PendingSelections                                                        []string
	Audit                                                                    fakeLinearAudit
}

func inspectScenario(t *testing.T, cfg config.Config, runID, auditPath string) inspectedScenario {
	t.Helper()
	db, err := sql.Open("sqlite", cfg.Paths.StateDB)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("PRAGMA busy_timeout = 5000"); err != nil {
		t.Fatalf("configure forensic busy timeout: %v", err)
	}
	result := inspectedScenario{JournalCounts: map[string]int{}}
	if err := db.QueryRow("PRAGMA quick_check").Scan(&result.QuickCheck); err != nil {
		t.Fatal(err)
	}
	var payload []byte
	if err := db.QueryRow(`SELECT payload FROM runs WHERE id = ?`, runID).Scan(&payload); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			t.Fatal(err)
		}
	} else {
		result.RunExists = true
		if err := json.Unmarshal(payload, &result.Run); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := db.Query(`SELECT phase, payload FROM journal WHERE run_id = ? ORDER BY sequence`, runID)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var phase string
		var journalPayload []byte
		if err := rows.Scan(&phase, &journalPayload); err != nil {
			t.Fatal(err)
		}
		result.JournalCounts[phase]++
		var journalRun domain.Run
		if json.Unmarshal(journalPayload, &journalRun) == nil && journalRun.ID != "" {
			if phase == "lease_acquired" {
				result.Fences = append(result.Fences, journalRun.Lease.Fence)
			}
			if journalRun.PendingAdvance != nil {
				result.PendingSelections = append(result.PendingSelections, journalRun.PendingAdvance.NextIssueID)
			}
		}
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*), count(DISTINCT attempt) FROM attempt_reservations WHERE run_id = ?`, runID).Scan(&result.AttemptRows, &result.DistinctAttempts); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*), coalesce(sum(CASE WHEN consumed_by_run = ? THEN 1 ELSE 0 END), 0) FROM reviews`, runID).Scan(&result.ReviewRows, &result.ConsumedReviews); err != nil {
		t.Fatal(err)
	}
	artifactEntries, err := os.ReadDir(cfg.Paths.ArtifactRoot)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range artifactEntries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), "openclaw-") {
			result.ArtifactCount++
		}
	}
	result.SnapshotCount = countPacketDirectories(filepath.Join(cfg.Paths.StateRoot, "review-packets"))
	entries, err := os.ReadDir(filepath.Join(cfg.Paths.StateRoot, "review-receipts"))
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !entry.IsDir() && !strings.HasPrefix(entry.Name(), "pending-") {
			result.ReceiptCount++
		}
	}
	raw, err := os.ReadFile(auditPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &result.Audit); err != nil {
		t.Fatal(err)
	}
	return result
}

func assertFaultSide(t *testing.T, spec matrixSpec, state inspectedScenario) {
	t.Helper()
	wantCommitted := spec.Side == "after"
	if spec.Boundary.Journal != "" {
		committed := state.JournalCounts[spec.Boundary.Journal] > 0
		if committed != wantCommitted {
			t.Fatalf("fault %s journal %s count=%d", spec.Point, spec.Boundary.Journal, state.JournalCounts[spec.Boundary.Journal])
		}
	}
	if spec.Boundary.RemoteKey != "" {
		count := 0
		for key, value := range state.Audit.Mutations {
			if strings.HasPrefix(key, spec.Boundary.RemoteKey) {
				count += value
			}
		}
		if (count > 0) != wantCommitted {
			t.Fatalf("fault %s remote prefix %s mutations=%v", spec.Point, spec.Boundary.RemoteKey, state.Audit.Mutations)
		}
	}
	switch spec.Boundary.Artifact {
	case "openclaw":
		if (state.ArtifactCount > 0) != wantCommitted {
			t.Fatalf("fault %s artifact count=%d", spec.Point, state.ArtifactCount)
		}
	case "snapshot":
		if (state.SnapshotCount > 0) != wantCommitted {
			t.Fatalf("fault %s snapshot count=%d", spec.Point, state.SnapshotCount)
		}
	case "receipt":
		if (state.ReceiptCount > 0) != wantCommitted {
			t.Fatalf("fault %s receipt count=%d", spec.Point, state.ReceiptCount)
		}
	}
}

func assertScenario(t *testing.T, cfg config.Config, runID, transcript string, state inspectedScenario) {
	t.Helper()
	if !state.RunExists {
		t.Fatal("terminal run is missing")
	}
	if state.QuickCheck != "ok" {
		t.Fatalf("quick_check=%q", state.QuickCheck)
	}
	previous := uint64(0)
	for _, fence := range state.Fences {
		if fence <= previous {
			t.Fatalf("fence regressed: %v", state.Fences)
		}
		previous = fence
	}
	if state.Run.NextFence < state.Run.Lease.Fence {
		t.Fatalf("invalid final fence: %+v", state.Run.Lease)
	}
	if state.AttemptRows != state.Run.Attempts || state.DistinctAttempts != state.Run.Attempts {
		t.Fatalf("attempts run=%d rows=%d distinct=%d", state.Run.Attempts, state.AttemptRows, state.DistinctAttempts)
	}
	if state.Run.Attempts > cfg.Budgets.MaxAttempts {
		t.Fatalf("attempt budget exceeded: %d", state.Run.Attempts)
	}
	for _, selection := range state.PendingSelections {
		if selection != "" && selection != m5IssueTwo {
			t.Fatalf("frozen selection changed: %v", state.PendingSelections)
		}
	}
	for key, count := range state.Audit.Mutations {
		if count > 1 {
			t.Fatalf("duplicate remote mutation %s=%d", key, count)
		}
	}
	for issue, comments := range state.Audit.Comments {
		seen := map[string]bool{}
		for _, comment := range comments {
			if seen[comment] {
				t.Fatalf("duplicate comment on %s", issue)
			}
			seen[comment] = true
		}
	}
	switch state.Run.Status {
	case domain.RunComplete:
		if state.Run.IssueID != m5IssueTwo || state.Audit.States[m5IssueOne] != m5Done || state.Audit.States[m5IssueTwo] != m5Done || state.ReviewRows != 2 || state.ConsumedReviews != 2 || state.ReceiptCount != 2 {
			t.Fatalf("completed local/remote mismatch: %+v audit=%+v", state, state.Audit)
		}
	case domain.RunBlocked:
		if state.Run.BlockedReason == "" || state.Run.PendingDispatch != nil || state.Audit.States[state.Run.IssueID] != m5Started {
			t.Fatalf("blocked local/remote mismatch: %+v audit=%+v", state.Run, state.Audit)
		}
	default:
		t.Fatalf("non-terminal run: %+v", state.Run)
	}
	_ = assertOpenClawReaped(t, transcript)
	_ = runID
}

func reverifyPackets(t *testing.T, cfg config.Config) (int, int) {
	t.Helper()
	limits := filesystemadapter.Limits{MaxMemberBytes: cfg.Limits.MaxArtifactBytes, MaxPacketBytes: cfg.Limits.MaxPacketBytes, MaxMembers: cfg.Limits.MaxArtifacts}
	verifyRoot := func(root string, skipSource bool) int {
		entries, err := os.ReadDir(root)
		if err != nil {
			t.Fatal(err)
		}
		count := 0
		for _, entry := range entries {
			if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "packet-") {
				continue
			}
			options := filesystemadapter.VerifyOptions{Limits: limits, ExpectedUID: os.Geteuid(), SkipSource: skipSource}
			if !skipSource {
				options.Workspace = cfg.Paths.WorkspaceRoot
			}
			if _, err := filesystemadapter.VerifyPacket(context.Background(), filepath.Join(root, entry.Name()), options); err != nil {
				t.Fatalf("reverify %s: %v", entry.Name(), err)
			}
			count++
		}
		return count
	}
	packets := verifyRoot(cfg.Paths.ReviewRoot, false)
	snapshots := verifyRoot(filepath.Join(cfg.Paths.StateRoot, "review-packets"), true)
	if packets != snapshots {
		t.Fatalf("packet/snapshot cardinality %d/%d", packets, snapshots)
	}
	return packets, snapshots
}

func assertOpenClawReaped(t *testing.T, path string) int {
	t.Helper()
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0
	}
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	count := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record struct {
			PID                 int  `json:"pid"`
			LinearSecretPresent bool `json:"linear_secret_present"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatal(err)
		}
		if record.LinearSecretPresent {
			t.Fatal("Linear secret reached fake OpenClaw")
		}
		if err := unix.Kill(record.PID, 0); !errors.Is(err, unix.ESRCH) {
			t.Fatalf("OpenClaw child %d leaked: %v", record.PID, err)
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return count
}

func countDirectories(root string) int {
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() {
			count++
		}
	}
	return count
}

func countPacketDirectories(root string) int {
	entries, err := os.ReadDir(root)
	if err != nil {
		return 0
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "packet-") {
			count++
		}
	}
	return count
}

type fakeLinearIssue struct {
	ID, Identifier, Title, CreatedAt string
	Priority                         int
	StateID, StateName, StateType    string
}

type fakeLinearAudit struct {
	PID       int                 `json:"pid"`
	Path      string              `json:"-"`
	States    map[string]string   `json:"states"`
	Comments  map[string][]string `json:"comments"`
	Mutations map[string]int      `json:"mutations"`
	Requests  []string            `json:"requests"`
}

type fakeLinearState struct {
	mu        sync.Mutex
	issues    map[string]*fakeLinearIssue
	audit     fakeLinearAudit
	auditPath string
}

type graphQLRequest struct {
	OperationName string         `json:"operationName"`
	Variables     map[string]any `json:"variables"`
}

func TestM5FakeLinearProcess(t *testing.T) {
	if os.Getenv("DARK_FACTORY_M5_FAKE_LINEAR") != "1" {
		t.Skip("M5 helper process")
	}
	readyPath := os.Getenv("DARK_FACTORY_M5_FAKE_READY")
	auditPath := os.Getenv("DARK_FACTORY_M5_FAKE_AUDIT")
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	state := newFakeLinearState(auditPath)
	if err := state.persist(); err != nil {
		t.Fatal(err)
	}
	endpoint := "http://" + listener.Addr().String()
	if err := os.WriteFile(readyPath, []byte(endpoint+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: http.HandlerFunc(state.handle), ReadHeaderTimeout: time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() { <-ctx.Done(); _ = server.Shutdown(context.Background()) }()
	err = server.Serve(listener)
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

func newFakeLinearState(path string) *fakeLinearState {
	created := "2026-08-22T10:00:00Z"
	return &fakeLinearState{
		issues: map[string]*fakeLinearIssue{
			m5IssueOne: {ID: m5IssueOne, Identifier: "DF-M5-1", Title: "M5 first", CreatedAt: created, Priority: 1, StateID: m5Ready, StateName: "Ready", StateType: "unstarted"},
			m5IssueTwo: {ID: m5IssueTwo, Identifier: "DF-M5-2", Title: "M5 second", CreatedAt: "2026-08-22T10:00:01Z", Priority: 2, StateID: m5Ready, StateName: "Ready", StateType: "unstarted"},
		},
		audit:     fakeLinearAudit{PID: os.Getpid(), States: map[string]string{m5IssueOne: m5Ready, m5IssueTwo: m5Ready}, Comments: map[string][]string{}, Mutations: map[string]int{}},
		auditPath: path,
	}
}

func (s *fakeLinearState) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/audit" {
		raw, err := os.ReadFile(s.auditPath)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(raw)
		return
	}
	if r.URL.Path != "/graphql" || r.Method != http.MethodPost || r.Header.Get("Authorization") != m5Token {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	var request graphQLRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&request); err != nil {
		http.Error(w, "bad json", 400)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audit.Requests = append(s.audit.Requests, request.OperationName)
	w.Header().Set("Content-Type", "application/json")
	var data any
	switch request.OperationName {
	case "DarkFactoryStates":
		data = map[string]any{"team": map[string]any{"id": m5Team, "states": map[string]any{"nodes": []map[string]any{
			{"id": m5Ready, "name": "Ready", "type": "unstarted", "position": 1},
			{"id": m5Started, "name": "In Progress", "type": "started", "position": 2},
			{"id": m5Done, "name": "Done", "type": "completed", "position": 3},
		}, "pageInfo": pageInfo()}}}
	case "DarkFactoryIssue":
		data = map[string]any{"issue": s.node(s.find(stringVariable(request, "id")))}
	case "DarkFactoryIssues":
		data = map[string]any{"project": map[string]any{"id": m5Project, "issues": map[string]any{"nodes": []any{s.node(s.issues[m5IssueOne]), s.node(s.issues[m5IssueTwo])}, "pageInfo": pageInfo()}}}
	case "DarkFactoryRelations":
		issue := s.find(stringVariable(request, "id"))
		node := s.node(issue)
		data = map[string]any{"issue": map[string]any{"id": node["id"], "identifier": node["identifier"], "project": node["project"], "team": node["team"], "relations": map[string]any{"nodes": []any{}, "pageInfo": pageInfo()}}}
	case "DarkFactoryComments":
		issue := s.find(stringVariable(request, "id"))
		node := s.node(issue)
		var comments []map[string]any
		for index, body := range s.audit.Comments[issue.ID] {
			comments = append(comments, map[string]any{"id": fmt.Sprintf("comment-%d", index+1), "body": body})
		}
		data = map[string]any{"issue": map[string]any{"id": node["id"], "identifier": node["identifier"], "project": node["project"], "team": node["team"], "comments": map[string]any{"nodes": comments, "pageInfo": pageInfo()}}}
	case "DarkFactoryCreateComment":
		input, _ := request.Variables["input"].(map[string]any)
		issueID, _ := input["issueId"].(string)
		body, _ := input["body"].(string)
		key := "comment:" + issueID + ":" + commentTransition(body)
		s.audit.Mutations[key]++
		s.audit.Comments[issueID] = append(s.audit.Comments[issueID], body)
		data = map[string]any{"commentCreate": map[string]any{"success": true, "comment": map[string]any{"id": fmt.Sprintf("comment-%d", len(s.audit.Comments[issueID]))}}}
	case "DarkFactoryUpdateIssue":
		issueID := stringVariable(request, "id")
		input, _ := request.Variables["input"].(map[string]any)
		stateID, _ := input["stateId"].(string)
		s.audit.Mutations["state:"+issueID+":"+stateID]++
		s.setState(s.find(issueID), stateID)
		data = map[string]any{"issueUpdate": map[string]any{"success": true, "issue": s.node(s.find(issueID))}}
	default:
		http.Error(w, "unexpected operation", 400)
		return
	}
	if err := s.persistLocked(); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"data": data})
}

func (s *fakeLinearState) find(reference string) *fakeLinearIssue {
	if issue := s.issues[reference]; issue != nil {
		return issue
	}
	for _, issue := range s.issues {
		if issue.Identifier == reference {
			return issue
		}
	}
	return nil
}

func (s *fakeLinearState) node(issue *fakeLinearIssue) map[string]any {
	if issue == nil {
		return nil
	}
	return map[string]any{"id": issue.ID, "identifier": issue.Identifier, "title": issue.Title, "priority": issue.Priority, "createdAt": issue.CreatedAt,
		"project": map[string]any{"id": m5Project}, "team": map[string]any{"id": m5Team}, "state": map[string]any{"id": issue.StateID, "name": issue.StateName, "type": issue.StateType}}
}

func (s *fakeLinearState) setState(issue *fakeLinearIssue, stateID string) {
	switch stateID {
	case m5Ready:
		issue.StateID, issue.StateName, issue.StateType = m5Ready, "Ready", "unstarted"
	case m5Started:
		issue.StateID, issue.StateName, issue.StateType = m5Started, "In Progress", "started"
	case m5Done:
		issue.StateID, issue.StateName, issue.StateType = m5Done, "Done", "completed"
	}
	s.audit.States[issue.ID] = issue.StateID
}

func (s *fakeLinearState) persist() error { s.mu.Lock(); defer s.mu.Unlock(); return s.persistLocked() }

func (s *fakeLinearState) persistLocked() error {
	raw, err := json.MarshalIndent(s.audit, "", "  ")
	if err != nil {
		return err
	}
	temporary := s.auditPath + ".tmp"
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(append(raw, '\n')); err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(temporary, s.auditPath)
	}
	if err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(s.auditPath))
	if err != nil {
		return err
	}
	err = directory.Sync()
	_ = directory.Close()
	return err
}

func pageInfo() map[string]any { return map[string]any{"hasNextPage": false, "endCursor": ""} }

func commentTransition(body string) string {
	first, _, _ := strings.Cut(body, "\n")
	raw := strings.TrimSuffix(strings.TrimPrefix(first, "<!-- dark-factory:"), " -->")
	var marker struct {
		Transition string `json:"transition"`
	}
	if json.Unmarshal([]byte(raw), &marker) != nil || marker.Transition == "" {
		return "invalid-" + digestBytes([]byte(body))
	}
	return marker.Transition
}

func stringVariable(request graphQLRequest, name string) string {
	value, _ := request.Variables[name].(string)
	return value
}

type fakeLinearProcess struct {
	command             *exec.Cmd
	endpoint, auditPath string
	done                chan error
}

func startFakeLinear(t *testing.T, root string) *fakeLinearProcess {
	t.Helper()
	ready := filepath.Join(root, "linear-ready")
	audit := filepath.Join(root, "linear-audit.json")
	command := exec.Command(os.Args[0], "-test.run=^TestM5FakeLinearProcess$", "-test.v=false")
	command.Env = append(withoutEnvironment("DARK_FACTORY_M5_FAKE_LINEAR", "DARK_FACTORY_M5_FAKE_READY", "DARK_FACTORY_M5_FAKE_AUDIT"),
		"DARK_FACTORY_M5_FAKE_LINEAR=1", "DARK_FACTORY_M5_FAKE_READY="+ready, "DARK_FACTORY_M5_FAKE_AUDIT="+audit)
	var output bytes.Buffer
	command.Stdout, command.Stderr = &output, &output
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	process := &fakeLinearProcess{command: command, auditPath: audit, done: make(chan error, 1)}
	go func() { process.done <- command.Wait() }()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(ready)
		if err == nil {
			process.endpoint = strings.TrimSpace(string(raw))
			return process
		}
		select {
		case err := <-process.done:
			t.Fatalf("fake Linear exited: %v\n%s", err, output.String())
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = command.Process.Signal(syscall.SIGKILL)
	t.Fatal("fake Linear did not become ready")
	return nil
}

func (p *fakeLinearProcess) stop(t *testing.T) {
	t.Helper()
	if err := p.command.Process.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) {
		t.Fatal(err)
	}
	select {
	case err := <-p.done:
		if err != nil {
			t.Fatalf("fake Linear shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		_ = p.command.Process.Signal(syscall.SIGKILL)
		t.Fatal("fake Linear leaked")
	}
	if err := unix.Kill(p.command.Process.Pid, 0); !errors.Is(err, unix.ESRCH) {
		t.Fatalf("fake Linear PID %d leaked: %v", p.command.Process.Pid, err)
	}
}
