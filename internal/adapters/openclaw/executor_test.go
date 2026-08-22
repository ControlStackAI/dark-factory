package openclaw

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"golang.org/x/sys/unix"
)

type fakeTranscript struct {
	Executable          string   `json:"executable"`
	Args                []string `json:"args"`
	Prompt              string   `json:"prompt"`
	PromptMode          uint32   `json:"prompt_mode"`
	PID                 int      `json:"pid"`
	LinearSecretPresent bool     `json:"linear_secret_present"`
}

func TestExecutorArgvPromptIsolationAndArtifact(t *testing.T) {
	rig := newExecutorRig(t, 8192, 2*time.Second)
	t.Setenv("LINEAR_API_KEY", "must-not-reach-openclaw")
	rig.executor.options.StripEnvironment = []string{"LINEAR_API_KEY"}
	sentinel := filepath.Join(rig.root, "SENTINEL")
	prompt := "quotes '\" $() `touch " + sentinel + "` ;|&<> $(touch " + sentinel + ")"
	rig.executor.options.PromptBuilder = func(domain.TurnRequest) (string, error) { return prompt, nil }
	result, err := rig.executor.ExecuteTurn(context.Background(), request(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	transcript := rig.transcript(t)
	if transcript.Executable != rig.executor.options.Executable || transcript.Prompt != prompt || transcript.PromptMode != 0o600 || transcript.LinearSecretPresent {
		t.Fatalf("prompt=%q mode=%04o", transcript.Prompt, transcript.PromptMode)
	}
	if _, err := os.Stat(sentinel); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("shell sentinel executed: %v", err)
	}
	if slices.Contains(transcript.Args, "--deliver") || slices.Contains(transcript.Args, "--channel") || slices.Contains(transcript.Args, "--to") {
		t.Fatalf("delivery/routing argument present: %q", transcript.Args)
	}
	for _, arg := range transcript.Args {
		if strings.Contains(arg, "touch ") || arg == prompt {
			t.Fatalf("prompt leaked into argv: %q", transcript.Args)
		}
	}
	wantPrefix := []string{"agent", "--agent", "agent 'quotes' $()", "--session-key"}
	if len(transcript.Args) < len(wantPrefix) || !slices.Equal(transcript.Args[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("argv prefix=%q", transcript.Args)
	}
	if !slices.Contains(transcript.Args, "--message-file") || !slices.Contains(transcript.Args, "--json") || !slices.Contains(transcript.Args, "--timeout") {
		t.Fatalf("required argv absent: %q", transcript.Args)
	}
	artifact, err := os.ReadFile(result.ResponseRef)
	if err != nil || len(artifact) == 0 || result.ResponseSHA256 == "" || result.SessionKey == "" {
		t.Fatalf("artifact result=%+v read=%v", result, err)
	}
	entries, err := os.ReadDir(rig.promptRoot)
	if err != nil || len(entries) != 0 {
		t.Fatalf("prompt cleanup entries=%v err=%v", entries, err)
	}
	second, err := rig.executor.ExecuteTurn(context.Background(), request(2, 1))
	if err != nil {
		t.Fatal(err)
	}
	if second.SessionKey == result.SessionKey {
		t.Fatal("session key was reused across attempts")
	}
}

func TestAgentResultSnapshotHooksBracketDurableArtifact(t *testing.T) {
	rig := newExecutorRig(t, 8192, 2*time.Second)
	var phases []string
	rig.executor.options.Hook = func(phase string) error {
		phases = append(phases, phase)
		entries, err := os.ReadDir(rig.artifactRoot)
		if err != nil {
			return err
		}
		if phase == "before_agent_result_snapshot" && len(entries) != 0 {
			t.Fatalf("artifact existed before snapshot: %v", entries)
		}
		if phase == "after_agent_result_snapshot" && len(entries) != 1 {
			t.Fatalf("durable artifact absent after snapshot: %v", entries)
		}
		return nil
	}
	if _, err := rig.executor.ExecuteTurn(context.Background(), request(1, 1)); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(phases, []string{"before_agent_result_snapshot", "after_agent_result_snapshot"}) {
		t.Fatalf("snapshot phases=%v", phases)
	}
}

func TestExecutorFailureMatrixIsBoundedAndRedacted(t *testing.T) {
	tests := []struct {
		name, scenario, wantKind string
		timeout                  time.Duration
	}{
		{"invalid-json", "invalid-json", "invalid versioned result", 2 * time.Second},
		{"oversized-stdout", "oversized-stdout", "stdout exceeded", 2 * time.Second},
		{"oversized-stderr", "oversized-stderr", "", 2 * time.Second},
		{"nonzero", "nonzero", "process failed", 2 * time.Second},
		{"delayed-timeout", "delayed-timeout", "timed out", 50 * time.Millisecond},
		{"signal-death", "signal-death", "process failed", 2 * time.Second},
		{"deadline-success", "deadline-success", "", 300 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rig := newExecutorRig(t, 1024, test.timeout)
			t.Setenv("FAKE_OPENCLAW_SCENARIO", test.scenario)
			t.Setenv("FAKE_OPENCLAW_BYTES", "65536")
			if test.scenario == "delayed-timeout" {
				t.Setenv("FAKE_OPENCLAW_DELAY", "2s")
			}
			if test.scenario == "deadline-success" {
				t.Setenv("FAKE_OPENCLAW_DELAY", "100ms")
			}
			result, err := rig.executor.ExecuteTurn(context.Background(), request(1, 1))
			if test.wantKind == "" {
				if err != nil || result.ResponseRef == "" {
					t.Fatalf("result=%+v err=%v", result, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantKind) {
				t.Fatalf("error=%v want kind %q", err, test.wantKind)
			}
			if strings.Contains(err.Error(), "bearer-value") || strings.Contains(err.Error(), "secret-value") || strings.Contains(err.Error(), "hunter2") || len(err.Error()) > 4600 {
				t.Fatalf("unsafe/unbounded error (%d bytes): %v", len(err.Error()), err)
			}
			if test.scenario == "nonzero" && (!strings.Contains(err.Error(), "content withheld") || strings.Contains(err.Error(), "Dark Factory controller turn")) {
				t.Fatalf("untrusted stderr content was surfaced: %v", err)
			}
		})
	}
}

func TestExecutorCapsProcessToRemainingLeaseWindow(t *testing.T) {
	rig := newExecutorRig(t, 4096, 2*time.Second)
	t.Setenv("FAKE_OPENCLAW_SCENARIO", "delayed-timeout")
	t.Setenv("FAKE_OPENCLAW_DELAY", "2s")
	req := request(1, 1)
	req.LeaseUntil = time.Now().Add(500 * time.Millisecond)
	started := time.Now()
	if _, err := rig.executor.ExecuteTurn(context.Background(), req); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("lease-bounded execution error=%v", err)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("process exceeded remaining lease budget: %v", elapsed)
	}

	t.Run("insufficient-window-refuses-start", func(t *testing.T) {
		rig := newExecutorRig(t, 4096, 2*time.Second)
		req := request(1, 1)
		req.LeaseUntil = time.Now().Add(rig.executor.options.ShutdownTimeout / 2)
		if _, err := rig.executor.ExecuteTurn(context.Background(), req); err == nil || !strings.Contains(err.Error(), "remaining lease") {
			t.Fatalf("insufficient lease error=%v", err)
		}
		if _, err := os.Stat(rig.transcriptPath); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("OpenClaw started without a safe lease window: %v", err)
		}
	})
}

func TestExecutorTimeoutKillsProcessGroupWithoutChildLeak(t *testing.T) {
	rig := newExecutorRig(t, 1024, 50*time.Millisecond)
	childPath := filepath.Join(rig.root, "child.pid")
	t.Setenv("FAKE_OPENCLAW_SCENARIO", "child-timeout")
	t.Setenv("FAKE_OPENCLAW_CHILD_PID", childPath)
	_, err := rig.executor.ExecuteTurn(context.Background(), request(1, 1))
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error=%v", err)
	}
	raw, err := os.ReadFile(childPath)
	if err != nil {
		t.Fatal(err)
	}
	pid, _ := strconv.Atoi(string(raw))
	deadline := time.Now().Add(time.Second)
	for {
		err = unix.Kill(pid, 0)
		if errors.Is(err, unix.ESRCH) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("child process %d leaked: %v", pid, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSuccessAlreadyWaitingAtDeadlineWins(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	done <- nil
	if err, timedOut := waitProcess(ctx, os.Getpid(), done, time.Millisecond); err != nil || timedOut {
		t.Fatalf("err=%v timedOut=%v", err, timedOut)
	}
}

func TestStrictVersionedResultRejectsUnknownAndWrongVersion(t *testing.T) {
	for _, inner := range []string{
		`{"result_version":2,"step":"done","evidence":"test artifact"}`,
		`{"result_version":1,"step":"done","evidence":"test artifact","extra":true}`,
		"```json\n{\"result_version\":1,\"step\":\"done\",\"evidence\":\"test artifact\"}\n```",
	} {
		encoded, _ := json.Marshal(map[string]any{"status": "ok", "result": map[string]any{"payloads": []map[string]string{{"text": inner}}}})
		if _, err := parseResponse(encoded); err == nil {
			t.Fatalf("accepted %q", inner)
		}
	}
}

func TestResponseArtifactCountIsBounded(t *testing.T) {
	rig := newExecutorRig(t, 4096, time.Second)
	rig.executor.options.MaxArtifacts = 1
	if _, err := rig.executor.ExecuteTurn(context.Background(), request(1, 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.executor.ExecuteTurn(context.Background(), request(2, 1)); err == nil || !strings.Contains(err.Error(), "artifact count bound") {
		t.Fatalf("second artifact error=%v", err)
	}
}

func TestResponseSnapshotRejectsSymlinkCollision(t *testing.T) {
	rig := newExecutorRig(t, 4096, time.Second)
	result, err := rig.executor.ExecuteTurn(context.Background(), request(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(result.ResponseRef); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(rig.root, "outside")
	if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, result.ResponseRef); err != nil {
		t.Fatal(err)
	}
	if _, err := rig.executor.ExecuteTurn(context.Background(), request(1, 1)); err == nil || !strings.Contains(err.Error(), "conflicting response artifact") {
		t.Fatalf("symlink collision error=%v", err)
	}
}

type executorRig struct {
	root, promptRoot, artifactRoot, transcriptPath string
	executor                                       *Executor
}

func newExecutorRig(t *testing.T, maximum int64, timeout time.Duration) *executorRig {
	t.Helper()
	root := filepath.Join(t.TempDir(), "space 'quote' $() `ticks` ;&")
	promptRoot := filepath.Join(root, "private prompts")
	artifactRoot := filepath.Join(root, "response artifacts")
	for _, path := range []string{root, promptRoot, artifactRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	executable := filepath.Join(root, "fake openclaw ' $() ` ;&")
	build := exec.Command("go", "build", "-o", executable, "./testdata/fake-openclaw")
	build.Dir = "."
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fake: %v\n%s", err, output)
	}
	transcriptPath := filepath.Join(root, "argv transcript.json")
	t.Setenv("FAKE_OPENCLAW_TRANSCRIPT", transcriptPath)
	executor, err := New(Options{
		Executable: executable, Agent: "agent 'quotes' $()", SessionPrefix: "agent:main:factory",
		Timeout: timeout, ShutdownTimeout: 100 * time.Millisecond, PromptRoot: promptRoot,
		ArtifactRoot: artifactRoot, MaxOutputBytes: maximum, MaxArtifactBytes: maximum,
		MaxArtifacts: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &executorRig{root: root, promptRoot: promptRoot, artifactRoot: artifactRoot, transcriptPath: transcriptPath, executor: executor}
}

func (r *executorRig) transcript(t *testing.T) fakeTranscript {
	t.Helper()
	raw, err := os.ReadFile(r.transcriptPath)
	if err != nil {
		t.Fatal(err)
	}
	var transcript fakeTranscript
	if err := json.Unmarshal(raw, &transcript); err != nil {
		t.Fatal(err)
	}
	return transcript
}

func request(attempt int, fence uint64) domain.TurnRequest {
	return domain.TurnRequest{RunID: "run with spaces ' $() ` ;&", ProjectID: "project", IssueID: "DF-1", Attempt: attempt, Fence: fence, LeaseUntil: time.Now().Add(time.Minute)}
}
