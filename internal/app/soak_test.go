package app

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"

	openclawadapter "github.com/ControlStackAI/dark-factory/internal/adapters/openclaw"
	durablesqlite "github.com/ControlStackAI/dark-factory/internal/adapters/sqlite"
	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
	"golang.org/x/sys/unix"
	_ "modernc.org/sqlite"
)

type soakReceipt struct {
	Seed                    int64         `json:"seed"`
	RequestedDuration       string        `json:"requested_duration"`
	FakeExecutable          string        `json:"fake_openclaw_executable"`
	LoopbackEndpoint        string        `json:"loopback_endpoint"`
	ActualDuration          time.Duration `json:"actual_duration_ns"`
	Runs                    int           `json:"runs"`
	Complete                int           `json:"complete"`
	Blocked                 int           `json:"blocked"`
	DaemonRestarts          int           `json:"daemon_restarts"`
	RestartFaultPhase       string        `json:"restart_fault_phase"`
	EndpointRequests        int           `json:"loopback_endpoint_requests"`
	MaxEndpointLatency      time.Duration `json:"max_endpoint_latency_ns"`
	DuplicateClaimMutations int           `json:"duplicate_claim_mutations"`
	DuplicateAdvances       int           `json:"duplicate_advance_mutations"`
	UniqueSessionKeys       int           `json:"unique_session_keys"`
	MaxDatabaseBytes        int64         `json:"max_database_bytes"`
	MaxJournalRows          int           `json:"max_journal_rows"`
	QuickChecks             int           `json:"sqlite_quick_checks"`
	AttemptRows             int           `json:"attempt_reservation_rows"`
	ChildLeaks              int           `json:"child_process_leaks"`
	GoroutinesBefore        int           `json:"goroutines_before"`
	GoroutinesAfter         int           `json:"goroutines_after"`
	FirstArgv               []string      `json:"first_argv"`
	LastArgv                []string      `json:"last_argv"`
	AllTerminal             bool          `json:"all_terminal"`
}

func TestM3RandomizedRestartSoak(t *testing.T) {
	durationRaw := os.Getenv("DARK_FACTORY_SOAK_DURATION")
	if durationRaw == "" {
		t.Skip("set DARK_FACTORY_SOAK_DURATION=10m for the retained M3 soak")
	}
	duration, err := time.ParseDuration(durationRaw)
	if err != nil || duration <= 0 {
		t.Fatalf("invalid soak duration %q", durationRaw)
	}
	seed, err := strconv.ParseInt(os.Getenv("DARK_FACTORY_SOAK_SEED"), 10, 64)
	if err != nil {
		t.Fatalf("DARK_FACTORY_SOAK_SEED is required: %v", err)
	}
	receiptPath := os.Getenv("DARK_FACTORY_SOAK_RECEIPT")
	if receiptPath == "" {
		t.Fatal("DARK_FACTORY_SOAK_RECEIPT is required")
	}
	root := t.TempDir()
	fakeExecutable := filepath.Join(root, "fake openclaw soak")
	build := exec.Command("go", "build", "-o", fakeExecutable, "../adapters/openclaw/testdata/fake-openclaw")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fake OpenClaw: %v\n%s", err, output)
	}

	rng := rand.New(rand.NewSource(seed))
	var rngMu sync.Mutex
	endpointRequests := 0
	maxLatency := time.Duration(0)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rngMu.Lock()
		latency := time.Duration(5+rng.Intn(21)) * time.Millisecond
		endpointRequests++
		if latency > maxLatency {
			maxLatency = latency
		}
		rngMu.Unlock()
		time.Sleep(latency)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	receipt := soakReceipt{Seed: seed, RequestedDuration: durationRaw, FakeExecutable: fakeExecutable, LoopbackEndpoint: server.URL,
		RestartFaultPhase: "checkpoint_committed", GoroutinesBefore: runtime.NumGoroutine(), AllTerminal: true}
	sessionKeys := map[string]bool{}
	started := time.Now()
	for time.Since(started) < duration {
		index := receipt.Runs + 1
		runRoot := filepath.Join(root, fmt.Sprintf("run-%08d", index))
		stateRoot := filepath.Join(runRoot, "state")
		artifactRoot := filepath.Join(runRoot, "artifacts")
		for _, path := range []string{runRoot, stateRoot, artifactRoot} {
			if err := os.MkdirAll(path, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o700); err != nil {
				t.Fatal(err)
			}
		}
		dbPath := filepath.Join(stateRoot, "factory.db")
		transcriptPath := filepath.Join(runRoot, "argv.json")
		if err := os.Setenv("FAKE_OPENCLAW_TRANSCRIPT", transcriptPath); err != nil {
			t.Fatal(err)
		}
		blockedScenario := index%7 == 0
		if blockedScenario {
			_ = os.Setenv("FAKE_OPENCLAW_SCENARIO", "nonzero")
		} else {
			_ = os.Setenv("FAKE_OPENCLAW_SCENARIO", "success")
		}
		store, err := durablesqlite.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		remote := newSoakLinear(server.URL, fmt.Sprintf("DF-%d", index))
		executor, err := openclawadapter.New(openclawadapter.Options{
			Executable: fakeExecutable, Agent: "main", SessionPrefix: "agent:main:m3-soak", Timeout: time.Second,
			ShutdownTimeout: 250 * time.Millisecond, PromptRoot: stateRoot, ArtifactRoot: artifactRoot,
			MaxOutputBytes: 8192, MaxArtifactBytes: 8192, MaxArtifacts: 4,
		})
		if err != nil {
			t.Fatal(err)
		}
		clock := &supervisorClock{now: time.Now().UTC()}
		policy := domain.Policy{LeaseDuration: 3 * time.Second, MaxRunDuration: time.Minute, MaxAttempts: 1, MaxConsecutiveFailures: 1}
		runID := fmt.Sprintf("soak-run-%d", index)
		firstCtx, firstCancel := context.WithCancel(context.Background())
		var observedReviewID string
		first, err := NewSupervisor(SupervisorOptions{
			Store: store, Linear: remote, OpenClaw: executor, Claim: remote.Claim,
			RunID: runID, ProjectID: "soak-project", IssueID: remote.issue.ID, Holder: fmt.Sprintf("daemon-%d-a", index), Policy: policy,
			PollInterval: time.Millisecond, InitialBackoff: time.Millisecond, MaxBackoff: 8 * time.Millisecond, Clock: clock, Now: clock.Now,
			OnPhase: func(name string, run domain.Run) {
				if name == "turn_finished" && run.Status == domain.RunActive && run.CheckpointSequence == 1 {
					journal, journalErr := store.Journal(context.Background(), run.ID)
					if journalErr != nil || !journalHasPhase(journal, "checkpoint_committed") {
						t.Fatalf("restart fault point lacks durable checkpoint journal: entries=%v err=%v", journal, journalErr)
					}
					observedReviewID = ReviewID(run)
					firstCancel()
				}
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		run, runErr := first.Run(firstCtx)
		if runErr != nil {
			t.Fatal(runErr)
		}
		receipt.DaemonRestarts++
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		store, err = durablesqlite.Open(dbPath)
		if err != nil {
			t.Fatal(err)
		}
		if blockedScenario {
			if run.Status != domain.RunBlocked || run.Attempts != 1 {
				t.Fatalf("blocked scenario run=%+v", run)
			}
			receipt.Blocked++
		} else {
			if run.Status != domain.RunActive || run.Attempts != 1 || observedReviewID == "" || run.LastTurnArtifact == nil {
				t.Fatalf("pre-review run=%+v review=%q", run, observedReviewID)
			}
			response, err := os.ReadFile(run.LastTurnArtifact.ResponseRef)
			if err != nil {
				t.Fatal(err)
			}
			artifactRef := "sqlite://" + observedReviewID
			digest, err := store.EnsureArtifact(context.Background(), artifactRef, response)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.EnsureReview(context.Background(), domain.ReviewEvidence{
				ID: observedReviewID, ProjectID: "soak-project", IssueID: remote.issue.ID, Status: domain.ReviewApproved,
				Immutable: true, ArtifactRef: artifactRef, ArtifactSHA256: digest,
			}); err != nil {
				t.Fatal(err)
			}
			clock.Advance(5 * time.Second)
			second, err := NewSupervisor(SupervisorOptions{
				Store: store, Linear: remote, OpenClaw: executor, Claim: remote.Claim,
				RunID: runID, ProjectID: "soak-project", IssueID: remote.issue.ID, Holder: fmt.Sprintf("daemon-%d-b", index), Policy: policy,
				PollInterval: time.Millisecond, InitialBackoff: time.Millisecond, MaxBackoff: 8 * time.Millisecond, Clock: clock, Now: clock.Now,
			})
			if err != nil {
				t.Fatal(err)
			}
			run, err = second.Run(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if run.Status != domain.RunComplete || run.Attempts != 1 {
				t.Fatalf("completed scenario run=%+v", run)
			}
			receipt.Complete++
		}
		entries, err := store.Journal(context.Background(), runID)
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) > receipt.MaxJournalRows {
			receipt.MaxJournalRows = len(entries)
		}
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		quick, attempts := inspectSoakDB(t, dbPath, runID)
		if quick != "ok" || attempts != 1 {
			t.Fatalf("quick_check=%q attempts=%d", quick, attempts)
		}
		receipt.QuickChecks++
		receipt.AttemptRows += attempts
		var databaseBytes int64
		for _, suffix := range []string{"", "-wal", "-shm"} {
			if info, err := os.Stat(dbPath + suffix); err == nil {
				databaseBytes += info.Size()
			}
		}
		if databaseBytes > receipt.MaxDatabaseBytes {
			receipt.MaxDatabaseBytes = databaseBytes
		}
		if databaseBytes > 2<<20 {
			t.Fatalf("database growth unbounded: %d bytes", databaseBytes)
		}
		transcriptRaw, err := os.ReadFile(transcriptPath)
		if err != nil {
			t.Fatal(err)
		}
		var transcript struct {
			Args       []string `json:"args"`
			PromptMode uint32   `json:"prompt_mode"`
			PID        int      `json:"pid"`
		}
		if err := json.Unmarshal(transcriptRaw, &transcript); err != nil || transcript.PromptMode != 0o600 || slicesContains(transcript.Args, "--deliver") {
			t.Fatalf("unsafe transcript=%s err=%v", transcriptRaw, err)
		}
		key := valueAfterArg(transcript.Args, "--session-key")
		if key == "" || sessionKeys[key] {
			t.Fatalf("empty or reused session key %q", key)
		}
		sessionKeys[key] = true
		if receipt.FirstArgv == nil {
			receipt.FirstArgv = transcript.Args
		}
		receipt.LastArgv = transcript.Args
		if err := unix.Kill(transcript.PID, 0); !errors.Is(err, unix.ESRCH) {
			receipt.ChildLeaks++
			t.Fatalf("OpenClaw child %d leaked: %v", transcript.PID, err)
		}
		claimDuplicates, advanceDuplicates := remote.duplicates()
		receipt.DuplicateClaimMutations += claimDuplicates
		receipt.DuplicateAdvances += advanceDuplicates
		if claimDuplicates != 0 || advanceDuplicates != 0 {
			t.Fatalf("duplicate remote operations claim=%d advance=%d", claimDuplicates, advanceDuplicates)
		}
		if run.Status != domain.RunComplete && run.Status != domain.RunBlocked {
			receipt.AllTerminal = false
			t.Fatalf("non-terminal soak run=%+v", run)
		}
		receipt.Runs++
	}
	receipt.ActualDuration = time.Since(started)
	receipt.UniqueSessionKeys = len(sessionKeys)
	rngMu.Lock()
	receipt.EndpointRequests = endpointRequests
	receipt.MaxEndpointLatency = maxLatency
	rngMu.Unlock()
	receipt.GoroutinesAfter = runtime.NumGoroutine()
	if receipt.ActualDuration < duration || receipt.Runs == 0 || receipt.Complete+receipt.Blocked != receipt.Runs || receipt.UniqueSessionKeys != receipt.Runs || receipt.ChildLeaks != 0 || receipt.DuplicateClaimMutations != 0 || receipt.DuplicateAdvances != 0 || !receipt.AllTerminal {
		t.Fatalf("invalid soak receipt: %+v", receipt)
	}
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
	t.Logf("M3_SOAK_RECEIPT %s", encoded)
}

type soakLinear struct {
	endpoint string
	issue    domain.Issue
	mu       sync.Mutex
	claims   map[string]int
	advances map[string]int
}

func newSoakLinear(endpoint, issueID string) *soakLinear {
	return &soakLinear{endpoint: endpoint, issue: domain.Issue{ID: issueID, Identifier: issueID, ProjectID: "soak-project", State: domain.IssueReady, CreatedAt: time.Now().UTC()}, claims: map[string]int{}, advances: map[string]int{}}
}

func (l *soakLinear) latency(ctx context.Context, operation string) error {
	request, _ := http.NewRequestWithContext(ctx, http.MethodGet, l.endpoint+"/?operation="+operation, nil)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	return response.Body.Close()
}

func (l *soakLinear) GetIssue(ctx context.Context, id string) (domain.Issue, error) {
	if err := l.latency(ctx, "get"); err != nil {
		return domain.Issue{}, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if id != l.issue.ID {
		return domain.Issue{}, ports.ErrNotFound
	}
	return l.issue, nil
}

func (l *soakLinear) ListProjectIssues(ctx context.Context, project string) ([]domain.Issue, error) {
	if err := l.latency(ctx, "list"); err != nil {
		return nil, err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if project != l.issue.ProjectID {
		return nil, ports.ErrNotFound
	}
	return []domain.Issue{l.issue}, nil
}

func (l *soakLinear) Claim(ctx context.Context, _, issueID, key string) error {
	if err := l.latency(ctx, "claim"); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.claims[key]++
	if issueID != l.issue.ID {
		return ports.ErrConflict
	}
	l.issue.State = domain.IssueInProgress
	return nil
}

func (l *soakLinear) Advance(ctx context.Context, request domain.AdvanceRequest) error {
	if err := l.latency(ctx, "advance"); err != nil {
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.advances[request.IdempotencyKey]++
	l.issue.State = domain.IssueCompleted
	return nil
}

func (l *soakLinear) duplicates() (int, int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	claims, advances := 0, 0
	for _, count := range l.claims {
		if count > 1 {
			claims += count - 1
		}
	}
	for _, count := range l.advances {
		if count > 1 {
			advances += count - 1
		}
	}
	return claims, advances
}

func inspectSoakDB(t *testing.T, path, runID string) (string, int) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var quick string
	if err := db.QueryRow("PRAGMA quick_check").Scan(&quick); err != nil {
		t.Fatal(err)
	}
	var attempts int
	if err := db.QueryRow(`SELECT count(*) FROM attempt_reservations WHERE run_id = ?`, runID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	return quick, attempts
}

func slicesContains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func valueAfterArg(args []string, flag string) string {
	for index := range len(args) - 1 {
		if args[index] == flag {
			return args[index+1]
		}
	}
	return ""
}

func journalHasPhase(entries []durablesqlite.JournalEntry, phase string) bool {
	for _, entry := range entries {
		if entry.Phase == phase {
			return true
		}
	}
	return false
}

var _ ports.Linear = (*soakLinear)(nil)
