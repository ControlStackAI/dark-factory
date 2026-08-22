package app

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/adapters/memory"
	durablesqlite "github.com/ControlStackAI/dark-factory/internal/adapters/sqlite"
	"github.com/ControlStackAI/dark-factory/internal/config"
	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/factory"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

func TestProductionSupervisorRejectsModelOverrideOutsideExactM3Argv(t *testing.T) {
	_, err := NewProductionSupervisor(config.Config{Mode: "live", OpenClaw: config.OpenClaw{Model: "override"}})
	if err == nil || !strings.Contains(err.Error(), "exact OpenClaw argv") {
		t.Fatalf("model override error=%v", err)
	}
}

func TestSingleInstanceDatabaseLockExcludesSecondSupervisor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "factoryd.lock")
	first, err := AcquireInstanceLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if _, err := AcquireInstanceLock(path); !errors.Is(err, ports.ErrBusy) {
		t.Fatalf("second lock error=%v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	third, err := AcquireInstanceLock(path)
	if err != nil {
		t.Fatalf("reacquire after close: %v", err)
	}
	_ = third.Close()
}

func TestSupervisorReacquiresExpiredLeaseWithHigherFence(t *testing.T) {
	rig := newSupervisorRig(t, memory.Turn{Result: goodTurn()})
	clock := &supervisorClock{now: time.Date(2026, 8, 21, 1, 2, 3, 0, time.UTC)}
	controller := factory.NewWithClock(rig.store, rig.linear, rig.agent, rig.store, rig.store, clock)
	if _, err := controller.Start(context.Background(), rig.runID, "project", "DF-1", "start", policy(20*time.Millisecond, 3, 2)); err != nil {
		t.Fatal(err)
	}
	old, err := controller.AcquireLease(context.Background(), rig.runID, "dead-worker")
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acquired := false
	supervisor := rig.supervisor(t, SupervisorOptions{
		Policy: policy(20*time.Millisecond, 3, 2), Clock: clock, Now: clock.Now,
		OnPhase: func(name string, run domain.Run) {
			if name == "lease_acquired" {
				acquired = true
			}
		},
		Sleep: func(context.Context, time.Duration) error { cancel(); return context.Canceled },
	})
	if _, err := supervisor.Run(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := rig.store.Get(context.Background(), rig.runID)
	if err != nil {
		t.Fatal(err)
	}
	if !acquired || got.Lease.Fence <= old.Fence || got.Lease.Holder != "new-worker" {
		t.Fatalf("old=%+v new=%+v", old, got.Lease)
	}
}

func TestSupervisorReconcilesFrozenAdvanceBeforeAnyDispatch(t *testing.T) {
	rig := newSupervisorRig(t, memory.Turn{Result: goodTurn()})
	ctx := context.Background()
	digest, err := rig.store.EnsureArtifact(ctx, "sqlite://review", []byte("reviewed artifact"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.store.EnsureReview(ctx, domain.ReviewEvidence{ID: "review-1", ProjectID: "project", IssueID: "DF-1", Status: domain.ReviewApproved, Immutable: true, ArtifactRef: "sqlite://review", ArtifactSHA256: digest}); err != nil {
		t.Fatal(err)
	}
	fault := &failAdvanceOnce{Linear: rig.linear}
	controller := factory.New(rig.store, fault, rig.agent, rig.store, rig.store)
	if _, err := controller.Start(ctx, rig.runID, "project", "DF-1", "start", policy(time.Minute, 3, 2)); err != nil {
		t.Fatal(err)
	}
	lease, err := controller.AcquireLease(ctx, rig.runID, "old-worker")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ExecuteTurn(ctx, rig.runID, lease.Fence); err != nil {
		t.Fatal(err)
	}
	if err := controller.BindReview(ctx, rig.runID, lease.Fence, "review-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.CompleteAndAdvance(ctx, rig.runID, lease.Fence, "reviewed result artifact passed"); err == nil {
		t.Fatal("fault did not leave frozen advancement")
	}
	before := len(rig.agent.Requests())
	secondAgent := memory.NewOpenClaw(memory.Turn{Result: goodTurn()})
	supervisor, err := NewSupervisor(SupervisorOptions{
		Store: rig.store, Linear: rig.linear, OpenClaw: secondAgent,
		Claim: func(context.Context, string, string, string) error {
			t.Fatal("claim ran before reconciliation")
			return nil
		},
		RunID: rig.runID, ProjectID: "project", IssueID: "DF-1", Holder: "old-worker",
		Policy: policy(time.Minute, 3, 2), PollInterval: time.Millisecond, InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := supervisor.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != domain.RunComplete || run.PendingAdvance != nil || len(secondAgent.Requests()) != 0 || len(rig.agent.Requests()) != before {
		t.Fatalf("reconciled run=%+v second requests=%v", run, secondAgent.Requests())
	}
}

func TestSupervisorBackoffPreventsBusyLoop(t *testing.T) {
	rig := newSupervisorRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	claims, sleeps := 0, 0
	var delays []time.Duration
	supervisor := rig.supervisor(t, SupervisorOptions{
		Claim: func(context.Context, string, string, string) error {
			claims++
			return errors.New("loopback unavailable")
		},
		Sleep: func(_ context.Context, duration time.Duration) error {
			sleeps++
			delays = append(delays, duration)
			if sleeps == 4 {
				cancel()
				return context.Canceled
			}
			return nil
		},
	})
	if _, err := supervisor.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if claims != 4 || sleeps != 4 || len(delays) != 4 || delays[0] != time.Millisecond || delays[1] != 2*time.Millisecond || delays[2] != 4*time.Millisecond || delays[3] != 4*time.Millisecond {
		t.Fatalf("claims=%d sleeps=%d delays=%v", claims, sleeps, delays)
	}
	run, _ := rig.store.Get(context.Background(), rig.runID)
	if run.Attempts != 0 {
		t.Fatalf("claim retry spent %d attempts", run.Attempts)
	}
}

func TestSupervisorTurnFailuresUseBoundedExponentialBackoff(t *testing.T) {
	rig := newSupervisorRig(t,
		memory.Turn{Err: errors.New("agent failed once")},
		memory.Turn{Err: errors.New("agent failed twice")},
		memory.Turn{Err: errors.New("agent failed three times")},
	)
	var delays []time.Duration
	supervisor := rig.supervisor(t, SupervisorOptions{
		Policy: policy(time.Minute, 3, 3),
		Sleep: func(_ context.Context, duration time.Duration) error {
			delays = append(delays, duration)
			return nil
		},
	})
	run, err := supervisor.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{time.Millisecond, 2 * time.Millisecond}
	if run.Status != domain.RunBlocked || run.Attempts != 3 || !slices.Equal(delays, want) {
		t.Fatalf("run=%+v delays=%v want=%v", run, delays, want)
	}
}

func TestSupervisorRejectsReviewForDifferentResponseArtifact(t *testing.T) {
	rig := newSupervisorRig(t, memory.Turn{Result: goodTurn()})
	ctx := context.Background()
	reviewID := "review:" + rig.runID + ":DF-1:1"
	digest, err := rig.store.EnsureArtifact(ctx, "sqlite://wrong-response", []byte("different OpenClaw response"))
	if err != nil {
		t.Fatal(err)
	}
	if err := rig.store.EnsureReview(ctx, domain.ReviewEvidence{
		ID: reviewID, ProjectID: "project", IssueID: "DF-1", Status: domain.ReviewApproved,
		Immutable: true, ArtifactRef: "sqlite://wrong-response", ArtifactSHA256: digest,
	}); err != nil {
		t.Fatal(err)
	}
	supervisor := rig.supervisor(t, SupervisorOptions{})
	run, err := supervisor.Run(ctx)
	if err == nil || !strings.Contains(err.Error(), "not bound to the last OpenClaw response") {
		t.Fatalf("mismatched review error=%v run=%+v", err, run)
	}
	if run.Status != domain.RunActive || run.Attempts != 1 || run.PendingAdvance != nil {
		t.Fatalf("mismatched review advanced run=%+v", run)
	}
}

func TestSupervisorCleanCancellationRecordsAttemptAndLeavesValidDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "factory.db")
	store, err := durablesqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	linear := memory.NewLinear(domain.Issue{ID: "DF-1", Identifier: "DF-1", ProjectID: "project", State: domain.IssueReady, CreatedAt: time.Now().UTC()})
	agent := blockingAgent{started: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	supervisor, err := NewSupervisor(SupervisorOptions{
		Store: store, Linear: linear, OpenClaw: agent, Claim: func(context.Context, string, string, string) error { return nil },
		RunID: "clean-stop", ProjectID: "project", IssueID: "DF-1", Holder: "worker", Policy: policy(time.Minute, 3, 3),
		PollInterval: time.Millisecond, InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		run domain.Run
		err error
	}
	done := make(chan outcome, 1)
	go func() { run, runErr := supervisor.Run(ctx); done <- outcome{run: run, err: runErr} }()
	<-agent.started
	cancel()
	result := <-done
	if result.err != nil {
		t.Fatal(result.err)
	}
	run := result.run
	if run.Attempts != 1 || run.PendingDispatch != nil || run.Status != domain.RunActive {
		t.Fatalf("cleanly stopped run=%+v", run)
	}
	if err := supervisor.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := durablesqlite.Open(path)
	if err != nil {
		t.Fatalf("reopen after clean shutdown: %v", err)
	}
	defer reopened.Close()
	if _, err := reopened.Get(context.Background(), "clean-stop"); err != nil {
		t.Fatal(err)
	}
}

func TestSupervisorRestartAfterProcessLossBlocksAmbiguousAttemptWithoutRedispatch(t *testing.T) {
	rig := newSupervisorRig(t)
	controller := factory.New(rig.store, rig.linear, rig.agent, rig.store, rig.store)
	if _, err := controller.Start(context.Background(), rig.runID, "project", "DF-1", "start", policy(time.Minute, 3, 3)); err != nil {
		t.Fatal(err)
	}
	lease, err := controller.AcquireLease(context.Background(), rig.runID, "killed-worker")
	if err != nil {
		t.Fatal(err)
	}
	request, err := controller.ReserveTurn(context.Background(), rig.runID, lease.Fence)
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.MarkDispatchStarted(context.Background(), rig.runID, lease.Fence, request.Attempt); err != nil {
		t.Fatal(err)
	}
	restartedAgent := memory.NewOpenClaw(memory.Turn{Result: goodTurn()})
	supervisor, err := NewSupervisor(SupervisorOptions{
		Store: rig.store, Linear: rig.linear, OpenClaw: restartedAgent, Claim: func(context.Context, string, string, string) error { return nil },
		RunID: rig.runID, ProjectID: "project", IssueID: "DF-1", Holder: "replacement", Policy: policy(time.Minute, 3, 3),
		PollInterval: time.Millisecond, InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := supervisor.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != domain.RunBlocked || run.Attempts != 1 || len(restartedAgent.Requests()) != 0 || !strings.Contains(run.BlockedReason, "manual resolution") {
		t.Fatalf("restart run=%+v requests=%v", run, restartedAgent.Requests())
	}
}

func TestSupervisorTerminalAttemptBudgetAndWaitForReviewWithoutGrowth(t *testing.T) {
	t.Run("budget", func(t *testing.T) {
		rig := newSupervisorRig(t, memory.Turn{Err: errors.New("agent failed")})
		sleeps := 0
		supervisor := rig.supervisor(t, SupervisorOptions{
			Policy: policy(time.Minute, 1, 1),
			Sleep:  func(context.Context, time.Duration) error { sleeps++; return nil },
		})
		run, err := supervisor.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if run.Status != domain.RunBlocked || run.Attempts != 1 || len(rig.agent.Requests()) != 1 || sleeps != 0 {
			t.Fatalf("budget run=%+v requests=%d sleeps=%d", run, len(rig.agent.Requests()), sleeps)
		}
	})
	t.Run("wait-review", func(t *testing.T) {
		rig := newSupervisorRig(t, memory.Turn{Result: goodTurn()})
		ctx, cancel := context.WithCancel(context.Background())
		polls := 0
		supervisor := rig.supervisor(t, SupervisorOptions{
			Sleep: func(_ context.Context, duration time.Duration) error {
				if duration == time.Millisecond {
					polls++
				}
				if polls == 5 {
					cancel()
					return context.Canceled
				}
				return nil
			},
		})
		run, err := supervisor.Run(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if polls != 5 || run.Attempts != 1 || len(rig.agent.Requests()) != 1 || run.CheckpointSequence != 1 {
			t.Fatalf("polls=%d run=%+v requests=%d", polls, run, len(rig.agent.Requests()))
		}
	})
}

type supervisorRig struct {
	store  *durablesqlite.Store
	linear *memory.Linear
	agent  *memory.OpenClaw
	runID  string
}

func newSupervisorRig(t *testing.T, turns ...memory.Turn) *supervisorRig {
	t.Helper()
	store, err := durablesqlite.Open(filepath.Join(t.TempDir(), "factory.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	linear := memory.NewLinear(domain.Issue{ID: "DF-1", Identifier: "DF-1", ProjectID: "project", State: domain.IssueReady, CreatedAt: time.Now().UTC()})
	return &supervisorRig{store: store, linear: linear, agent: memory.NewOpenClaw(turns...), runID: "supervisor-run"}
}

func (r *supervisorRig) supervisor(t *testing.T, overrides SupervisorOptions) *Supervisor {
	t.Helper()
	options := SupervisorOptions{
		Store: r.store, Linear: r.linear, OpenClaw: r.agent, Claim: func(context.Context, string, string, string) error { return nil },
		RunID: r.runID, ProjectID: "project", IssueID: "DF-1", Holder: "new-worker", Policy: policy(time.Minute, 3, 2),
		PollInterval: time.Millisecond, InitialBackoff: time.Millisecond, MaxBackoff: 4 * time.Millisecond,
	}
	if overrides.Policy.Valid() {
		options.Policy = overrides.Policy
	}
	if overrides.Claim != nil {
		options.Claim = overrides.Claim
	}
	if overrides.Sleep != nil {
		options.Sleep = overrides.Sleep
	}
	if overrides.Clock != nil {
		options.Clock = overrides.Clock
	}
	if overrides.Now != nil {
		options.Now = overrides.Now
	}
	options.OnPhase = overrides.OnPhase
	supervisor, err := NewSupervisor(options)
	if err != nil {
		t.Fatal(err)
	}
	return supervisor
}

func policy(lease time.Duration, attempts, failures int) domain.Policy {
	return domain.Policy{LeaseDuration: lease, MaxRunDuration: time.Hour, MaxAttempts: attempts, MaxConsecutiveFailures: failures}
}

func goodTurn() domain.TurnResult {
	return domain.TurnResult{Step: "completed test turn", Evidence: "go test supervisor artifact passed"}
}

type supervisorClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *supervisorClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}
func (c *supervisorClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type failAdvanceOnce struct {
	ports.Linear
	fired bool
}

func (f *failAdvanceOnce) Advance(ctx context.Context, request domain.AdvanceRequest) error {
	if !f.fired {
		f.fired = true
		return errors.New("injected before remote commit")
	}
	return f.Linear.Advance(ctx, request)
}

type blockingAgent struct{ started chan struct{} }

func (a blockingAgent) ExecuteTurn(ctx context.Context, _ domain.TurnRequest) (domain.TurnResult, error) {
	close(a.started)
	<-ctx.Done()
	return domain.TurnResult{}, ctx.Err()
}
