package sqlite_test

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/adapters/memory"
	durablesqlite "github.com/ControlStackAI/dark-factory/internal/adapters/sqlite"
	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/factory"
	"github.com/ControlStackAI/dark-factory/internal/ports"
	_ "modernc.org/sqlite"
)

var errInjectedCrash = errors.New("injected crash after durable commit")

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(duration time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(duration)
}

func TestReopenAfterPersistedControllerPhases(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "factory.db")
	clock := newClock()
	store := openStore(t, path)
	seed(t, store, clock.Now())
	agent := memory.NewOpenClaw(memory.Turn{Result: domain.TurnResult{Step: "tested recovery", Evidence: "go test recovery phase passed"}})
	controller := factory.NewWithClock(store, store, agent, store, store, clock)

	run, err := controller.Start(ctx, "run-1", "project", "DF-1", "start", policy())
	if err != nil {
		t.Fatal(err)
	}
	if run.Version != 1 {
		t.Fatalf("start version = %d, want 1", run.Version)
	}
	store = reopen(t, store, path)
	controller = factory.NewWithClock(store, store, agent, store, store, clock)
	lease, err := controller.AcquireLease(ctx, "run-1", "worker-a")
	if err != nil {
		t.Fatal(err)
	}
	store = reopen(t, store, path)
	controller = factory.NewWithClock(store, store, agent, store, store, clock)
	if _, err := controller.ExecuteTurn(ctx, "run-1", lease.Fence); err != nil {
		t.Fatal(err)
	}
	store = reopen(t, store, path)
	controller = factory.NewWithClock(store, store, agent, store, store, clock)
	if err := controller.BindReview(ctx, "run-1", lease.Fence, "review-1"); err != nil {
		t.Fatal(err)
	}
	store = reopen(t, store, path)
	controller = factory.NewWithClock(store, store, agent, store, store, clock)
	completed, err := controller.CompleteAndAdvance(ctx, "run-1", lease.Fence, "review-1 approved durable artifact")
	if err != nil {
		t.Fatal(err)
	}
	if completed.IssueID != "DF-2" || completed.PendingAdvance != nil {
		t.Fatalf("reconciled run = %#v", completed)
	}
	store = reopen(t, store, path)
	defer store.Close()
	got, err := store.Get(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.IssueID != "DF-2" || got.Attempts != 1 || got.CheckpointSequence != 0 {
		t.Fatalf("reopened run = %#v", got)
	}
	entries, err := store.Journal(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{
		"run_created": true, "lease_acquired": true, "attempt_reserved": true,
		"checkpoint_committed": true, "review_bound": true, "review_consumed": true,
		"advance_frozen": true, "remote_advance_committed": true, "advance_reconciled": true,
	}
	for _, entry := range entries {
		delete(want, entry.Phase)
	}
	if len(want) != 0 {
		t.Fatalf("missing journal phases: %v; entries=%v", want, entries)
	}
}

func TestAfterCommitCrashRecoveryAtEveryAdvancementBoundary(t *testing.T) {
	for _, phase := range []string{"advance_frozen", "review_consumed", "remote_advance_committed", "advance_reconciled"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "factory.db")
			clock := newClock()
			fired := false
			hook := func(committedPhase string) error {
				if !fired && committedPhase == phase {
					fired = true
					return errInjectedCrash
				}
				return nil
			}
			store := openStore(t, path, durablesqlite.WithAfterCommitHook(hook))
			seed(t, store, clock.Now())
			controller, fence := prepareBoundRun(t, store, clock)
			_, err := controller.CompleteAndAdvance(ctx, "run-1", fence, "review-1 approved durable artifact")
			if !errors.Is(err, errInjectedCrash) {
				t.Fatalf("CompleteAndAdvance error = %v, want injected crash", err)
			}
			if !fired {
				t.Fatal("fault hook did not fire")
			}
			store = reopen(t, store, path)
			defer store.Close()
			run, err := store.Get(ctx, "run-1")
			if err != nil {
				t.Fatal(err)
			}
			if phase == "advance_reconciled" {
				if run.PendingAdvance != nil || run.IssueID != "DF-2" {
					t.Fatalf("reconciled durable run = %#v", run)
				}
			} else {
				if run.PendingAdvance == nil {
					t.Fatalf("phase %s did not leave a frozen advancement", phase)
				}
				controller = factory.NewWithClock(store, store, memory.NewOpenClaw(), store, store, clock)
				run, err = controller.CompleteAndAdvance(ctx, "run-1", fence, "ignored because frozen evidence is reused")
				if err != nil {
					t.Fatalf("reconcile after %s: %v", phase, err)
				}
				if run.PendingAdvance != nil || run.IssueID != "DF-2" {
					t.Fatalf("reconciled run = %#v", run)
				}
			}
			count, err := store.AdvanceReceiptCount(ctx, runReceiptKey(t, store))
			if err != nil {
				t.Fatal(err)
			}
			if count != 1 {
				t.Fatalf("remote mutation receipts = %d, want exactly 1", count)
			}
		})
	}
}

func TestAfterCommitCrashRecoveryAtControllerBoundaries(t *testing.T) {
	tests := []struct {
		phase  string
		assert func(*testing.T, domain.Run)
		run    func(*testing.T, *factory.Controller) error
	}{
		{
			phase: "run_created",
			run: func(t *testing.T, controller *factory.Controller) error {
				_, err := controller.Start(context.Background(), "run-1", "project", "DF-1", "start", policy())
				return err
			},
			assert: func(t *testing.T, run domain.Run) {
				if run.Version != 1 || run.Status != domain.RunActive {
					t.Fatalf("reopened created run = %#v", run)
				}
			},
		},
		{
			phase: "lease_acquired",
			run: func(t *testing.T, controller *factory.Controller) error {
				if _, err := controller.Start(context.Background(), "run-1", "project", "DF-1", "start", policy()); err != nil {
					t.Fatal(err)
				}
				_, err := controller.AcquireLease(context.Background(), "run-1", "worker")
				return err
			},
			assert: func(t *testing.T, run domain.Run) {
				if run.Lease.Fence != 1 || run.NextFence != 1 {
					t.Fatalf("reopened leased run = %#v", run)
				}
			},
		},
		{
			phase: "checkpoint_committed",
			run: func(t *testing.T, controller *factory.Controller) error {
				if _, err := controller.Start(context.Background(), "run-1", "project", "DF-1", "start", policy()); err != nil {
					t.Fatal(err)
				}
				lease, err := controller.AcquireLease(context.Background(), "run-1", "worker")
				if err != nil {
					t.Fatal(err)
				}
				_, err = controller.ExecuteTurn(context.Background(), "run-1", lease.Fence)
				return err
			},
			assert: func(t *testing.T, run domain.Run) {
				if run.Attempts != 1 || run.CheckpointSequence != 1 || len(run.Evidence) != 1 {
					t.Fatalf("reopened checkpointed run = %#v", run)
				}
			},
		},
		{
			phase: "review_bound",
			run: func(t *testing.T, controller *factory.Controller) error {
				if _, err := controller.Start(context.Background(), "run-1", "project", "DF-1", "start", policy()); err != nil {
					t.Fatal(err)
				}
				lease, err := controller.AcquireLease(context.Background(), "run-1", "worker")
				if err != nil {
					t.Fatal(err)
				}
				if _, err := controller.ExecuteTurn(context.Background(), "run-1", lease.Fence); err != nil {
					t.Fatal(err)
				}
				return controller.BindReview(context.Background(), "run-1", lease.Fence, "review-1")
			},
			assert: func(t *testing.T, run domain.Run) {
				if run.Review == nil || run.Review.ReviewID != "review-1" {
					t.Fatalf("reopened reviewed run = %#v", run)
				}
			},
		},
	}
	for _, test := range tests {
		t.Run(test.phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "factory.db")
			clock := newClock()
			fired := false
			store := openStore(t, path, durablesqlite.WithAfterCommitHook(func(phase string) error {
				if !fired && phase == test.phase {
					fired = true
					return errInjectedCrash
				}
				return nil
			}))
			seed(t, store, clock.Now())
			agent := memory.NewOpenClaw(memory.Turn{Result: domain.TurnResult{Step: "persisted", Evidence: "checkpoint survived injected crash"}})
			controller := factory.NewWithClock(store, store, agent, store, store, clock)
			if err := test.run(t, controller); !errors.Is(err, errInjectedCrash) {
				t.Fatalf("operation error = %v, want injected crash", err)
			}
			if !fired {
				t.Fatal("fault hook did not fire")
			}
			store = reopen(t, store, path)
			defer store.Close()
			run, err := store.Get(context.Background(), "run-1")
			if err != nil {
				t.Fatal(err)
			}
			test.assert(t, run)
		})
	}
}

func TestConcurrentLeaseAcquisitionHasOneWinner(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "factory.db")
	clock := newClock()
	store := openStore(t, path)
	defer store.Close()
	seed(t, store, clock.Now())
	controller := factory.NewWithClock(store, store, memory.NewOpenClaw(), store, store, clock)
	if _, err := controller.Start(ctx, "run-1", "project", "DF-1", "start", policy()); err != nil {
		t.Fatal(err)
	}
	const workers = 24
	start := make(chan struct{})
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			_, err := controller.AcquireLease(ctx, "run-1", string(rune('a'+worker)))
			results <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		switch {
		case err == nil:
			winners++
		case errors.Is(err, factory.ErrLeaseHeld):
		default:
			t.Fatalf("unexpected acquisition error: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("lease winners = %d, want 1", winners)
	}
}

func TestStaleFenceRejectedAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "factory.db")
	clock := newClock()
	store := openStore(t, path)
	seed(t, store, clock.Now())
	controller := factory.NewWithClock(store, store, memory.NewOpenClaw(), store, store, clock)
	if _, err := controller.Start(ctx, "run-1", "project", "DF-1", "start", policy()); err != nil {
		t.Fatal(err)
	}
	oldLease, err := controller.AcquireLease(ctx, "run-1", "old")
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(2 * time.Minute)
	store = reopen(t, store, path)
	controller = factory.NewWithClock(store, store, memory.NewOpenClaw(), store, store, clock)
	newLease, err := controller.AcquireLease(ctx, "run-1", "new")
	if err != nil {
		t.Fatal(err)
	}
	if newLease.Fence <= oldLease.Fence {
		t.Fatalf("new fence %d <= old fence %d", newLease.Fence, oldLease.Fence)
	}
	store = reopen(t, store, path)
	defer store.Close()
	controller = factory.NewWithClock(store, store, memory.NewOpenClaw(), store, store, clock)
	if err := controller.Checkpoint(ctx, "run-1", oldLease.Fence, 1, "stale", "stale worker artifact result"); !errors.Is(err, factory.ErrStaleFence) {
		t.Fatalf("stale checkpoint error = %v", err)
	}
}

func TestAttemptReservationSurvivesCrashWithoutRedispatch(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "factory.db")
	clock := newClock()
	fired := false
	store := openStore(t, path, durablesqlite.WithAfterCommitHook(func(phase string) error {
		if phase == "attempt_reserved" && !fired {
			fired = true
			return errInjectedCrash
		}
		return nil
	}))
	seed(t, store, clock.Now())
	agent := memory.NewOpenClaw(memory.Turn{Result: domain.TurnResult{Step: "should not run", Evidence: "first dispatch should not happen"}})
	controller := factory.NewWithClock(store, store, agent, store, store, clock)
	if _, err := controller.Start(ctx, "run-1", "project", "DF-1", "start", policy()); err != nil {
		t.Fatal(err)
	}
	lease, err := controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ExecuteTurn(ctx, "run-1", lease.Fence); !errors.Is(err, errInjectedCrash) {
		t.Fatalf("ExecuteTurn error = %v", err)
	}
	if len(agent.Requests()) != 0 {
		t.Fatalf("ambiguous reservation dispatched %d turns", len(agent.Requests()))
	}
	store = reopen(t, store, path)
	defer store.Close()
	run, err := store.Get(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	count, err := store.AttemptReservationCount(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if run.Attempts != 1 || count != 1 {
		t.Fatalf("after restart attempts=%d reservations=%d", run.Attempts, count)
	}
	secondAgent := memory.NewOpenClaw(memory.Turn{Result: domain.TurnResult{Step: "recovered", Evidence: "second reserved attempt completed"}})
	controller = factory.NewWithClock(store, store, secondAgent, store, store, clock)
	if _, err := controller.ExecuteTurn(ctx, "run-1", lease.Fence); err != nil {
		t.Fatal(err)
	}
	requests := secondAgent.Requests()
	if len(requests) != 1 || requests[0].Attempt != 2 {
		t.Fatalf("recovery requests = %#v", requests)
	}
}

func TestBeforeCommitFaultRollsBackAttemptReservation(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "factory.db")
	clock := newClock()
	fired := false
	store := openStore(t, path, durablesqlite.WithBeforeCommitHook(func(phase string) error {
		if phase == "attempt_reserved" && !fired {
			fired = true
			return errInjectedCrash
		}
		return nil
	}))
	seed(t, store, clock.Now())
	agent := memory.NewOpenClaw(memory.Turn{Result: domain.TurnResult{Step: "not dispatched", Evidence: "rolled back reservation was not dispatched"}})
	controller := factory.NewWithClock(store, store, agent, store, store, clock)
	if _, err := controller.Start(ctx, "run-1", "project", "DF-1", "start", policy()); err != nil {
		t.Fatal(err)
	}
	lease, err := controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ExecuteTurn(ctx, "run-1", lease.Fence); !errors.Is(err, errInjectedCrash) {
		t.Fatalf("ExecuteTurn error = %v", err)
	}
	if len(agent.Requests()) != 0 {
		t.Fatal("turn dispatched after rolled-back reservation")
	}
	store = reopen(t, store, path)
	defer store.Close()
	run, err := store.Get(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	count, err := store.AttemptReservationCount(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if run.Attempts != 0 || count != 0 {
		t.Fatalf("rolled-back attempt persisted: run=%d reservations=%d", run.Attempts, count)
	}
}

func TestBeforeCommitFaultRollsBackRemoteMutationAtomically(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "factory.db")
	clock := newClock()
	fired := false
	store := openStore(t, path, durablesqlite.WithBeforeCommitHook(func(phase string) error {
		if phase == "remote_advance_committed" && !fired {
			fired = true
			return errInjectedCrash
		}
		return nil
	}))
	seed(t, store, clock.Now())
	controller, fence := prepareBoundRun(t, store, clock)
	if _, err := controller.CompleteAndAdvance(ctx, "run-1", fence, "review-1 approved durable artifact"); !errors.Is(err, errInjectedCrash) {
		t.Fatalf("CompleteAndAdvance error = %v", err)
	}
	store = reopen(t, store, path)
	run, err := store.Get(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if run.PendingAdvance == nil {
		t.Fatal("frozen operation was not retained")
	}
	current, err := store.GetIssue(ctx, "DF-1")
	if err != nil {
		t.Fatal(err)
	}
	next, err := store.GetIssue(ctx, "DF-2")
	if err != nil {
		t.Fatal(err)
	}
	count, err := store.AdvanceReceiptCount(ctx, run.PendingAdvance.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != domain.IssueInProgress || next.State != domain.IssueReady || count != 0 {
		t.Fatalf("partial remote transaction: current=%s next=%s receipts=%d", current.State, next.State, count)
	}
	controller = factory.NewWithClock(store, store, memory.NewOpenClaw(), store, store, clock)
	completed, err := controller.CompleteAndAdvance(ctx, "run-1", fence, "frozen evidence")
	if err != nil {
		t.Fatal(err)
	}
	if completed.IssueID != "DF-2" {
		t.Fatalf("recovered issue = %s", completed.IssueID)
	}
	store.Close()
}

func TestReviewConsumptionIsDurableAndSingleConsumer(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "factory.db")
	clock := newClock()
	store := openStore(t, path)
	seed(t, store, clock.Now())
	controller := factory.NewWithClock(store, store, memory.NewOpenClaw(), store, store, clock)
	for _, id := range []string{"run-1", "run-2"} {
		if _, err := controller.Start(ctx, id, "project", "DF-1", "start", policy()); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ConsumeReview(ctx, "review-1", "run-1", reviewDigest(t, store)); err != nil {
		t.Fatal(err)
	}
	store = reopen(t, store, path)
	defer store.Close()
	digest := reviewDigest(t, store)
	if err := store.ConsumeReview(ctx, "review-1", "run-1", digest); err != nil {
		t.Fatalf("same consumer retry: %v", err)
	}
	if err := store.ConsumeReview(ctx, "review-1", "run-2", digest); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("second consumer error = %v", err)
	}
}

func TestOpenFailsClosedForCorruptNewerAndInvalidDatabases(t *testing.T) {
	t.Run("corrupt", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "factory.db")
		if err := os.WriteFile(path, []byte("this is not sqlite"), 0o600); err != nil {
			t.Fatal(err)
		}
		if store, err := durablesqlite.Open(path); err == nil {
			store.Close()
			t.Fatal("corrupt database opened")
		} else if !errors.Is(err, durablesqlite.ErrCorruptDatabase) {
			t.Fatalf("corrupt database error = %v", err)
		}
	})
	t.Run("newer schema", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "factory.db")
		store := openStore(t, path)
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		raw, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Exec(`PRAGMA user_version = 2`); err != nil {
			t.Fatal(err)
		}
		raw.Close()
		if store, err := durablesqlite.Open(path); err == nil {
			store.Close()
			t.Fatal("newer database opened")
		} else if !errors.Is(err, durablesqlite.ErrUnsupportedSchema) {
			t.Fatalf("newer schema error = %v", err)
		}
	})
	t.Run("invalid record", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "factory.db")
		store := openStore(t, path)
		seed(t, store, newClock().Now())
		controller := factory.NewWithClock(store, store, memory.NewOpenClaw(), store, store, newClock())
		if _, err := controller.Start(context.Background(), "run-1", "project", "DF-1", "start", policy()); err != nil {
			t.Fatal(err)
		}
		store.Close()
		raw, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Exec(`UPDATE runs SET payload = '{"id":"run-1"}' WHERE id = 'run-1'`); err != nil {
			t.Fatal(err)
		}
		raw.Close()
		if store, err := durablesqlite.Open(path); err == nil {
			store.Close()
			t.Fatal("invalid record database opened")
		} else if !errors.Is(err, durablesqlite.ErrInvalidRecord) {
			t.Fatalf("invalid record error = %v", err)
		}
	})
	t.Run("unversioned nonempty", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "factory.db")
		raw, err := sql.Open("sqlite", path)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := raw.Exec(`CREATE TABLE unexpected(value TEXT)`); err != nil {
			t.Fatal(err)
		}
		raw.Close()
		if store, err := durablesqlite.Open(path); err == nil {
			store.Close()
			t.Fatal("non-empty unversioned database opened")
		} else if !errors.Is(err, durablesqlite.ErrInvalidRecord) {
			t.Fatalf("unversioned database error = %v", err)
		}
	})
}

func prepareBoundRun(t *testing.T, store *durablesqlite.Store, clock *testClock) (*factory.Controller, uint64) {
	t.Helper()
	ctx := context.Background()
	agent := memory.NewOpenClaw(memory.Turn{Result: domain.TurnResult{Step: "tested recovery", Evidence: "go test recovery phase passed"}})
	controller := factory.NewWithClock(store, store, agent, store, store, clock)
	if _, err := controller.Start(ctx, "run-1", "project", "DF-1", "start", policy()); err != nil {
		t.Fatal(err)
	}
	lease, err := controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ExecuteTurn(ctx, "run-1", lease.Fence); err != nil {
		t.Fatal(err)
	}
	if err := controller.BindReview(ctx, "run-1", lease.Fence, "review-1"); err != nil {
		t.Fatal(err)
	}
	return controller, lease.Fence
}

func seed(t *testing.T, store *durablesqlite.Store, now time.Time) {
	t.Helper()
	ctx := context.Background()
	for _, issue := range []domain.Issue{
		{ID: "DF-1", ProjectID: "project", Title: "current", Priority: 1, CreatedAt: now.Add(-2 * time.Hour), State: domain.IssueInProgress},
		{ID: "DF-2", ProjectID: "project", Title: "next", Priority: 2, CreatedAt: now.Add(-time.Hour), State: domain.IssueReady},
	} {
		if err := store.EnsureIssue(ctx, issue); err != nil {
			t.Fatal(err)
		}
	}
	digest, err := store.EnsureArtifact(ctx, "sqlite://review/DF-1", []byte("immutable reviewed artifact\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureReview(ctx, domain.ReviewEvidence{
		ID: "review-1", ProjectID: "project", IssueID: "DF-1", Status: domain.ReviewApproved,
		Immutable: true, ArtifactRef: "sqlite://review/DF-1", ArtifactSHA256: digest,
	}); err != nil {
		t.Fatal(err)
	}
}

func reviewDigest(t *testing.T, store *durablesqlite.Store) string {
	t.Helper()
	review, err := store.GetReview(context.Background(), "review-1")
	if err != nil {
		t.Fatal(err)
	}
	return review.ArtifactSHA256
}

func runReceiptKey(t *testing.T, store *durablesqlite.Store) string {
	t.Helper()
	entries, err := store.Journal(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Phase == "remote_advance_committed" {
			run, err := store.Get(context.Background(), "run-1")
			if err != nil {
				t.Fatal(err)
			}
			// The receipt key is deterministic from the run/issue/review tuple. Retrieve it
			// from the frozen record when present, or recompute the controller's SHA-256 key.
			if run.PendingAdvance != nil {
				return run.PendingAdvance.IdempotencyKey
			}
			return advanceKey("run-1", "DF-1", "review-1")
		}
	}
	t.Fatal("remote advancement journal entry missing")
	return ""
}

func advanceKey(runID, issueID, reviewID string) string {
	// Kept local to avoid exporting controller internals; this mirrors the documented
	// deterministic key contract for inspection only.
	return fmtSHA256(runID + "\x00" + issueID + "\x00" + reviewID)
}

func fmtSHA256(value string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(value)))
}

func newClock() *testClock {
	return &testClock{now: time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC)}
}

func policy() domain.Policy {
	return domain.Policy{LeaseDuration: time.Minute, MaxRunDuration: time.Hour, MaxAttempts: 3, MaxConsecutiveFailures: 2}
}

func openStore(t *testing.T, path string, opts ...durablesqlite.Option) *durablesqlite.Store {
	t.Helper()
	store, err := durablesqlite.Open(path, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func reopen(t *testing.T, store *durablesqlite.Store, path string) *durablesqlite.Store {
	t.Helper()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	return openStore(t, path)
}
