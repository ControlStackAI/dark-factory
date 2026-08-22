package factory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/adapters/memory"
	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/factory"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time          { return c.now }
func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

type clockAdvancingAgent struct {
	clock   *fakeClock
	advance time.Duration
	result  domain.TurnResult
}

func (a *clockAdvancingAgent) ExecuteTurn(context.Context, domain.TurnRequest) (domain.TurnResult, error) {
	a.clock.Advance(a.advance)
	return a.result, nil
}

type clockAdvancingLinear struct {
	*memory.Linear
	clock    *fakeClock
	advance  time.Duration
	advanced bool
}

type reviewClearingStore struct {
	base     *memory.RunStore
	injected bool
}

func (s *reviewClearingStore) Create(ctx context.Context, run domain.Run) error {
	return s.base.Create(ctx, run)
}

func (s *reviewClearingStore) Get(ctx context.Context, runID string) (domain.Run, error) {
	return s.base.Get(ctx, runID)
}

func (s *reviewClearingStore) CompareAndSwap(ctx context.Context, runID string, expected uint64, next domain.Run) error {
	if next.PendingAdvance != nil && !s.injected {
		current, err := s.base.Get(ctx, runID)
		if err != nil {
			return err
		}
		current.Review = nil
		current.Version = expected + 1
		if err := s.base.CompareAndSwap(ctx, runID, expected, current); err != nil {
			return err
		}
		s.injected = true
		return ports.ErrConflict
	}
	return s.base.CompareAndSwap(ctx, runID, expected, next)
}

func (l *clockAdvancingLinear) Advance(ctx context.Context, request domain.AdvanceRequest) error {
	if !l.advanced {
		l.clock.Advance(l.advance)
		l.advanced = true
	}
	return l.Linear.Advance(ctx, request)
}

type rig struct {
	controller *factory.Controller
	clock      *fakeClock
	linear     *memory.Linear
	reviews    *memory.Reviews
	artifacts  *memory.Artifacts
	agent      *memory.OpenClaw
}

func newRig(t *testing.T, policy domain.Policy, turns ...memory.Turn) *rig {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)}
	linear := memory.NewLinear(
		domain.Issue{ID: "DF-1", ProjectID: "project", Priority: 1, CreatedAt: clock.now.Add(-time.Hour), State: domain.IssueInProgress},
		domain.Issue{ID: "DF-2", ProjectID: "project", Priority: 2, CreatedAt: clock.now.Add(-2 * time.Hour), State: domain.IssueReady},
	)
	artifacts := memory.NewArtifacts()
	digest := artifacts.Put("artifact://DF-1", []byte("reviewed bytes\n"))
	reviews := memory.NewReviews(domain.ReviewEvidence{
		ID: "review-1", ProjectID: "project", IssueID: "DF-1", Status: domain.ReviewApproved,
		Immutable: true, ArtifactRef: "artifact://DF-1", ArtifactSHA256: digest,
	})
	agent := memory.NewOpenClaw(turns...)
	controller := factory.NewWithClock(memory.NewRunStore(), linear, agent, reviews, artifacts, clock)
	if _, err := controller.Start(context.Background(), "run-1", "project", "DF-1", "initial step", policy); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return &rig{controller: controller, clock: clock, linear: linear, reviews: reviews, artifacts: artifacts, agent: agent}
}

func testPolicy() domain.Policy {
	return domain.Policy{LeaseDuration: time.Minute, MaxRunDuration: time.Hour, MaxAttempts: 4, MaxConsecutiveFailures: 2}
}

func TestStaleFenceRejectedAfterLeaseReacquisition(t *testing.T) {
	r := newRig(t, testPolicy())
	ctx := context.Background()
	first, err := r.controller.AcquireLease(ctx, "run-1", "worker-a")
	if err != nil {
		t.Fatalf("first AcquireLease: %v", err)
	}
	r.clock.Advance(first.ExpiresAt.Sub(r.clock.Now()))
	second, err := r.controller.AcquireLease(ctx, "run-1", "worker-b")
	if err != nil {
		t.Fatalf("second AcquireLease: %v", err)
	}
	if second.Fence <= first.Fence {
		t.Fatalf("new fence %d is not greater than stale fence %d", second.Fence, first.Fence)
	}

	err = r.controller.Checkpoint(ctx, "run-1", first.Fence, 1, "stale mutation", "go test ./... passed")
	if !errors.Is(err, factory.ErrStaleFence) {
		t.Fatalf("stale Checkpoint error = %v, want ErrStaleFence", err)
	}
	if err := r.controller.Checkpoint(ctx, "run-1", second.Fence, 1, "current mutation", "go test ./... passed"); err != nil {
		t.Fatalf("current Checkpoint: %v", err)
	}
}

func TestExpiredLeaseRejected(t *testing.T) {
	r := newRig(t, testPolicy())
	ctx := context.Background()
	lease, err := r.controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	r.clock.Advance(time.Minute)
	err = r.controller.Checkpoint(ctx, "run-1", lease.Fence, 1, "late mutation", "go test ./... passed")
	if !errors.Is(err, factory.ErrLeaseExpired) {
		t.Fatalf("Checkpoint error = %v, want ErrLeaseExpired", err)
	}
}

func TestCompletionRejectedWithoutImmutableReview(t *testing.T) {
	r := newRig(t, testPolicy())
	ctx := context.Background()
	lease, err := r.controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}

	_, err = r.controller.CompleteAndAdvance(ctx, "run-1", lease.Fence, "unit tests and diff inspection passed")
	if !errors.Is(err, factory.ErrReviewRequired) {
		t.Fatalf("CompleteAndAdvance error = %v, want ErrReviewRequired", err)
	}
	issue, err := r.linear.GetIssue(ctx, "DF-1")
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if issue.State != domain.IssueInProgress {
		t.Fatalf("issue state = %s, completion mutated Linear", issue.State)
	}
}

func TestReviewMustBeImmutableAndArtifactUnchanged(t *testing.T) {
	r := newRig(t, testPolicy())
	ctx := context.Background()
	lease, err := r.controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	mutable := domain.ReviewEvidence{
		ID: "mutable", ProjectID: "project", IssueID: "DF-1", Status: domain.ReviewApproved,
		Immutable: false, ArtifactRef: "artifact://DF-1", ArtifactSHA256: "present-but-untrusted",
	}
	r.reviews.Put(mutable)
	if err := r.controller.BindReview(ctx, "run-1", lease.Fence, "mutable"); !errors.Is(err, factory.ErrReviewRequired) {
		t.Fatalf("BindReview mutable error = %v, want ErrReviewRequired", err)
	}
	if err := r.controller.BindReview(ctx, "run-1", lease.Fence, "review-1"); err != nil {
		t.Fatalf("BindReview: %v", err)
	}
	r.artifacts.Put("artifact://DF-1", []byte("mutated after review\n"))
	_, err = r.controller.CompleteAndAdvance(ctx, "run-1", lease.Fence, "unit tests and diff inspection passed")
	if !errors.Is(err, factory.ErrReviewMismatch) {
		t.Fatalf("CompleteAndAdvance mutated artifact error = %v, want ErrReviewMismatch", err)
	}
}

func TestCompletionDeterministicallyAdvancesIssue(t *testing.T) {
	r := newRig(t, testPolicy())
	ctx := context.Background()
	lease, err := r.controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if err := r.controller.BindReview(ctx, "run-1", lease.Fence, "review-1"); err != nil {
		t.Fatalf("BindReview: %v", err)
	}
	run, err := r.controller.CompleteAndAdvance(ctx, "run-1", lease.Fence, "unit tests and immutable review passed")
	if err != nil {
		t.Fatalf("CompleteAndAdvance: %v", err)
	}
	if run.Status != domain.RunActive || run.IssueID != "DF-2" || run.Review != nil || run.PendingAdvance != nil {
		t.Fatalf("unexpected advanced run: %+v", run)
	}
	completed, _ := r.linear.GetIssue(ctx, "DF-1")
	adopted, _ := r.linear.GetIssue(ctx, "DF-2")
	if completed.State != domain.IssueCompleted || adopted.State != domain.IssueInProgress {
		t.Fatalf("issue states after advancement: current=%s next=%s", completed.State, adopted.State)
	}
}

func TestAttemptAndFailureBudgetsFailClosed(t *testing.T) {
	policy := testPolicy()
	policy.MaxConsecutiveFailures = 2
	r := newRig(t, policy,
		memory.Turn{Err: errors.New("agent unavailable")},
		memory.Turn{Result: domain.TurnResult{Step: "not enough", Evidence: "ping"}},
	)
	ctx := context.Background()
	lease, err := r.controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if _, err := r.controller.ExecuteTurn(ctx, "run-1", lease.Fence); err == nil {
		t.Fatal("first ExecuteTurn unexpectedly succeeded")
	}
	if _, err := r.controller.ExecuteTurn(ctx, "run-1", lease.Fence); !errors.Is(err, factory.ErrInvalidEvidence) {
		t.Fatalf("second ExecuteTurn error = %v, want ErrInvalidEvidence", err)
	}
	run, err := r.controller.Get(ctx, "run-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if run.Status != domain.RunBlocked || run.ConsecutiveFailures != 2 {
		t.Fatalf("run did not fail closed after failure budget: %+v", run)
	}
}

func TestAttemptBudgetBlocksBeforeDispatch(t *testing.T) {
	policy := testPolicy()
	policy.MaxAttempts = 1
	r := newRig(t, policy, memory.Turn{Result: domain.TurnResult{Step: "progress", Evidence: "go test ./... passed"}})
	ctx := context.Background()
	lease, err := r.controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if _, err := r.controller.ExecuteTurn(ctx, "run-1", lease.Fence); err != nil {
		t.Fatalf("first ExecuteTurn: %v", err)
	}
	if _, err := r.controller.ExecuteTurn(ctx, "run-1", lease.Fence); !errors.Is(err, factory.ErrBudgetExhausted) {
		t.Fatalf("second ExecuteTurn error = %v, want ErrBudgetExhausted", err)
	}
	if got := len(r.agent.Requests()); got != 1 {
		t.Fatalf("OpenClaw dispatch count = %d, want 1", got)
	}
	run, _ := r.controller.Get(ctx, "run-1")
	if run.Status != domain.RunBlocked {
		t.Fatalf("run status = %s, want blocked", run.Status)
	}
}

func TestEachAgentInvocationChargesExactlyOneAttempt(t *testing.T) {
	tests := []memory.Turn{
		{Result: domain.TurnResult{Step: "success", Evidence: "successful invocation artifact"}},
		{Err: errors.New("nonzero process exit")},
		{Result: domain.TurnResult{Step: "", Evidence: "invalid result artifact"}},
	}
	for index, turn := range tests {
		r := newRig(t, testPolicy(), turn)
		lease, err := r.controller.AcquireLease(context.Background(), "run-1", "worker")
		if err != nil {
			t.Fatal(err)
		}
		_, _ = r.controller.ExecuteTurn(context.Background(), "run-1", lease.Fence)
		run, err := r.controller.Get(context.Background(), "run-1")
		if err != nil {
			t.Fatal(err)
		}
		if run.Attempts != 1 || len(r.agent.Requests()) != 1 {
			t.Fatalf("case %d attempts=%d invocations=%d", index, run.Attempts, len(r.agent.Requests()))
		}
	}
}

func TestWallClockBudgetRejectsCheckpointAndBlocks(t *testing.T) {
	policy := testPolicy()
	policy.MaxRunDuration = 30 * time.Second
	r := newRig(t, policy)
	ctx := context.Background()
	lease, err := r.controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	r.clock.Advance(30 * time.Second)
	err = r.controller.Checkpoint(ctx, "run-1", lease.Fence, 1, "late checkpoint", "go test ./... passed")
	if !errors.Is(err, factory.ErrBudgetExhausted) {
		t.Fatalf("Checkpoint error = %v, want ErrBudgetExhausted", err)
	}
	run, err := r.controller.Get(ctx, "run-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if run.Status != domain.RunBlocked || run.CheckpointSequence != 0 {
		t.Fatalf("deadline did not fail closed: %+v", run)
	}
}

func TestTurnFinishingAtDeadlineIsRejectedAndBlocks(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)}
	linear := memory.NewLinear(domain.Issue{
		ID: "DF-1", ProjectID: "project", Priority: 1, CreatedAt: clock.now, State: domain.IssueInProgress,
	})
	agent := &clockAdvancingAgent{
		clock:   clock,
		advance: 30 * time.Second,
		result:  domain.TurnResult{Step: "late result", Evidence: "go test ./... passed"},
	}
	controller := factory.NewWithClock(memory.NewRunStore(), linear, agent, memory.NewReviews(), memory.NewArtifacts(), clock)
	policy := testPolicy()
	policy.MaxRunDuration = 30 * time.Second
	if _, err := controller.Start(context.Background(), "run-1", "project", "DF-1", "initial step", policy); err != nil {
		t.Fatalf("Start: %v", err)
	}
	lease, err := controller.AcquireLease(context.Background(), "run-1", "worker")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if _, err := controller.ExecuteTurn(context.Background(), "run-1", lease.Fence); !errors.Is(err, factory.ErrBudgetExhausted) {
		t.Fatalf("ExecuteTurn error = %v, want ErrBudgetExhausted", err)
	}
	run, err := controller.Get(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if run.Status != domain.RunBlocked || run.CheckpointSequence != 0 || len(run.Evidence) != 0 {
		t.Fatalf("late turn did not fail closed: %+v", run)
	}
}

func TestCompletionDoesNotSelectCurrentReadyIssueAsNext(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)}
	linear := memory.NewLinear(
		domain.Issue{ID: "DF-1", ProjectID: "project", Priority: 1, CreatedAt: clock.now.Add(-time.Hour), State: domain.IssueReady},
		domain.Issue{ID: "DF-2", ProjectID: "project", Priority: 2, CreatedAt: clock.now, State: domain.IssueReady},
	)
	artifacts := memory.NewArtifacts()
	digest := artifacts.Put("artifact://DF-1", []byte("reviewed bytes\n"))
	reviews := memory.NewReviews(domain.ReviewEvidence{
		ID: "review-1", ProjectID: "project", IssueID: "DF-1", Status: domain.ReviewApproved,
		Immutable: true, ArtifactRef: "artifact://DF-1", ArtifactSHA256: digest,
	})
	controller := factory.NewWithClock(memory.NewRunStore(), linear, memory.NewOpenClaw(), reviews, artifacts, clock)
	if _, err := controller.Start(context.Background(), "run-1", "project", "DF-1", "initial step", testPolicy()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	lease, err := controller.AcquireLease(context.Background(), "run-1", "worker")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if err := controller.BindReview(context.Background(), "run-1", lease.Fence, "review-1"); err != nil {
		t.Fatalf("BindReview: %v", err)
	}
	run, err := controller.CompleteAndAdvance(context.Background(), "run-1", lease.Fence, "immutable review approved DF-1")
	if err != nil {
		t.Fatalf("CompleteAndAdvance: %v", err)
	}
	if run.IssueID != "DF-2" {
		t.Fatalf("adopted issue = %q, want DF-2", run.IssueID)
	}
}

func TestExpiredWorkerCannotFinalizePendingAdvance(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)}
	baseLinear := memory.NewLinear(
		domain.Issue{ID: "DF-1", ProjectID: "project", Priority: 1, CreatedAt: clock.now.Add(-time.Hour), State: domain.IssueInProgress},
		domain.Issue{ID: "DF-2", ProjectID: "project", Priority: 2, CreatedAt: clock.now, State: domain.IssueReady},
	)
	linear := &clockAdvancingLinear{Linear: baseLinear, clock: clock, advance: time.Minute}
	artifacts := memory.NewArtifacts()
	digest := artifacts.Put("artifact://DF-1", []byte("reviewed bytes\n"))
	reviews := memory.NewReviews(domain.ReviewEvidence{
		ID: "review-1", ProjectID: "project", IssueID: "DF-1", Status: domain.ReviewApproved,
		Immutable: true, ArtifactRef: "artifact://DF-1", ArtifactSHA256: digest,
	})
	controller := factory.NewWithClock(memory.NewRunStore(), linear, memory.NewOpenClaw(), reviews, artifacts, clock)
	if _, err := controller.Start(context.Background(), "run-1", "project", "DF-1", "initial step", testPolicy()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	first, err := controller.AcquireLease(context.Background(), "run-1", "worker-a")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if err := controller.BindReview(context.Background(), "run-1", first.Fence, "review-1"); err != nil {
		t.Fatalf("BindReview: %v", err)
	}
	if _, err := controller.CompleteAndAdvance(context.Background(), "run-1", first.Fence, "immutable review approved DF-1"); !errors.Is(err, factory.ErrLeaseExpired) {
		t.Fatalf("CompleteAndAdvance error = %v, want ErrLeaseExpired", err)
	}
	run, err := controller.Get(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if run.PendingAdvance == nil || run.IssueID != "DF-1" {
		t.Fatalf("expired worker finalized controller state: %+v", run)
	}
	second, err := controller.AcquireLease(context.Background(), "run-1", "worker-b")
	if err != nil {
		t.Fatalf("reacquire lease: %v", err)
	}
	if err := controller.Checkpoint(context.Background(), "run-1", second.Fence, 1, "new work", "go test ./... passed"); !errors.Is(err, factory.ErrInvalidTransition) {
		t.Fatalf("Checkpoint with pending advance error = %v, want ErrInvalidTransition", err)
	}
	run, err = controller.CompleteAndAdvance(context.Background(), "run-1", second.Fence, "immutable review approved DF-1")
	if err != nil {
		t.Fatalf("retry CompleteAndAdvance: %v", err)
	}
	if run.IssueID != "DF-2" || run.PendingAdvance != nil {
		t.Fatalf("recovered advancement state: %+v", run)
	}
}

func TestConcurrentProgressInvalidatesReviewBeforeAdvanceIsFrozen(t *testing.T) {
	clock := &fakeClock{now: time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)}
	linear := memory.NewLinear(
		domain.Issue{ID: "DF-1", ProjectID: "project", Priority: 1, CreatedAt: clock.now.Add(-time.Hour), State: domain.IssueInProgress},
		domain.Issue{ID: "DF-2", ProjectID: "project", Priority: 2, CreatedAt: clock.now, State: domain.IssueReady},
	)
	artifacts := memory.NewArtifacts()
	digest := artifacts.Put("artifact://DF-1", []byte("reviewed bytes\n"))
	reviews := memory.NewReviews(domain.ReviewEvidence{
		ID: "review-1", ProjectID: "project", IssueID: "DF-1", Status: domain.ReviewApproved,
		Immutable: true, ArtifactRef: "artifact://DF-1", ArtifactSHA256: digest,
	})
	store := &reviewClearingStore{base: memory.NewRunStore()}
	controller := factory.NewWithClock(store, linear, memory.NewOpenClaw(), reviews, artifacts, clock)
	if _, err := controller.Start(context.Background(), "run-1", "project", "DF-1", "initial step", testPolicy()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	lease, err := controller.AcquireLease(context.Background(), "run-1", "worker")
	if err != nil {
		t.Fatalf("AcquireLease: %v", err)
	}
	if err := controller.BindReview(context.Background(), "run-1", lease.Fence, "review-1"); err != nil {
		t.Fatalf("BindReview: %v", err)
	}
	if _, err := controller.CompleteAndAdvance(context.Background(), "run-1", lease.Fence, "immutable review approved DF-1"); !errors.Is(err, factory.ErrReviewMismatch) {
		t.Fatalf("CompleteAndAdvance error = %v, want ErrReviewMismatch", err)
	}
	run, err := controller.Get(context.Background(), "run-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if run.PendingAdvance != nil || run.Review != nil {
		t.Fatalf("stale review was frozen for advancement: %+v", run)
	}
	issue, err := linear.GetIssue(context.Background(), "DF-1")
	if err != nil {
		t.Fatalf("GetIssue: %v", err)
	}
	if issue.State != domain.IssueInProgress {
		t.Fatalf("issue state = %s, want in_progress", issue.State)
	}
}
