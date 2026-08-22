package factory

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

const maxCASAttempts = 16

var (
	ErrAlreadyExists     = ports.ErrAlreadyExists
	ErrNotFound          = ports.ErrNotFound
	ErrConflict          = ports.ErrConflict
	ErrBusy              = ports.ErrBusy
	ErrInvalidTransition = ports.ErrInvalidTransition
	ErrStaleFence        = ports.ErrStaleFence
)

type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }

type Controller struct {
	runs      ports.RunStore
	linear    ports.Linear
	openclaw  ports.OpenClaw
	reviews   ports.Reviews
	artifacts ports.Artifacts
	clock     Clock
}

func New(runs ports.RunStore, linear ports.Linear, openclaw ports.OpenClaw, reviews ports.Reviews, artifacts ports.Artifacts) *Controller {
	return NewWithClock(runs, linear, openclaw, reviews, artifacts, realClock{})
}

func NewWithClock(runs ports.RunStore, linear ports.Linear, openclaw ports.OpenClaw, reviews ports.Reviews, artifacts ports.Artifacts, clock Clock) *Controller {
	return &Controller{runs: runs, linear: linear, openclaw: openclaw, reviews: reviews, artifacts: artifacts, clock: clock}
}

func (c *Controller) Start(ctx context.Context, runID, projectID, issueID, step string, policy domain.Policy) (domain.Run, error) {
	if runID == "" || projectID == "" || issueID == "" || strings.TrimSpace(step) == "" || !policy.Valid() {
		return domain.Run{}, fmt.Errorf("%w: invalid start parameters", ErrInvalidTransition)
	}
	issue, err := c.linear.GetIssue(ctx, issueID)
	if err != nil {
		return domain.Run{}, fmt.Errorf("get initial issue: %w", err)
	}
	if issue.ProjectID != projectID || (issue.State != domain.IssueReady && issue.State != domain.IssueInProgress) || issue.Blocked {
		return domain.Run{}, fmt.Errorf("%w: initial issue is unavailable", ErrInvalidTransition)
	}
	now := c.clock.Now()
	run := domain.Run{
		ID:         runID,
		ProjectID:  projectID,
		IssueID:    issueID,
		Status:     domain.RunActive,
		Step:       step,
		Policy:     policy,
		StartedAt:  now,
		DeadlineAt: now.Add(policy.MaxRunDuration),
		Version:    1,
	}
	if err := c.runs.Create(ctx, run); err != nil {
		return domain.Run{}, err
	}
	return run, nil
}

func (c *Controller) Get(ctx context.Context, runID string) (domain.Run, error) {
	return c.runs.Get(ctx, runID)
}

func (c *Controller) AcquireLease(ctx context.Context, runID, holder string) (domain.Lease, error) {
	if strings.TrimSpace(holder) == "" {
		return domain.Lease{}, fmt.Errorf("%w: empty lease holder", ErrInvalidTransition)
	}
	var acquired domain.Lease
	budgetExhausted := false
	err := c.update(ctx, runID, func(run *domain.Run) error {
		now := c.clock.Now()
		if run.Status != domain.RunActive {
			return ErrInvalidTransition
		}
		if !now.Before(run.DeadlineAt) {
			block(run, now, "wall-clock budget exhausted")
			budgetExhausted = true
			return nil
		}
		if run.Lease.Fence != 0 && now.Before(run.Lease.ExpiresAt) {
			return ErrLeaseHeld
		}
		run.NextFence++
		run.Lease = domain.Lease{Holder: holder, Fence: run.NextFence, ExpiresAt: now.Add(run.Policy.LeaseDuration)}
		acquired = run.Lease
		return nil
	})
	if err == nil && budgetExhausted {
		err = ErrBudgetExhausted
	}
	return acquired, err
}

func (c *Controller) Checkpoint(ctx context.Context, runID string, fence, sequence uint64, step, evidence string) error {
	budgetExhausted := false
	err := c.update(ctx, runID, func(run *domain.Run) error {
		now := c.clock.Now()
		if err := validateLease(run, fence, now); err != nil {
			return err
		}
		if run.PendingAdvance != nil {
			return fmt.Errorf("%w: issue advancement is pending", ErrInvalidTransition)
		}
		if !now.Before(run.DeadlineAt) {
			block(run, now, "wall-clock budget exhausted")
			budgetExhausted = true
			return nil
		}
		if sequence != run.CheckpointSequence+1 {
			return fmt.Errorf("%w: checkpoint sequence %d, want %d", ErrInvalidTransition, sequence, run.CheckpointSequence+1)
		}
		if !concreteEvidence(evidence) || strings.TrimSpace(step) == "" {
			return ErrInvalidEvidence
		}
		run.Step = step
		run.CheckpointSequence = sequence
		run.Lease.ExpiresAt = now.Add(run.Policy.LeaseDuration)
		run.ConsecutiveFailures = 0
		run.Review = nil
		run.Evidence = appendEvidence(run.Evidence, evidence)
		return nil
	})
	if err == nil && budgetExhausted {
		return ErrBudgetExhausted
	}
	return err
}

func (c *Controller) ExecuteTurn(ctx context.Context, runID string, fence uint64) (domain.TurnResult, error) {
	request, err := c.ReserveTurn(ctx, runID, fence)
	if err != nil {
		return domain.TurnResult{}, err
	}
	if err := c.MarkDispatchStarted(ctx, runID, fence, request.Attempt); err != nil {
		return domain.TurnResult{}, err
	}
	result, turnErr := c.openclaw.ExecuteTurn(ctx, request)
	return c.RecordTurn(context.WithoutCancel(ctx), runID, fence, request.Attempt, result, turnErr)
}

// ReserveTurn atomically spends an attempt before any external process is started.
func (c *Controller) ReserveTurn(ctx context.Context, runID string, fence uint64) (domain.TurnRequest, error) {
	var request domain.TurnRequest
	err := c.update(ctx, runID, func(run *domain.Run) error {
		now := c.clock.Now()
		if err := validateLease(run, fence, now); err != nil {
			return err
		}
		if run.PendingDispatch != nil {
			return ErrDispatchPending
		}
		if run.PendingAdvance != nil {
			return fmt.Errorf("%w: issue advancement is pending", ErrInvalidTransition)
		}
		if run.Attempts >= run.Policy.MaxAttempts || !now.Before(run.DeadlineAt) {
			block(run, now, "attempt or wall-clock budget exhausted")
			return nil
		}
		run.Attempts++
		run.PendingDispatch = &domain.PendingDispatch{Attempt: run.Attempts, Fence: fence, State: domain.DispatchReserved, ReservedAt: now}
		request = domain.TurnRequest{RunID: run.ID, ProjectID: run.ProjectID, IssueID: run.IssueID, Attempt: run.Attempts, Fence: fence, LeaseUntil: run.Lease.ExpiresAt}
		return nil
	})
	if err != nil {
		return domain.TurnRequest{}, err
	}
	if request.RunID == "" {
		return domain.TurnRequest{}, ErrBudgetExhausted
	}
	return request, nil
}

// MarkDispatchStarted is committed immediately before exec. A crash on either side of
// this acknowledgement leaves PendingDispatch set and recovery refuses redispatch.
func (c *Controller) MarkDispatchStarted(ctx context.Context, runID string, fence uint64, attempt int) error {
	return c.update(ctx, runID, func(run *domain.Run) error {
		now := c.clock.Now()
		if err := validateLease(run, fence, now); err != nil {
			return err
		}
		if run.PendingDispatch == nil || run.PendingDispatch.Attempt != attempt || run.PendingDispatch.Fence != fence || run.PendingDispatch.State != domain.DispatchReserved {
			return ErrDispatchPending
		}
		run.PendingDispatch.State = domain.DispatchStarted
		run.PendingDispatch.StartedAt = now
		return nil
	})
}

// RecordTurn clears the durable dispatch only while the same live worker holds its fence.
func (c *Controller) RecordTurn(ctx context.Context, runID string, fence uint64, attempt int, result domain.TurnResult, turnErr error) (domain.TurnResult, error) {
	budgetExhausted := false
	applyErr := c.update(ctx, runID, func(run *domain.Run) error {
		now := c.clock.Now()
		if err := validateLease(run, fence, now); err != nil {
			return err
		}
		if run.PendingDispatch == nil || run.PendingDispatch.Attempt != attempt || run.PendingDispatch.Fence != fence || run.PendingDispatch.State != domain.DispatchStarted {
			return ErrDispatchPending
		}
		run.PendingDispatch = nil
		if !now.Before(run.DeadlineAt) {
			block(run, now, "wall-clock budget exhausted")
			budgetExhausted = true
			return nil
		}
		if turnErr != nil || !concreteEvidence(result.Evidence) || strings.TrimSpace(result.Step) == "" {
			run.ConsecutiveFailures++
			if run.ConsecutiveFailures >= run.Policy.MaxConsecutiveFailures {
				block(run, now, "consecutive agent-turn failure budget exhausted")
			}
			return nil
		}
		if result.ResponseRef == "" || !validSHA256(result.ResponseSHA256) || result.SessionKey == "" {
			run.ConsecutiveFailures++
			if run.ConsecutiveFailures >= run.Policy.MaxConsecutiveFailures {
				block(run, now, "consecutive agent-turn failure budget exhausted")
			}
			turnErr = ErrInvalidEvidence
			return nil
		}
		run.Step = result.Step
		run.CheckpointSequence++
		run.Lease.ExpiresAt = now.Add(run.Policy.LeaseDuration)
		run.ConsecutiveFailures = 0
		run.Review = nil
		run.Evidence = appendEvidence(run.Evidence, result.Evidence)
		run.LastTurnArtifact = &domain.TurnArtifact{Attempt: attempt, SessionKey: result.SessionKey, ResponseRef: result.ResponseRef, ResponseSHA256: result.ResponseSHA256}
		return nil
	})
	if applyErr != nil {
		return domain.TurnResult{}, applyErr
	}
	if budgetExhausted {
		return domain.TurnResult{}, ErrBudgetExhausted
	}
	if turnErr != nil {
		return domain.TurnResult{}, fmt.Errorf("agent turn: %w", turnErr)
	}
	if !concreteEvidence(result.Evidence) || strings.TrimSpace(result.Step) == "" {
		return domain.TurnResult{}, ErrInvalidEvidence
	}
	return result, nil
}

// BlockAmbiguousDispatch makes process-loss ambiguity explicit and terminal. Operators
// can inspect the attempt/fence in durable state; v0.1 intentionally has no unsafe replay.
func (c *Controller) BlockAmbiguousDispatch(ctx context.Context, runID string) error {
	return c.update(ctx, runID, func(run *domain.Run) error {
		if run.Status != domain.RunActive || run.PendingDispatch == nil {
			return ErrDispatchPending
		}
		attempt := run.PendingDispatch.Attempt
		run.PendingDispatch = nil
		block(run, c.clock.Now(), fmt.Sprintf("OpenClaw attempt %d is ambiguous and requires manual resolution", attempt))
		return nil
	})
}

func (c *Controller) BindReview(ctx context.Context, runID string, fence uint64, reviewID string) error {
	review, err := c.reviews.GetReview(ctx, reviewID)
	if err != nil {
		return fmt.Errorf("get review: %w", err)
	}
	digest, err := c.artifacts.SHA256(ctx, review.ArtifactRef)
	if err != nil {
		return fmt.Errorf("hash reviewed artifact: %w", err)
	}
	budgetExhausted := false
	err = c.update(ctx, runID, func(run *domain.Run) error {
		now := c.clock.Now()
		if err := validateLease(run, fence, now); err != nil {
			return err
		}
		if run.PendingAdvance != nil {
			return fmt.Errorf("%w: issue advancement is pending", ErrInvalidTransition)
		}
		if !now.Before(run.DeadlineAt) {
			block(run, now, "wall-clock budget exhausted")
			budgetExhausted = true
			return nil
		}
		if err := validateReview(run, review, digest); err != nil {
			return err
		}
		run.Review = &domain.ReviewBinding{ReviewID: review.ID, ArtifactRef: review.ArtifactRef, ArtifactSHA256: review.ArtifactSHA256}
		return nil
	})
	if err == nil && budgetExhausted {
		return ErrBudgetExhausted
	}
	return err
}

func (c *Controller) CompleteAndAdvance(ctx context.Context, runID string, fence uint64, evidence string) (domain.Run, error) {
	if !concreteEvidence(evidence) {
		return domain.Run{}, ErrInvalidEvidence
	}

	run, err := c.runs.Get(ctx, runID)
	if err != nil {
		return domain.Run{}, err
	}
	if err := validateLease(&run, fence, c.clock.Now()); err != nil {
		return domain.Run{}, err
	}
	if !c.clock.Now().Before(run.DeadlineAt) {
		_ = c.update(ctx, runID, func(current *domain.Run) error {
			if err := validateFence(current, fence); err != nil {
				return err
			}
			if current.Status == domain.RunActive {
				block(current, c.clock.Now(), "wall-clock budget exhausted")
			}
			return nil
		})
		return domain.Run{}, ErrBudgetExhausted
	}
	if run.Review == nil {
		return domain.Run{}, ErrReviewRequired
	}
	review, err := c.reviews.GetReview(ctx, run.Review.ReviewID)
	if err != nil {
		return domain.Run{}, fmt.Errorf("reload review: %w", err)
	}
	digest, err := c.artifacts.SHA256(ctx, review.ArtifactRef)
	if err != nil {
		return domain.Run{}, fmt.Errorf("rehash reviewed artifact: %w", err)
	}
	if err := validateReview(&run, review, digest); err != nil {
		return domain.Run{}, err
	}
	if review.ArtifactRef != run.Review.ArtifactRef || review.ArtifactSHA256 != run.Review.ArtifactSHA256 {
		return domain.Run{}, ErrReviewMismatch
	}

	if run.PendingAdvance == nil {
		issues, listErr := c.linear.ListProjectIssues(ctx, run.ProjectID)
		if listErr != nil {
			return domain.Run{}, fmt.Errorf("list project issues: %w", listErr)
		}
		candidates := make([]domain.Issue, 0, len(issues))
		for _, issue := range issues {
			if issue.ID != run.IssueID {
				candidates = append(candidates, issue)
			}
		}
		next, found := domain.SelectNext(candidates)
		nextID := ""
		if found {
			nextID = next.ID
		}
		pending := &domain.PendingAdvance{
			CurrentIssueID: run.IssueID,
			NextIssueID:    nextID,
			Evidence:       evidence,
			ReviewID:       review.ID,
			IdempotencyKey: advanceKey(run.ID, run.IssueID, review.ID),
		}
		if err := c.update(ctx, runID, func(current *domain.Run) error {
			now := c.clock.Now()
			if err := validateLease(current, fence, now); err != nil {
				return err
			}
			if !now.Before(current.DeadlineAt) {
				return ErrBudgetExhausted
			}
			if current.PendingAdvance != nil {
				return ErrConflict
			}
			if current.IssueID != run.IssueID || current.Review == nil ||
				current.Review.ReviewID != review.ID || current.Review.ArtifactRef != review.ArtifactRef ||
				current.Review.ArtifactSHA256 != review.ArtifactSHA256 {
				return ErrReviewMismatch
			}
			current.PendingAdvance = pending
			return nil
		}); err != nil && !errors.Is(err, ErrConflict) {
			return domain.Run{}, err
		}
		run, err = c.runs.Get(ctx, runID)
		if err != nil {
			return domain.Run{}, err
		}
	}

	pending := *run.PendingAdvance
	if pending.CurrentIssueID != run.IssueID || pending.ReviewID != review.ID {
		return domain.Run{}, fmt.Errorf("%w: pending advancement changed", ErrInvalidTransition)
	}
	if err := validateLease(&run, fence, c.clock.Now()); err != nil {
		return domain.Run{}, err
	}
	if !c.clock.Now().Before(run.DeadlineAt) {
		return domain.Run{}, ErrBudgetExhausted
	}
	if err := c.reviews.ConsumeReview(ctx, review.ID, run.ID, review.ArtifactSHA256); err != nil {
		return domain.Run{}, fmt.Errorf("consume review: %w", err)
	}
	run, err = c.runs.Get(ctx, runID)
	if err != nil {
		return domain.Run{}, err
	}
	if err := validateLease(&run, fence, c.clock.Now()); err != nil {
		return domain.Run{}, err
	}
	if !c.clock.Now().Before(run.DeadlineAt) {
		return domain.Run{}, ErrBudgetExhausted
	}
	request := domain.AdvanceRequest{
		RunID: run.ID, ProjectID: run.ProjectID, CurrentIssueID: pending.CurrentIssueID,
		NextIssueID: pending.NextIssueID, Evidence: pending.Evidence, ReviewID: pending.ReviewID,
		Fence: fence, IdempotencyKey: pending.IdempotencyKey,
	}
	if err := c.linear.Advance(ctx, request); err != nil {
		return domain.Run{}, fmt.Errorf("advance issue: %w", err)
	}

	err = c.update(ctx, runID, func(current *domain.Run) error {
		now := c.clock.Now()
		if err := validateLease(current, fence, now); err != nil {
			return err
		}
		if !now.Before(current.DeadlineAt) {
			return ErrBudgetExhausted
		}
		if current.PendingAdvance == nil || current.PendingAdvance.IdempotencyKey != pending.IdempotencyKey {
			return fmt.Errorf("%w: pending advancement was replaced", ErrInvalidTransition)
		}
		current.Evidence = appendEvidence(current.Evidence, pending.Evidence)
		current.PendingAdvance = nil
		current.Review = nil
		current.ConsecutiveFailures = 0
		if pending.NextIssueID == "" {
			current.Status = domain.RunComplete
			current.Step = "no unblocked ready issues remain"
			current.FinishedAt = now
			current.Lease.ExpiresAt = now
			return nil
		}
		current.IssueID = pending.NextIssueID
		current.Step = "adopted " + pending.NextIssueID
		current.CheckpointSequence = 0
		return nil
	})
	if err != nil {
		return domain.Run{}, err
	}
	return c.runs.Get(ctx, runID)
}

func (c *Controller) update(ctx context.Context, runID string, mutate func(*domain.Run) error) error {
	for range maxCASAttempts {
		run, err := c.runs.Get(ctx, runID)
		if err != nil {
			return err
		}
		expected := run.Version
		if err := mutate(&run); err != nil {
			return err
		}
		run.Version = expected + 1
		if err := c.runs.CompareAndSwap(ctx, runID, expected, run); err != nil {
			if errors.Is(err, ErrConflict) {
				continue
			}
			return err
		}
		return nil
	}
	return ErrConflict
}

func validateLease(run *domain.Run, fence uint64, now time.Time) error {
	if run.Status != domain.RunActive {
		return ErrInvalidTransition
	}
	if err := validateFence(run, fence); err != nil {
		return err
	}
	if !now.Before(run.Lease.ExpiresAt) {
		return ErrLeaseExpired
	}
	return nil
}

func validateFence(run *domain.Run, fence uint64) error {
	if fence == 0 || fence != run.Lease.Fence {
		return ErrStaleFence
	}
	return nil
}

func validateReview(run *domain.Run, review domain.ReviewEvidence, actualDigest string) error {
	if review.Status != domain.ReviewApproved || !review.Immutable || review.ID == "" || review.ArtifactRef == "" || review.ArtifactSHA256 == "" {
		return ErrReviewRequired
	}
	if review.ProjectID != run.ProjectID || review.IssueID != run.IssueID ||
		(review.ConsumedByRun != "" && review.ConsumedByRun != run.ID) || actualDigest != review.ArtifactSHA256 {
		return ErrReviewMismatch
	}
	return nil
}

func concreteEvidence(evidence string) bool {
	normalized := strings.ToLower(strings.TrimSpace(evidence))
	if len(normalized) < 8 {
		return false
	}
	switch normalized {
	case "ping", "pong", "heartbeat", "status", "still working", "working":
		return false
	default:
		return true
	}
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	for _, r := range value {
		if !strings.ContainsRune("0123456789abcdef", r) {
			return false
		}
	}
	return true
}

func appendEvidence(existing []string, evidence string) []string {
	result := append(existing, evidence)
	if len(result) > 20 {
		result = result[len(result)-20:]
	}
	return result
}

func block(run *domain.Run, now time.Time, reason string) {
	run.Status = domain.RunBlocked
	run.BlockedReason = reason
	run.Step = reason
	run.FinishedAt = now
	run.Lease.ExpiresAt = now
}

func advanceKey(runID, issueID, reviewID string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(runID+"\x00"+issueID+"\x00"+reviewID)))
}
