package app

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/adapters/memory"
	durablesqlite "github.com/ControlStackAI/dark-factory/internal/adapters/sqlite"
	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/factory"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

const durableRunID = "durable-dry-run"

// DurableDryRun executes or resumes a credential-free single-issue fixture. Repeating
// the command against the same path reopens and validates the database, then returns the
// already completed run without spending another attempt or applying another advancement.
func DurableDryRun(ctx context.Context, path string) (domain.Run, error) {
	store, err := durablesqlite.Open(path)
	if err != nil {
		return domain.Run{}, err
	}
	defer store.Close()

	run, err := store.Get(ctx, durableRunID)
	if errors.Is(err, ports.ErrNotFound) {
		if err := seedDurableFixture(ctx, store); err != nil {
			return domain.Run{}, err
		}
		controller := recoveryController(store)
		policy := domain.Policy{LeaseDuration: time.Minute, MaxRunDuration: time.Hour, MaxAttempts: 3, MaxConsecutiveFailures: 2}
		run, err = controller.Start(ctx, durableRunID, "durable-demo", "DR-1", "start durable recovery", policy)
		if err != nil {
			return domain.Run{}, err
		}
	} else if err != nil {
		return domain.Run{}, err
	}
	if run.Status == domain.RunComplete || run.Status == domain.RunBlocked {
		return run, nil
	}

	controller := recoveryController(store)
	fence := run.Lease.Fence
	now := time.Now().UTC()
	if fence == 0 || !now.Before(run.Lease.ExpiresAt) {
		lease, err := controller.AcquireLease(ctx, durableRunID, "factoryd-recovery")
		if err != nil {
			return domain.Run{}, err
		}
		fence = lease.Fence
	} else {
		return domain.Run{}, factory.ErrLeaseHeld
	}

	run, err = controller.Get(ctx, durableRunID)
	if err != nil {
		return domain.Run{}, err
	}
	if run.PendingAdvance != nil {
		return controller.CompleteAndAdvance(ctx, durableRunID, fence, "frozen durable evidence is authoritative")
	}
	if run.CheckpointSequence == 0 {
		if _, err := controller.ExecuteTurn(ctx, durableRunID, fence); err != nil {
			return domain.Run{}, err
		}
	}
	run, err = controller.Get(ctx, durableRunID)
	if err != nil {
		return domain.Run{}, err
	}
	if run.Review == nil {
		if err := controller.BindReview(ctx, durableRunID, fence, "durable-review"); err != nil {
			return domain.Run{}, err
		}
	}
	return controller.CompleteAndAdvance(ctx, durableRunID, fence, "durable-review approved immutable SQLite artifact")
}

func recoveryController(store *durablesqlite.Store) *factory.Controller {
	agent := memory.NewOpenClaw(memory.Turn{Result: domain.TurnResult{
		Step: "verified restart-safe durable slice", Evidence: "SQLite recovery command fixture completed",
	}})
	return factory.New(store, store, agent, store, store)
}

func seedDurableFixture(ctx context.Context, store *durablesqlite.Store) error {
	now := time.Now().UTC()
	if err := store.EnsureIssue(ctx, domain.Issue{
		ID: "DR-1", ProjectID: "durable-demo", Title: "exercise durable recovery",
		Priority: 1, CreatedAt: now, State: domain.IssueInProgress,
	}); err != nil {
		return fmt.Errorf("seed durable issue: %w", err)
	}
	digest, err := store.EnsureArtifact(ctx, "sqlite://durable-review", []byte("credential-free durable recovery artifact\n"))
	if err != nil {
		return fmt.Errorf("seed durable artifact: %w", err)
	}
	if err := store.EnsureReview(ctx, domain.ReviewEvidence{
		ID: "durable-review", ProjectID: "durable-demo", IssueID: "DR-1", Status: domain.ReviewApproved,
		Immutable: true, ArtifactRef: "sqlite://durable-review", ArtifactSHA256: digest,
	}); err != nil {
		return fmt.Errorf("seed durable review: %w", err)
	}
	return nil
}
