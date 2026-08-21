package app

import (
	"context"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/adapters/memory"
	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/factory"
)

func DryRun(ctx context.Context) (domain.Run, error) {
	now := time.Now().UTC()
	issues := memory.NewLinear(
		domain.Issue{ID: "DF-1", ProjectID: "demo", Title: "bootstrap controller", Priority: 1, CreatedAt: now.Add(-2 * time.Hour), State: domain.IssueInProgress},
		domain.Issue{ID: "DF-2", ProjectID: "demo", Title: "document adapters", Priority: 2, CreatedAt: now.Add(-time.Hour), State: domain.IssueReady},
	)
	artifacts := memory.NewArtifacts()
	digest := artifacts.Put("memory://review/DF-1", []byte("reviewed dry-run artifact\n"))
	reviews := memory.NewReviews(domain.ReviewEvidence{
		ID: "review-DF-1", ProjectID: "demo", IssueID: "DF-1", Status: domain.ReviewApproved,
		Immutable: true, ArtifactRef: "memory://review/DF-1", ArtifactSHA256: digest,
	})
	agent := memory.NewOpenClaw(memory.Turn{Result: domain.TurnResult{
		Step: "verified controller vertical slice", Evidence: "go test ./... passed for dry-run fixture",
	}})
	controller := factory.New(memory.NewRunStore(), issues, agent, reviews, artifacts)
	policy := domain.Policy{LeaseDuration: time.Minute, MaxRunDuration: time.Hour, MaxAttempts: 3, MaxConsecutiveFailures: 2}
	if _, err := controller.Start(ctx, "dry-run", "demo", "DF-1", "bootstrap", policy); err != nil {
		return domain.Run{}, err
	}
	lease, err := controller.AcquireLease(ctx, "dry-run", "factoryd")
	if err != nil {
		return domain.Run{}, err
	}
	if _, err := controller.ExecuteTurn(ctx, "dry-run", lease.Fence); err != nil {
		return domain.Run{}, err
	}
	if err := controller.BindReview(ctx, "dry-run", lease.Fence, "review-DF-1"); err != nil {
		return domain.Run{}, err
	}
	return controller.CompleteAndAdvance(ctx, "dry-run", lease.Fence, "review-DF-1 approved immutable artifact")
}
