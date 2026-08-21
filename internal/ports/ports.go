package ports

import (
	"context"

	"github.com/ControlStackAI/dark-factory/internal/domain"
)

// RunStore is the controller's durable state boundary. CompareAndSwap must
// atomically reject updates whose expected version is no longer current.
type RunStore interface {
	Create(context.Context, domain.Run) error
	Get(context.Context, string) (domain.Run, error)
	CompareAndSwap(context.Context, string, uint64, domain.Run) error
}

// Linear is the issue-control-plane contract. Advance must be idempotent for a
// repeated IdempotencyKey and must apply completion/adoption as one logical operation.
type Linear interface {
	GetIssue(context.Context, string) (domain.Issue, error)
	ListProjectIssues(context.Context, string) ([]domain.Issue, error)
	Advance(context.Context, domain.AdvanceRequest) error
}

// OpenClaw executes one bounded agent turn. It cannot mutate controller state;
// progress is accepted only when the controller validates the returned evidence.
type OpenClaw interface {
	ExecuteTurn(context.Context, domain.TurnRequest) (domain.TurnResult, error)
}

// Reviews supplies immutable, independently produced review metadata.
type Reviews interface {
	GetReview(context.Context, string) (domain.ReviewEvidence, error)
	// ConsumeReview atomically binds the immutable evidence to a run. Repeating
	// the same run/digest is idempotent; a different consumer must be rejected.
	ConsumeReview(context.Context, string, string, string) error
}

// Artifacts independently recomputes the digest of the frozen review artifact.
type Artifacts interface {
	SHA256(context.Context, string) (string, error)
}
