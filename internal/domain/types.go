package domain

import "time"

type RunStatus string

const (
	RunActive   RunStatus = "active"
	RunBlocked  RunStatus = "blocked"
	RunComplete RunStatus = "complete"
)

type IssueState string

const (
	IssueReady      IssueState = "ready"
	IssueInProgress IssueState = "in_progress"
	IssueCompleted  IssueState = "completed"
)

type Policy struct {
	LeaseDuration          time.Duration
	MaxRunDuration         time.Duration
	MaxAttempts            int
	MaxConsecutiveFailures int
}

func (p Policy) Valid() bool {
	return p.LeaseDuration > 0 && p.MaxRunDuration > 0 && p.MaxAttempts > 0 && p.MaxConsecutiveFailures > 0
}

type Lease struct {
	Holder    string    `json:"holder"`
	Fence     uint64    `json:"fence"`
	ExpiresAt time.Time `json:"expires_at"`
}

type ReviewBinding struct {
	ReviewID       string `json:"review_id"`
	ArtifactRef    string `json:"artifact_ref"`
	ArtifactSHA256 string `json:"artifact_sha256"`
}

type PendingAdvance struct {
	CurrentIssueID string `json:"current_issue_id"`
	NextIssueID    string `json:"next_issue_id,omitempty"`
	Evidence       string `json:"evidence"`
	ReviewID       string `json:"review_id"`
	IdempotencyKey string `json:"idempotency_key"`
}

type DispatchState string

const (
	DispatchReserved DispatchState = "reserved"
	DispatchStarted  DispatchState = "started"
)

// PendingDispatch is the durable boundary around an external OpenClaw invocation.
// Any persisted value is ambiguous after process loss and must never be dispatched again.
type PendingDispatch struct {
	Attempt    int           `json:"attempt"`
	Fence      uint64        `json:"fence"`
	State      DispatchState `json:"state"`
	ReservedAt time.Time     `json:"reserved_at"`
	StartedAt  time.Time     `json:"started_at,omitempty"`
}

type TurnArtifact struct {
	Attempt        int    `json:"attempt"`
	SessionKey     string `json:"session_key"`
	ResponseRef    string `json:"response_ref"`
	ResponseSHA256 string `json:"response_sha256"`
}

type Run struct {
	ID                  string           `json:"id"`
	ProjectID           string           `json:"project_id"`
	IssueID             string           `json:"issue_id"`
	Status              RunStatus        `json:"status"`
	Step                string           `json:"step"`
	Policy              Policy           `json:"policy"`
	StartedAt           time.Time        `json:"started_at"`
	DeadlineAt          time.Time        `json:"deadline_at"`
	FinishedAt          time.Time        `json:"finished_at,omitempty"`
	Lease               Lease            `json:"lease"`
	NextFence           uint64           `json:"next_fence"`
	CheckpointSequence  uint64           `json:"checkpoint_sequence"`
	Attempts            int              `json:"attempts"`
	ConsecutiveFailures int              `json:"consecutive_failures"`
	BlockedReason       string           `json:"blocked_reason,omitempty"`
	Evidence            []string         `json:"evidence,omitempty"`
	Review              *ReviewBinding   `json:"review,omitempty"`
	PendingDispatch     *PendingDispatch `json:"pending_dispatch,omitempty"`
	LastTurnArtifact    *TurnArtifact    `json:"last_turn_artifact,omitempty"`
	PendingAdvance      *PendingAdvance  `json:"pending_advance,omitempty"`
	Version             uint64           `json:"version"`
}

type Issue struct {
	ID         string
	Identifier string
	ProjectID  string
	Title      string
	Priority   int
	CreatedAt  time.Time
	State      IssueState
	Blocked    bool
}

type ReviewStatus string

const ReviewApproved ReviewStatus = "approved"

type ReviewEvidence struct {
	ID             string
	ProjectID      string
	IssueID        string
	Status         ReviewStatus
	Immutable      bool
	ArtifactRef    string
	ArtifactSHA256 string
	ConsumedByRun  string
}

type TurnRequest struct {
	RunID      string
	ProjectID  string
	IssueID    string
	Attempt    int
	Fence      uint64
	LeaseUntil time.Time
}

type TurnResult struct {
	Step           string
	Evidence       string
	SessionKey     string
	ResponseRef    string
	ResponseSHA256 string
}

type AdvanceRequest struct {
	RunID          string
	ProjectID      string
	CurrentIssueID string
	NextIssueID    string
	Evidence       string
	ReviewID       string
	Fence          uint64
	IdempotencyKey string
}

// StepAdoptedPrefix is the durable prefix set on a run step when a verified
// advancement adopts a frozen next issue remotely.
const StepAdoptedPrefix = "adopted "
