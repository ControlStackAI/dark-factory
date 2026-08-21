package memory

import (
	"context"
	"fmt"
	"sync"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

type Linear struct {
	mu          sync.RWMutex
	issues      map[string]domain.Issue
	applied     map[string]domain.AdvanceRequest
	latestFence map[string]uint64
}

func NewLinear(issues ...domain.Issue) *Linear {
	adapter := &Linear{
		issues:      make(map[string]domain.Issue, len(issues)),
		applied:     make(map[string]domain.AdvanceRequest),
		latestFence: make(map[string]uint64),
	}
	for _, issue := range issues {
		adapter.issues[issue.ID] = issue
	}
	return adapter
}

func (l *Linear) GetIssue(_ context.Context, id string) (domain.Issue, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	issue, exists := l.issues[id]
	if !exists {
		return domain.Issue{}, ports.ErrNotFound
	}
	return issue, nil
}

func (l *Linear) ListProjectIssues(_ context.Context, projectID string) ([]domain.Issue, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	issues := make([]domain.Issue, 0)
	for _, issue := range l.issues {
		if issue.ProjectID == projectID {
			issues = append(issues, issue)
		}
	}
	return issues, nil
}

func (l *Linear) Advance(_ context.Context, request domain.AdvanceRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if applied, exists := l.applied[request.IdempotencyKey]; exists {
		if sameAdvance(applied, request) {
			return nil
		}
		return fmt.Errorf("%w: idempotency key reused for a different advancement", ports.ErrConflict)
	}
	if request.Fence < l.latestFence[request.RunID] {
		return ports.ErrStaleFence
	}
	current, exists := l.issues[request.CurrentIssueID]
	if !exists || current.ProjectID != request.ProjectID {
		return ports.ErrNotFound
	}
	if request.NextIssueID != "" {
		next, nextExists := l.issues[request.NextIssueID]
		if !nextExists || next.ProjectID != request.ProjectID || next.State != domain.IssueReady || next.Blocked {
			return fmt.Errorf("%w: next issue is unavailable", ports.ErrInvalidTransition)
		}
		next.State = domain.IssueInProgress
		l.issues[next.ID] = next
	}
	current.State = domain.IssueCompleted
	l.issues[current.ID] = current
	l.latestFence[request.RunID] = request.Fence
	l.applied[request.IdempotencyKey] = request
	return nil
}

func sameAdvance(a, b domain.AdvanceRequest) bool {
	return a.RunID == b.RunID && a.ProjectID == b.ProjectID && a.CurrentIssueID == b.CurrentIssueID &&
		a.NextIssueID == b.NextIssueID && a.Evidence == b.Evidence && a.ReviewID == b.ReviewID
}
