package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

func (s *Store) EnsureIssue(ctx context.Context, issue domain.Issue) error {
	if err := validateIssue(issue); err != nil {
		return err
	}
	blocked := 0
	if issue.Blocked {
		blocked = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO issues(id, project_id, title, priority, created_at, state, blocked)
		VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`, issue.ID, issue.ProjectID, issue.Title,
		issue.Priority, issue.CreatedAt.UTC().Format(time.RFC3339Nano), issue.State, blocked)
	if err != nil {
		return classify(err)
	}
	stored, err := s.GetIssue(ctx, issue.ID)
	if err != nil {
		return err
	}
	if !sameIssue(stored, issue) {
		return fmt.Errorf("%w: issue %q differs from durable state", ports.ErrConflict, issue.ID)
	}
	return nil
}

func (s *Store) GetIssue(ctx context.Context, id string) (domain.Issue, error) {
	var issue domain.Issue
	var created string
	var blocked int
	err := s.db.QueryRowContext(ctx, `SELECT id, project_id, title, priority, created_at, state, blocked FROM issues WHERE id = ?`, id).Scan(
		&issue.ID, &issue.ProjectID, &issue.Title, &issue.Priority, &created, &issue.State, &blocked)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Issue{}, ports.ErrNotFound
	}
	if err != nil {
		return domain.Issue{}, classify(err)
	}
	issue.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return domain.Issue{}, fmt.Errorf("%w: issue %q timestamp: %v", ErrInvalidRecord, id, err)
	}
	issue.Blocked = blocked == 1
	if err := validateIssue(issue); err != nil {
		return domain.Issue{}, err
	}
	return issue, nil
}

func (s *Store) ListProjectIssues(ctx context.Context, projectID string) ([]domain.Issue, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, project_id, title, priority, created_at, state, blocked FROM issues WHERE project_id = ?`, projectID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var issues []domain.Issue
	for rows.Next() {
		var issue domain.Issue
		var created string
		var blocked int
		if err := rows.Scan(&issue.ID, &issue.ProjectID, &issue.Title, &issue.Priority, &created, &issue.State, &blocked); err != nil {
			return nil, classify(err)
		}
		issue.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return nil, fmt.Errorf("%w: issue %q timestamp: %v", ErrInvalidRecord, issue.ID, err)
		}
		issue.Blocked = blocked == 1
		if err := validateIssue(issue); err != nil {
			return nil, err
		}
		issues = append(issues, issue)
	}
	if err := rows.Err(); err != nil {
		return nil, classify(err)
	}
	sort.Slice(issues, func(i, j int) bool { return issues[i].ID < issues[j].ID })
	return issues, nil
}

func (s *Store) Advance(ctx context.Context, request domain.AdvanceRequest) error {
	if err := validateAdvance(request); err != nil {
		return err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("encode advancement: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return classify(err)
	}
	defer tx.Rollback()
	var storedPayload []byte
	err = tx.QueryRowContext(ctx, `SELECT request FROM advance_receipts WHERE idempotency_key = ?`, request.IdempotencyKey).Scan(&storedPayload)
	if err == nil {
		var stored domain.AdvanceRequest
		if err := decodeStrict(storedPayload, &stored); err != nil {
			return fmt.Errorf("%w: advancement receipt JSON: %v", ErrInvalidRecord, err)
		}
		if !sameAdvance(stored, request) {
			return fmt.Errorf("%w: idempotency key reused for different advancement", ports.ErrConflict)
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return classify(err)
	}
	var runVersion uint64
	var runPayload []byte
	if err := tx.QueryRowContext(ctx, `SELECT version, payload FROM runs WHERE id = ?`, request.RunID).Scan(&runVersion, &runPayload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ports.ErrNotFound
		}
		return classify(err)
	}
	run, err := decodeRun(request.RunID, runVersion, runPayload)
	if err != nil {
		return err
	}
	if run.Lease.Fence != request.Fence {
		return ports.ErrStaleFence
	}
	if run.Status != domain.RunActive || run.ProjectID != request.ProjectID || !matchesPending(run.PendingAdvance, request) {
		return fmt.Errorf("%w: advancement is not the frozen controller operation", ports.ErrInvalidTransition)
	}
	current, err := getIssueTx(ctx, tx, request.CurrentIssueID)
	if err != nil {
		return err
	}
	if current.ProjectID != request.ProjectID || current.State == domain.IssueCompleted {
		return ports.ErrInvalidTransition
	}
	if request.NextIssueID != "" {
		next, err := getIssueTx(ctx, tx, request.NextIssueID)
		if err != nil {
			return err
		}
		if next.ProjectID != request.ProjectID || next.State != domain.IssueReady || next.Blocked {
			return fmt.Errorf("%w: next issue is unavailable", ports.ErrInvalidTransition)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE issues SET state = ? WHERE id = ?`, domain.IssueInProgress, next.ID); err != nil {
			return classify(err)
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE issues SET state = ? WHERE id = ?`, domain.IssueCompleted, current.ID); err != nil {
		return classify(err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO advance_receipts(idempotency_key, request, committed_at) VALUES (?, ?, ?)`, request.IdempotencyKey, payload, nowText()); err != nil {
		return classify(err)
	}
	if err := appendJournal(ctx, tx, request.RunID, runVersion, "remote_advance_committed", payload); err != nil {
		return err
	}
	if err := s.committing("remote_advance_committed"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return classify(err)
	}
	return s.committed("remote_advance_committed")
}

func (s *Store) AdvanceReceiptCount(ctx context.Context, idempotencyKey string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM advance_receipts WHERE idempotency_key = ?`, idempotencyKey).Scan(&count)
	return count, classify(err)
}

func getIssueTx(ctx context.Context, tx *sql.Tx, id string) (domain.Issue, error) {
	var issue domain.Issue
	var created string
	var blocked int
	err := tx.QueryRowContext(ctx, `SELECT id, project_id, title, priority, created_at, state, blocked FROM issues WHERE id = ?`, id).Scan(
		&issue.ID, &issue.ProjectID, &issue.Title, &issue.Priority, &created, &issue.State, &blocked)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Issue{}, ports.ErrNotFound
	}
	if err != nil {
		return domain.Issue{}, classify(err)
	}
	issue.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
	if err != nil {
		return domain.Issue{}, fmt.Errorf("%w: issue timestamp: %v", ErrInvalidRecord, err)
	}
	issue.Blocked = blocked == 1
	return issue, validateIssue(issue)
}

func sameAdvance(a, b domain.AdvanceRequest) bool {
	return a.RunID == b.RunID && a.ProjectID == b.ProjectID && a.CurrentIssueID == b.CurrentIssueID &&
		a.NextIssueID == b.NextIssueID && a.Evidence == b.Evidence && a.ReviewID == b.ReviewID &&
		a.IdempotencyKey == b.IdempotencyKey
}

func matchesPending(pending *domain.PendingAdvance, request domain.AdvanceRequest) bool {
	return pending != nil && pending.CurrentIssueID == request.CurrentIssueID && pending.NextIssueID == request.NextIssueID &&
		pending.Evidence == request.Evidence && pending.ReviewID == request.ReviewID && pending.IdempotencyKey == request.IdempotencyKey
}

func validateAdvance(request domain.AdvanceRequest) error {
	if request.RunID == "" || request.ProjectID == "" || request.CurrentIssueID == "" || request.Evidence == "" || request.ReviewID == "" || request.Fence == 0 || request.IdempotencyKey == "" || request.CurrentIssueID == request.NextIssueID {
		return fmt.Errorf("%w: incomplete advancement", ErrInvalidRecord)
	}
	return nil
}

func validateIssue(issue domain.Issue) error {
	if issue.ID == "" || issue.ProjectID == "" || issue.CreatedAt.IsZero() || (issue.State != domain.IssueReady && issue.State != domain.IssueInProgress && issue.State != domain.IssueCompleted) {
		return fmt.Errorf("%w: incomplete issue %q", ErrInvalidRecord, issue.ID)
	}
	return nil
}

func sameIssue(a, b domain.Issue) bool {
	return a.ID == b.ID && a.ProjectID == b.ProjectID && a.Title == b.Title && a.Priority == b.Priority &&
		a.CreatedAt.Equal(b.CreatedAt) && a.State == b.State && a.Blocked == b.Blocked
}
