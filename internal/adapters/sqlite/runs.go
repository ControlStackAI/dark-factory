package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

type JournalEntry struct {
	Sequence   int64
	RunID      string
	RunVersion uint64
	Phase      string
	CreatedAt  time.Time
}

func (s *Store) Create(ctx context.Context, run domain.Run) error {
	if err := validateRun(run); err != nil {
		return err
	}
	payload, err := json.Marshal(run)
	if err != nil {
		return fmt.Errorf("encode run: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return classify(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `INSERT INTO runs(id, version, payload) VALUES (?, ?, ?)`, run.ID, run.Version, payload); err != nil {
		if isConstraint(err) {
			return ports.ErrAlreadyExists
		}
		return classify(err)
	}
	if err := appendJournal(ctx, tx, run.ID, run.Version, "run_created", payload); err != nil {
		return err
	}
	if err := s.committing("run_created"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return classify(err)
	}
	return s.committed("run_created")
}

func (s *Store) Get(ctx context.Context, id string) (domain.Run, error) {
	var version uint64
	var payload []byte
	if err := s.db.QueryRowContext(ctx, `SELECT version, payload FROM runs WHERE id = ?`, id).Scan(&version, &payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Run{}, ports.ErrNotFound
		}
		return domain.Run{}, classify(err)
	}
	return decodeRun(id, version, payload)
}

func (s *Store) CompareAndSwap(ctx context.Context, id string, expectedVersion uint64, next domain.Run) error {
	current, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	if current.Version != expectedVersion {
		return ports.ErrConflict
	}
	if next.ID != id || next.Version != expectedVersion+1 {
		return fmt.Errorf("%w: CAS version or run id changed", ErrInvalidRecord)
	}
	if current.ProjectID != next.ProjectID || current.StartedAt != next.StartedAt || current.DeadlineAt != next.DeadlineAt || current.Policy != next.Policy {
		return fmt.Errorf("%w: immutable run fields changed", ErrInvalidRecord)
	}
	if next.Attempts < current.Attempts || next.Attempts > current.Attempts+1 {
		return fmt.Errorf("%w: attempt counter changed from %d to %d", ErrInvalidRecord, current.Attempts, next.Attempts)
	}
	if err := validateRun(next); err != nil {
		return err
	}
	payload, err := json.Marshal(next)
	if err != nil {
		return fmt.Errorf("encode run: %w", err)
	}
	phases := transitionPhases(current, next)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return classify(err)
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE runs SET version = ?, payload = ? WHERE id = ? AND version = ?`, next.Version, payload, id, expectedVersion)
	if err != nil {
		return classify(err)
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return classify(err)
	}
	if changed != 1 {
		return ports.ErrConflict
	}
	if next.Attempts == current.Attempts+1 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO attempt_reservations(run_id, attempt, fence, reserved_at) VALUES (?, ?, ?, ?)`, id, next.Attempts, next.Lease.Fence, nowText()); err != nil {
			return classify(err)
		}
	}
	for _, phase := range phases {
		if err := appendJournal(ctx, tx, id, next.Version, phase, payload); err != nil {
			return err
		}
	}
	for _, phase := range phases {
		if err := s.committing(phase); err != nil {
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return classify(err)
	}
	for _, phase := range phases {
		if err := s.committed(phase); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Journal(ctx context.Context, runID string) ([]JournalEntry, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT sequence, run_id, run_version, phase, created_at FROM journal WHERE run_id = ? ORDER BY sequence`, runID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var entries []JournalEntry
	for rows.Next() {
		var entry JournalEntry
		var created string
		if err := rows.Scan(&entry.Sequence, &entry.RunID, &entry.RunVersion, &entry.Phase, &created); err != nil {
			return nil, classify(err)
		}
		entry.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
		if err != nil {
			return nil, fmt.Errorf("%w: journal timestamp: %v", ErrInvalidRecord, err)
		}
		entries = append(entries, entry)
	}
	return entries, classify(rows.Err())
}

func (s *Store) AttemptReservationCount(ctx context.Context, runID string) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM attempt_reservations WHERE run_id = ?`, runID).Scan(&count)
	return count, classify(err)
}

func appendJournal(ctx context.Context, tx *sql.Tx, runID string, version uint64, phase string, payload []byte) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO journal(run_id, run_version, phase, payload, created_at) VALUES (?, ?, ?, ?, ?)`, runID, version, phase, payload, nowText())
	return classify(err)
}

func transitionPhases(current, next domain.Run) []string {
	var phases []string
	if current.PendingDispatch != nil && next.PendingDispatch == nil {
		phases = append(phases, "dispatch_recorded")
	} else if current.PendingDispatch != nil && next.PendingDispatch != nil && current.PendingDispatch.State == domain.DispatchReserved && next.PendingDispatch.State == domain.DispatchStarted {
		phases = append(phases, "dispatch_started")
	}
	switch {
	case current.PendingAdvance != nil && next.PendingAdvance == nil:
		phases = append(phases, "advance_reconciled")
	case current.PendingAdvance == nil && next.PendingAdvance != nil:
		phases = append(phases, "advance_frozen")
	}
	if current.Status != next.Status {
		switch next.Status {
		case domain.RunBlocked:
			phases = append(phases, "run_blocked")
		case domain.RunComplete:
			phases = append(phases, "run_completed")
		}
	}
	if current.Review == nil && next.Review != nil {
		phases = append(phases, "review_bound")
	}
	if next.Attempts == current.Attempts+1 {
		phases = append(phases, "attempt_reserved")
	}
	if next.CheckpointSequence > current.CheckpointSequence {
		phases = append(phases, "checkpoint_committed")
	}
	if next.Lease.Fence > current.Lease.Fence {
		phases = append(phases, "lease_acquired")
	}
	if len(phases) == 0 {
		phases = append(phases, "state_updated")
	}
	return phases
}

func decodeRun(id string, version uint64, payload []byte) (domain.Run, error) {
	var run domain.Run
	if err := decodeStrict(payload, &run); err != nil {
		return domain.Run{}, fmt.Errorf("%w: run %q JSON: %v", ErrInvalidRecord, id, err)
	}
	if run.ID != id || run.Version != version {
		return domain.Run{}, fmt.Errorf("%w: run %q identity/version mismatch", ErrInvalidRecord, id)
	}
	if err := validateRun(run); err != nil {
		return domain.Run{}, err
	}
	return run, nil
}

func decodeStrict(payload []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func validateRun(run domain.Run) error {
	invalid := func(reason string) error { return fmt.Errorf("%w: run %q %s", ErrInvalidRecord, run.ID, reason) }
	if strings.TrimSpace(run.ID) == "" || strings.TrimSpace(run.ProjectID) == "" || strings.TrimSpace(run.IssueID) == "" || strings.TrimSpace(run.Step) == "" {
		return invalid("has incomplete identity or step")
	}
	if run.Status != domain.RunActive && run.Status != domain.RunBlocked && run.Status != domain.RunComplete {
		return invalid("has unknown status")
	}
	if !run.Policy.Valid() || run.StartedAt.IsZero() || !run.DeadlineAt.After(run.StartedAt) || run.Version == 0 {
		return invalid("has invalid policy, time bounds, or version")
	}
	if run.Status == domain.RunActive && !run.FinishedAt.IsZero() {
		return invalid("active run has finish time")
	}
	if run.Status != domain.RunActive && run.FinishedAt.IsZero() {
		return invalid("terminal run has no finish time")
	}
	if run.Attempts < 0 || run.Attempts > run.Policy.MaxAttempts || run.ConsecutiveFailures < 0 || run.ConsecutiveFailures > run.Policy.MaxConsecutiveFailures {
		return invalid("has invalid budget counters")
	}
	if run.NextFence < run.Lease.Fence {
		return invalid("has regressed fence counter")
	}
	if run.Lease.Fence == 0 {
		if run.Lease.Holder != "" || !run.Lease.ExpiresAt.IsZero() {
			return invalid("has incomplete zero fence lease")
		}
	} else if strings.TrimSpace(run.Lease.Holder) == "" || run.Lease.ExpiresAt.IsZero() {
		return invalid("has incomplete lease")
	}
	for _, evidence := range run.Evidence {
		if strings.TrimSpace(evidence) == "" {
			return invalid("has empty evidence")
		}
	}
	if run.Review != nil {
		if run.Review.ReviewID == "" || run.Review.ArtifactRef == "" || !validDigest(run.Review.ArtifactSHA256) {
			return invalid("has incomplete review binding")
		}
	}
	if run.PendingDispatch != nil {
		dispatch := run.PendingDispatch
		if dispatch.Attempt != run.Attempts || dispatch.Attempt < 1 || dispatch.Fence == 0 || dispatch.Fence != run.Lease.Fence || dispatch.ReservedAt.IsZero() ||
			(dispatch.State != domain.DispatchReserved && dispatch.State != domain.DispatchStarted) ||
			(dispatch.State == domain.DispatchReserved && !dispatch.StartedAt.IsZero()) || (dispatch.State == domain.DispatchStarted && dispatch.StartedAt.IsZero()) {
			return invalid("has incomplete or inconsistent pending dispatch")
		}
	}
	if run.LastTurnArtifact != nil {
		artifact := run.LastTurnArtifact
		if artifact.Attempt < 1 || artifact.Attempt > run.Attempts || artifact.SessionKey == "" || artifact.ResponseRef == "" || !validDigest(artifact.ResponseSHA256) {
			return invalid("has incomplete turn artifact")
		}
	}
	if run.PendingAdvance != nil {
		pending := run.PendingAdvance
		if pending.CurrentIssueID != run.IssueID || pending.Evidence == "" || pending.ReviewID == "" || pending.IdempotencyKey == "" || run.Review == nil || pending.ReviewID != run.Review.ReviewID {
			return invalid("has incomplete or inconsistent frozen advancement")
		}
	}
	if run.Status != domain.RunActive && (run.PendingAdvance != nil || run.PendingDispatch != nil) {
		return invalid("terminal run has pending work")
	}
	return nil
}

func validDigest(digest string) bool {
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == 32 && strings.ToLower(digest) == digest
}

func isConstraint(err error) bool {
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "constraint") || strings.Contains(lower, "unique")
}
