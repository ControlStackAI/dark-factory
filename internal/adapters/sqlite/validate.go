package sqlite

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/domain"
)

func (s *Store) validateRecords(ctx context.Context) error {
	runs := make(map[string]domain.Run)
	rows, err := s.db.QueryContext(ctx, `SELECT id, version, payload FROM runs`)
	if err != nil {
		return fmt.Errorf("validate runs: %w", classify(err))
	}
	for rows.Next() {
		var id string
		var version uint64
		var payload []byte
		if err := rows.Scan(&id, &version, &payload); err != nil {
			rows.Close()
			return fmt.Errorf("validate runs: %w", classify(err))
		}
		run, err := decodeRun(id, version, payload)
		if err != nil {
			rows.Close()
			return err
		}
		runs[id] = run
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("validate runs: %w", classify(err))
	}
	rows.Close()

	rows, err = s.db.QueryContext(ctx, `SELECT run_id, attempt, fence, reserved_at FROM attempt_reservations`)
	if err != nil {
		return fmt.Errorf("validate attempt reservations: %w", classify(err))
	}
	reservationCounts := make(map[string]int)
	for rows.Next() {
		var runID, reservedAt string
		var attempt int
		var fence uint64
		if err := rows.Scan(&runID, &attempt, &fence, &reservedAt); err != nil {
			rows.Close()
			return fmt.Errorf("validate attempt reservation: %w", classify(err))
		}
		run, ok := runs[runID]
		if _, err := time.Parse(time.RFC3339Nano, reservedAt); !ok || err != nil || attempt < 1 || attempt > run.Attempts || fence == 0 || fence > run.NextFence {
			rows.Close()
			return fmt.Errorf("%w: invalid attempt reservation for run %q", ErrInvalidRecord, runID)
		}
		reservationCounts[runID]++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("validate attempt reservations: %w", classify(err))
	}
	rows.Close()
	for id, run := range runs {
		if reservationCounts[id] != run.Attempts {
			return fmt.Errorf("%w: run %q has %d attempts but %d reservations", ErrInvalidRecord, id, run.Attempts, reservationCounts[id])
		}
	}

	allowedPhases := map[string]bool{
		"run_created": true, "lease_acquired": true, "attempt_reserved": true,
		"checkpoint_committed": true, "review_bound": true, "review_consumed": true,
		"advance_frozen": true, "remote_advance_committed": true, "advance_reconciled": true,
		"run_blocked": true, "run_completed": true, "state_updated": true,
	}
	journalCounts := make(map[string]int)
	latestJournalVersion := make(map[string]uint64)
	firstJournalPhase := make(map[string]string)
	attemptJournalCounts := make(map[string]int)
	remoteJournalKeys := make(map[string]bool)
	rows, err = s.db.QueryContext(ctx, `SELECT run_id, run_version, phase, payload, created_at FROM journal ORDER BY sequence`)
	if err != nil {
		return fmt.Errorf("validate journal: %w", classify(err))
	}
	for rows.Next() {
		var runID, phase, createdAt string
		var version uint64
		var payload []byte
		if err := rows.Scan(&runID, &version, &phase, &payload, &createdAt); err != nil {
			rows.Close()
			return fmt.Errorf("validate journal: %w", classify(err))
		}
		run, ok := runs[runID]
		if _, err := time.Parse(time.RFC3339Nano, createdAt); !ok || err != nil || version == 0 || version > run.Version || !allowedPhases[phase] || !json.Valid(payload) {
			rows.Close()
			return fmt.Errorf("%w: invalid journal entry for run %q", ErrInvalidRecord, runID)
		}
		journalCounts[runID]++
		latestJournalVersion[runID] = version
		if firstJournalPhase[runID] == "" {
			firstJournalPhase[runID] = phase
		}
		if phase == "attempt_reserved" {
			attemptJournalCounts[runID]++
		}
		if phase == "remote_advance_committed" {
			var request domain.AdvanceRequest
			if err := decodeStrict(payload, &request); err != nil || request.RunID != runID {
				rows.Close()
				return fmt.Errorf("%w: invalid remote advancement journal for run %q", ErrInvalidRecord, runID)
			}
			remoteJournalKeys[request.IdempotencyKey] = true
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("validate journal: %w", classify(err))
	}
	rows.Close()
	for id, run := range runs {
		if journalCounts[id] == 0 || firstJournalPhase[id] != "run_created" || latestJournalVersion[id] != run.Version || attemptJournalCounts[id] != run.Attempts {
			return fmt.Errorf("%w: incomplete journal for run %q", ErrInvalidRecord, id)
		}
	}

	reviews := make(map[string]domain.ReviewEvidence)
	rows, err = s.db.QueryContext(ctx, `SELECT id, project_id, issue_id, status, immutable, artifact_ref, artifact_sha256, consumed_by_run FROM reviews`)
	if err != nil {
		return fmt.Errorf("validate reviews: %w", classify(err))
	}
	for rows.Next() {
		var review domain.ReviewEvidence
		var immutable int
		if err := rows.Scan(&review.ID, &review.ProjectID, &review.IssueID, &review.Status, &immutable, &review.ArtifactRef, &review.ArtifactSHA256, &review.ConsumedByRun); err != nil {
			rows.Close()
			return fmt.Errorf("validate review: %w", classify(err))
		}
		review.Immutable = immutable == 1
		if err := validateReviewRecord(review); err != nil {
			rows.Close()
			return err
		}
		if review.ConsumedByRun != "" {
			if _, ok := runs[review.ConsumedByRun]; !ok {
				rows.Close()
				return fmt.Errorf("%w: review %q has unknown consumer", ErrInvalidRecord, review.ID)
			}
		}
		reviews[review.ID] = review
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("validate reviews: %w", classify(err))
	}
	rows.Close()
	for id, run := range runs {
		if run.Review == nil {
			continue
		}
		review, ok := reviews[run.Review.ReviewID]
		if !ok || review.ProjectID != run.ProjectID || review.IssueID != run.IssueID ||
			review.ArtifactRef != run.Review.ArtifactRef || review.ArtifactSHA256 != run.Review.ArtifactSHA256 {
			return fmt.Errorf("%w: run %q review binding is missing or mismatched", ErrInvalidRecord, id)
		}
	}

	artifactDigests := make(map[string]string)
	rows, err = s.db.QueryContext(ctx, `SELECT ref, contents, sha256 FROM artifacts`)
	if err != nil {
		return fmt.Errorf("validate artifacts: %w", classify(err))
	}
	for rows.Next() {
		var ref, digest string
		var contents []byte
		if err := rows.Scan(&ref, &contents, &digest); err != nil {
			rows.Close()
			return fmt.Errorf("validate artifact: %w", classify(err))
		}
		if ref == "" || !validDigest(digest) || fmt.Sprintf("%x", sha256.Sum256(contents)) != digest {
			rows.Close()
			return fmt.Errorf("%w: artifact %q digest mismatch", ErrInvalidRecord, ref)
		}
		artifactDigests[ref] = digest
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("validate artifacts: %w", classify(err))
	}
	rows.Close()
	for id, review := range reviews {
		if artifactDigests[review.ArtifactRef] != review.ArtifactSHA256 {
			return fmt.Errorf("%w: review %q artifact is missing or changed", ErrInvalidRecord, id)
		}
	}

	issues := make(map[string]domain.Issue)
	rows, err = s.db.QueryContext(ctx, `SELECT id, project_id, title, priority, created_at, state, blocked FROM issues`)
	if err != nil {
		return fmt.Errorf("validate issues: %w", classify(err))
	}
	for rows.Next() {
		var issue domain.Issue
		var createdAt string
		var blocked int
		if err := rows.Scan(&issue.ID, &issue.ProjectID, &issue.Title, &issue.Priority, &createdAt, &issue.State, &blocked); err != nil {
			rows.Close()
			return fmt.Errorf("validate issue: %w", classify(err))
		}
		issue.CreatedAt, err = time.Parse(time.RFC3339Nano, createdAt)
		issue.Blocked = blocked == 1
		if err != nil {
			rows.Close()
			return fmt.Errorf("%w: issue %q timestamp", ErrInvalidRecord, issue.ID)
		}
		if err := validateIssue(issue); err != nil {
			rows.Close()
			return err
		}
		issues[issue.ID] = issue
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("validate issues: %w", classify(err))
	}
	rows.Close()
	for id, run := range runs {
		issue, ok := issues[run.IssueID]
		if !ok || issue.ProjectID != run.ProjectID {
			return fmt.Errorf("%w: run %q current issue is missing or mismatched", ErrInvalidRecord, id)
		}
	}
	for id, review := range reviews {
		issue, ok := issues[review.IssueID]
		if !ok || issue.ProjectID != review.ProjectID {
			return fmt.Errorf("%w: review %q issue is missing or mismatched", ErrInvalidRecord, id)
		}
	}

	rows, err = s.db.QueryContext(ctx, `SELECT idempotency_key, request, committed_at FROM advance_receipts`)
	if err != nil {
		return fmt.Errorf("validate advancement receipts: %w", classify(err))
	}
	for rows.Next() {
		var key, committedAt string
		var payload []byte
		if err := rows.Scan(&key, &payload, &committedAt); err != nil {
			rows.Close()
			return fmt.Errorf("validate advancement receipt: %w", classify(err))
		}
		var request domain.AdvanceRequest
		if err := decodeStrict(payload, &request); err != nil {
			rows.Close()
			return fmt.Errorf("%w: advancement receipt JSON: %v", ErrInvalidRecord, err)
		}
		if _, err := time.Parse(time.RFC3339Nano, committedAt); err != nil || request.IdempotencyKey != key {
			rows.Close()
			return fmt.Errorf("%w: invalid advancement receipt %q", ErrInvalidRecord, key)
		}
		if err := validateAdvance(request); err != nil {
			rows.Close()
			return err
		}
		if _, ok := runs[request.RunID]; !ok {
			rows.Close()
			return fmt.Errorf("%w: receipt %q has unknown run", ErrInvalidRecord, key)
		}
		run := runs[request.RunID]
		current, currentOK := issues[request.CurrentIssueID]
		nextOK := request.NextIssueID == ""
		if request.NextIssueID != "" {
			_, nextOK = issues[request.NextIssueID]
		}
		if run.ProjectID != request.ProjectID || !currentOK || current.ProjectID != request.ProjectID || !nextOK || !remoteJournalKeys[key] {
			rows.Close()
			return fmt.Errorf("%w: receipt %q references incomplete state", ErrInvalidRecord, key)
		}
		delete(remoteJournalKeys, key)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("validate advancement receipts: %w", classify(err))
	}
	rows.Close()
	if len(remoteJournalKeys) != 0 {
		return fmt.Errorf("%w: remote advancement journal has no receipt", ErrInvalidRecord)
	}

	rows, err = s.db.QueryContext(ctx, `PRAGMA foreign_key_check`)
	if err != nil {
		return fmt.Errorf("validate foreign keys: %w", classify(err))
	}
	if rows.Next() {
		rows.Close()
		return fmt.Errorf("%w: foreign key violation", ErrInvalidRecord)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("validate foreign keys: %w", classify(err))
	}
	rows.Close()
	return nil
}
