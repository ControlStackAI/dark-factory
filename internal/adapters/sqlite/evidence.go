package sqlite

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

func (s *Store) EnsureArtifact(ctx context.Context, ref string, contents []byte) (string, error) {
	digest := fmt.Sprintf("%x", sha256.Sum256(contents))
	if ref == "" {
		return "", fmt.Errorf("%w: empty artifact reference", ErrInvalidRecord)
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO artifacts(ref, contents, sha256) VALUES (?, ?, ?)
		ON CONFLICT(ref) DO NOTHING`, ref, contents, digest)
	if err != nil {
		return "", classify(err)
	}
	var storedContents []byte
	var storedDigest string
	if err := s.db.QueryRowContext(ctx, `SELECT contents, sha256 FROM artifacts WHERE ref = ?`, ref).Scan(&storedContents, &storedDigest); err != nil {
		return "", classify(err)
	}
	if storedDigest != digest || fmt.Sprintf("%x", sha256.Sum256(storedContents)) != digest {
		return "", fmt.Errorf("%w: immutable artifact %q changed", ports.ErrConflict, ref)
	}
	return digest, nil
}

func (s *Store) SHA256(ctx context.Context, ref string) (string, error) {
	var contents []byte
	var storedDigest string
	if err := s.db.QueryRowContext(ctx, `SELECT contents, sha256 FROM artifacts WHERE ref = ?`, ref).Scan(&contents, &storedDigest); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ports.ErrNotFound
		}
		return "", classify(err)
	}
	actual := fmt.Sprintf("%x", sha256.Sum256(contents))
	if !validDigest(storedDigest) || actual != storedDigest {
		return "", fmt.Errorf("%w: artifact %q digest mismatch", ErrInvalidRecord, ref)
	}
	return actual, nil
}

func (s *Store) EnsureReview(ctx context.Context, review domain.ReviewEvidence) error {
	if err := validateReviewRecord(review); err != nil {
		return err
	}
	if review.ConsumedByRun != "" {
		return fmt.Errorf("%w: new review %q is already consumed", ErrInvalidRecord, review.ID)
	}
	immutable := 0
	if review.Immutable {
		immutable = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO reviews(
		id, project_id, issue_id, status, immutable, artifact_ref, artifact_sha256, consumed_by_run
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(id) DO NOTHING`, review.ID, review.ProjectID,
		review.IssueID, review.Status, immutable, review.ArtifactRef, review.ArtifactSHA256, review.ConsumedByRun)
	if err != nil {
		return classify(err)
	}
	stored, err := s.GetReview(ctx, review.ID)
	if err != nil {
		return err
	}
	if stored != review {
		return fmt.Errorf("%w: review %q is immutable", ports.ErrConflict, review.ID)
	}
	return nil
}

func (s *Store) GetReview(ctx context.Context, id string) (domain.ReviewEvidence, error) {
	var review domain.ReviewEvidence
	var immutable int
	err := s.db.QueryRowContext(ctx, `SELECT id, project_id, issue_id, status, immutable,
		artifact_ref, artifact_sha256, consumed_by_run FROM reviews WHERE id = ?`, id).Scan(
		&review.ID, &review.ProjectID, &review.IssueID, &review.Status, &immutable,
		&review.ArtifactRef, &review.ArtifactSHA256, &review.ConsumedByRun)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.ReviewEvidence{}, ports.ErrNotFound
	}
	if err != nil {
		return domain.ReviewEvidence{}, classify(err)
	}
	review.Immutable = immutable == 1
	if err := validateReviewRecord(review); err != nil {
		return domain.ReviewEvidence{}, err
	}
	return review, nil
}

func (s *Store) ConsumeReview(ctx context.Context, id, runID, artifactSHA256 string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return classify(err)
	}
	defer tx.Rollback()
	var storedDigest, consumedBy string
	if err := tx.QueryRowContext(ctx, `SELECT artifact_sha256, consumed_by_run FROM reviews WHERE id = ?`, id).Scan(&storedDigest, &consumedBy); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ports.ErrNotFound
		}
		return classify(err)
	}
	if storedDigest != artifactSHA256 {
		return ports.ErrConflict
	}
	if consumedBy != "" {
		if consumedBy == runID {
			return nil
		}
		return ports.ErrConflict
	}
	var runVersion uint64
	if err := tx.QueryRowContext(ctx, `SELECT version FROM runs WHERE id = ?`, runID).Scan(&runVersion); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ports.ErrNotFound
		}
		return classify(err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE reviews SET consumed_by_run = ? WHERE id = ? AND consumed_by_run = ''`, runID, id)
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
	payload, err := json.Marshal(map[string]string{"review_id": id, "artifact_sha256": artifactSHA256})
	if err != nil {
		return fmt.Errorf("encode review consumption: %w", err)
	}
	if err := appendJournal(ctx, tx, runID, runVersion, "review_consumed", payload); err != nil {
		return err
	}
	if err := s.committing("review_consumed"); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return classify(err)
	}
	return s.committed("review_consumed")
}

func validateReviewRecord(review domain.ReviewEvidence) error {
	if review.ID == "" || review.ProjectID == "" || review.IssueID == "" || review.Status != domain.ReviewApproved || review.ArtifactRef == "" || !validDigest(review.ArtifactSHA256) {
		return fmt.Errorf("%w: incomplete review %q", ErrInvalidRecord, review.ID)
	}
	return nil
}
