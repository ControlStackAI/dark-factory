package memory

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sync"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

type Reviews struct {
	mu      sync.RWMutex
	reviews map[string]domain.ReviewEvidence
}

func NewReviews(reviews ...domain.ReviewEvidence) *Reviews {
	store := &Reviews{reviews: make(map[string]domain.ReviewEvidence, len(reviews))}
	for _, review := range reviews {
		store.reviews[review.ID] = review
	}
	return store
}

func (r *Reviews) GetReview(_ context.Context, id string) (domain.ReviewEvidence, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	review, exists := r.reviews[id]
	if !exists {
		return domain.ReviewEvidence{}, ports.ErrNotFound
	}
	return review, nil
}

func (r *Reviews) ConsumeReview(_ context.Context, id, runID, artifactSHA256 string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	review, exists := r.reviews[id]
	if !exists {
		return ports.ErrNotFound
	}
	if review.ArtifactSHA256 != artifactSHA256 {
		return ports.ErrConflict
	}
	if review.ConsumedByRun != "" && review.ConsumedByRun != runID {
		return ports.ErrConflict
	}
	review.ConsumedByRun = runID
	r.reviews[id] = review
	return nil
}

func (r *Reviews) Put(review domain.ReviewEvidence) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reviews[review.ID] = review
}

type Artifacts struct {
	mu        sync.RWMutex
	artifacts map[string][]byte
}

func NewArtifacts() *Artifacts {
	return &Artifacts{artifacts: make(map[string][]byte)}
}

func (a *Artifacts) Put(ref string, contents []byte) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.artifacts[ref] = append([]byte(nil), contents...)
	return fmt.Sprintf("%x", sha256.Sum256(contents))
}

func (a *Artifacts) SHA256(_ context.Context, ref string) (string, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	contents, exists := a.artifacts[ref]
	if !exists {
		return "", ports.ErrNotFound
	}
	return fmt.Sprintf("%x", sha256.Sum256(contents)), nil
}
