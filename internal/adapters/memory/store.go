package memory

import (
	"context"
	"sync"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

type RunStore struct {
	mu   sync.RWMutex
	runs map[string]domain.Run
}

func NewRunStore() *RunStore {
	return &RunStore{runs: make(map[string]domain.Run)}
}

func (s *RunStore) Create(_ context.Context, run domain.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.runs[run.ID]; exists {
		return ports.ErrAlreadyExists
	}
	s.runs[run.ID] = cloneRun(run)
	return nil
}

func (s *RunStore) Get(_ context.Context, id string) (domain.Run, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run, exists := s.runs[id]
	if !exists {
		return domain.Run{}, ports.ErrNotFound
	}
	return cloneRun(run), nil
}

func (s *RunStore) CompareAndSwap(_ context.Context, id string, expectedVersion uint64, next domain.Run) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.runs[id]
	if !exists {
		return ports.ErrNotFound
	}
	if current.Version != expectedVersion {
		return ports.ErrConflict
	}
	s.runs[id] = cloneRun(next)
	return nil
}

func cloneRun(run domain.Run) domain.Run {
	run.Evidence = append([]string(nil), run.Evidence...)
	if run.Review != nil {
		review := *run.Review
		run.Review = &review
	}
	if run.PendingAdvance != nil {
		pending := *run.PendingAdvance
		run.PendingAdvance = &pending
	}
	return run
}
