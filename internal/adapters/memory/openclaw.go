package memory

import (
	"context"
	"errors"
	"sync"

	"github.com/ControlStackAI/dark-factory/internal/domain"
)

type Turn struct {
	Result domain.TurnResult
	Err    error
}

type OpenClaw struct {
	mu       sync.Mutex
	turns    []Turn
	requests []domain.TurnRequest
}

func NewOpenClaw(turns ...Turn) *OpenClaw {
	return &OpenClaw{turns: append([]Turn(nil), turns...)}
}

func (o *OpenClaw) ExecuteTurn(_ context.Context, request domain.TurnRequest) (domain.TurnResult, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.requests = append(o.requests, request)
	if len(o.turns) == 0 {
		return domain.TurnResult{}, errors.New("no scripted OpenClaw turn remains")
	}
	turn := o.turns[0]
	o.turns = o.turns[1:]
	return turn.Result, turn.Err
}

func (o *OpenClaw) Requests() []domain.TurnRequest {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]domain.TurnRequest(nil), o.requests...)
}
