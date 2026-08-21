package ports

import "errors"

var (
	ErrAlreadyExists     = errors.New("already exists")
	ErrNotFound          = errors.New("not found")
	ErrConflict          = errors.New("state conflict")
	ErrBusy              = errors.New("durable store is busy")
	ErrInvalidTransition = errors.New("invalid state transition")
	ErrStaleFence        = errors.New("stale fencing token")
)
