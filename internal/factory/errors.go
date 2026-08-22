package factory

import "errors"

var (
	ErrLeaseHeld       = errors.New("lease is already held")
	ErrLeaseExpired    = errors.New("lease expired")
	ErrInvalidEvidence = errors.New("evidence must name a concrete artifact, test, commit, or observed result")
	ErrBudgetExhausted = errors.New("run budget exhausted")
	ErrReviewRequired  = errors.New("immutable approved review evidence is required")
	ErrReviewMismatch  = errors.New("review evidence does not match the current issue and artifact")
	ErrDispatchPending = errors.New("reserved OpenClaw dispatch requires reconciliation or manual resolution")
)
