package app

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	linearadapter "github.com/ControlStackAI/dark-factory/internal/adapters/linear"
	openclawadapter "github.com/ControlStackAI/dark-factory/internal/adapters/openclaw"
	durablesqlite "github.com/ControlStackAI/dark-factory/internal/adapters/sqlite"
	"github.com/ControlStackAI/dark-factory/internal/config"
	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/factory"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

type SupervisorStore interface {
	ports.RunStore
	ports.Reviews
	ports.Artifacts
	Close() error
}

type ClaimFunc func(context.Context, string, string, string) error
type SleepFunc func(context.Context, time.Duration) error

type SupervisorOptions struct {
	Store          SupervisorStore
	Linear         ports.Linear
	OpenClaw       ports.OpenClaw
	Claim          ClaimFunc
	RunID          string
	ProjectID      string
	IssueID        string
	Holder         string
	Policy         domain.Policy
	PollInterval   time.Duration
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Sleep          SleepFunc
	Lock           *InstanceLock
	OnPhase        func(string, domain.Run)
	Clock          factory.Clock
	Now            func() time.Time
}

type Supervisor struct {
	options    SupervisorOptions
	controller *factory.Controller
}

func NewSupervisor(options SupervisorOptions) (*Supervisor, error) {
	if options.Store == nil || options.Linear == nil || options.OpenClaw == nil || options.Claim == nil || options.RunID == "" || options.ProjectID == "" || options.IssueID == "" || options.Holder == "" ||
		!options.Policy.Valid() || options.PollInterval <= 0 || options.InitialBackoff <= 0 || options.MaxBackoff < options.InitialBackoff {
		return nil, errors.New("invalid supervisor options")
	}
	if options.Sleep == nil {
		options.Sleep = sleepContext
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}
	controller := factory.New(options.Store, options.Linear, options.OpenClaw, options.Store, options.Store)
	if options.Clock != nil {
		controller = factory.NewWithClock(options.Store, options.Linear, options.OpenClaw, options.Store, options.Store, options.Clock)
	}
	return &Supervisor{options: options, controller: controller}, nil
}

func (s *Supervisor) Close() error {
	var result error
	if s.options.Store != nil {
		result = s.options.Store.Close()
	}
	if s.options.Lock != nil {
		if err := s.options.Lock.Close(); result == nil {
			result = err
		}
	}
	return result
}

// Run is a foreground loop. It returns only on cancellation, terminal state, or a
// fail-closed startup error; it never creates a background daemon process.
func (s *Supervisor) Run(ctx context.Context) (domain.Run, error) {
	run, err := s.controller.Get(ctx, s.options.RunID)
	if errors.Is(err, ports.ErrNotFound) {
		run, err = s.controller.Start(ctx, s.options.RunID, s.options.ProjectID, s.options.IssueID, "supervisor initialized", s.options.Policy)
		if err == nil {
			s.phase("run_created", run)
		}
	}
	if err != nil {
		return domain.Run{}, err
	}
	if run.ProjectID != s.options.ProjectID || run.Policy != s.options.Policy {
		return domain.Run{}, fmt.Errorf("%w: durable run identity or policy differs from configuration", ports.ErrConflict)
	}
	backoff := s.options.InitialBackoff
	turnBackoff := s.options.InitialBackoff
	for {
		if err := ctx.Err(); err != nil {
			return s.currentWithoutCanceledContext(), nil
		}
		run, err = s.controller.Get(ctx, s.options.RunID)
		if err != nil {
			return domain.Run{}, err
		}
		if run.Status != domain.RunActive {
			s.phase("terminal", run)
			return run, nil
		}
		if run.PendingDispatch != nil {
			if err := s.controller.BlockAmbiguousDispatch(ctx, run.ID); err != nil {
				return domain.Run{}, err
			}
			run, _ = s.controller.Get(ctx, run.ID)
			s.phase("ambiguous_dispatch_blocked", run)
			return run, nil
		}
		fence, wait, err := s.ensureLease(ctx, run)
		if err != nil {
			if sleepErr := s.options.Sleep(ctx, backoff); sleepErr != nil {
				return s.currentWithoutCanceledContext(), nil
			}
			backoff = nextBackoff(backoff, s.options.MaxBackoff)
			continue
		}
		if wait > 0 {
			if sleepErr := s.options.Sleep(ctx, wait); sleepErr != nil {
				return s.currentWithoutCanceledContext(), nil
			}
			continue
		}
		run, err = s.controller.Get(ctx, run.ID)
		if err != nil {
			return domain.Run{}, err
		}

		// Reconcile frozen remote work before claim, dispatch, receipt polling, or selection.
		if run.PendingAdvance != nil {
			s.phase("reconcile_before_dispatch", run)
			if _, err := s.controller.CompleteAndAdvance(ctx, run.ID, fence, "frozen advancement evidence is authoritative"); err != nil {
				if sleepErr := s.options.Sleep(ctx, backoff); sleepErr != nil {
					return s.currentWithoutCanceledContext(), nil
				}
				backoff = nextBackoff(backoff, s.options.MaxBackoff)
				continue
			}
			continue
		}

		if run.CheckpointSequence == 0 {
			claimKey := stableKey("claim", run.ID, run.IssueID)
			if err := s.options.Claim(ctx, run.ID, run.IssueID, claimKey); err != nil {
				if sleepErr := s.options.Sleep(ctx, backoff); sleepErr != nil {
					return s.currentWithoutCanceledContext(), nil
				}
				backoff = nextBackoff(backoff, s.options.MaxBackoff)
				continue
			}
			backoff = s.options.InitialBackoff
			s.phase("claim_reconciled", run)
			_, turnErr := s.controller.ExecuteTurn(ctx, run.ID, fence)
			run = s.currentWithoutCanceledContext()
			s.phase("turn_finished", run)
			if ctx.Err() != nil {
				return run, nil
			}
			if turnErr != nil {
				if run.Status != domain.RunActive {
					return run, nil
				}
				if sleepErr := s.options.Sleep(ctx, turnBackoff); sleepErr != nil {
					return s.currentWithoutCanceledContext(), nil
				}
				turnBackoff = nextBackoff(turnBackoff, s.options.MaxBackoff)
				continue
			}
			turnBackoff = s.options.InitialBackoff
			backoff = s.options.InitialBackoff
			continue
		}

		reviewID := ReviewID(run)
		if run.Review == nil {
			err := s.controller.BindReview(ctx, run.ID, fence, reviewID)
			if errors.Is(err, ports.ErrNotFound) {
				s.phase("waiting_for_review", run)
				if sleepErr := s.options.Sleep(ctx, s.options.PollInterval); sleepErr != nil {
					return s.currentWithoutCanceledContext(), nil
				}
				continue
			}
			if err != nil {
				if sleepErr := s.options.Sleep(ctx, backoff); sleepErr != nil {
					return s.currentWithoutCanceledContext(), nil
				}
				backoff = nextBackoff(backoff, s.options.MaxBackoff)
				continue
			}
			backoff = s.options.InitialBackoff
			continue
		}
		if run.LastTurnArtifact == nil {
			return run, fmt.Errorf("%w: checkpoint has no durable OpenClaw response artifact", ports.ErrConflict)
		}
		if run.Review.ArtifactSHA256 != run.LastTurnArtifact.ResponseSHA256 {
			return run, fmt.Errorf("%w: review is not bound to the last OpenClaw response artifact", ports.ErrConflict)
		}
		evidence := fmt.Sprintf("review %s approved response artifact %s", run.Review.ReviewID, run.LastTurnArtifact.ResponseSHA256)
		if _, err := s.controller.CompleteAndAdvance(ctx, run.ID, fence, evidence); err != nil {
			if sleepErr := s.options.Sleep(ctx, backoff); sleepErr != nil {
				return s.currentWithoutCanceledContext(), nil
			}
			backoff = nextBackoff(backoff, s.options.MaxBackoff)
			continue
		}
	}
}

func (s *Supervisor) ensureLease(ctx context.Context, run domain.Run) (uint64, time.Duration, error) {
	now := s.options.Now()
	if run.Lease.Fence != 0 && now.Before(run.Lease.ExpiresAt) {
		if run.Lease.Holder == s.options.Holder {
			return run.Lease.Fence, 0, nil
		}
		wait := run.Lease.ExpiresAt.Sub(now)
		if wait > s.options.MaxBackoff {
			wait = s.options.MaxBackoff
		}
		return 0, wait, nil
	}
	lease, err := s.controller.AcquireLease(ctx, run.ID, s.options.Holder)
	if err != nil {
		return 0, 0, err
	}
	run, _ = s.controller.Get(ctx, run.ID)
	s.phase("lease_acquired", run)
	return lease.Fence, 0, nil
}

func (s *Supervisor) phase(name string, run domain.Run) {
	if s.options.OnPhase != nil {
		s.options.OnPhase(name, run)
	}
}

func (s *Supervisor) currentWithoutCanceledContext() domain.Run {
	run, _ := s.controller.Get(context.Background(), s.options.RunID)
	return run
}

func ReviewID(run domain.Run) string {
	return fmt.Sprintf("review:%s:%s:%d", run.ID, run.IssueID, run.CheckpointSequence)
}

func StableRunID(cfg config.Config) string {
	return "run-" + stableKey(cfg.Scope.ProjectID, cfg.Scope.IssueID)[:32]
}

func stableKey(parts ...string) string {
	hash := sha256.New()
	for _, part := range parts {
		_, _ = hash.Write([]byte(part))
		_, _ = hash.Write([]byte{0})
	}
	return fmt.Sprintf("%x", hash.Sum(nil))
}

func nextBackoff(current, maximum time.Duration) time.Duration {
	if current >= maximum/2 {
		return maximum
	}
	return current * 2
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func NewProductionSupervisor(cfg config.Config) (*Supervisor, error) {
	if cfg.Mode != "live" {
		return nil, errors.New("production supervisor requires config mode live")
	}
	if cfg.OpenClaw.Model != "" {
		return nil, errors.New("M3 production supervisor requires the exact OpenClaw argv contract; model overrides are unsupported")
	}
	apiKey, err := config.ResolveSecret(cfg.Linear.APIKey)
	if err != nil {
		return nil, err
	}
	if err := ensurePrivateDatabase(cfg.Paths.StateDB); err != nil {
		return nil, err
	}
	lock, err := AcquireInstanceLock(filepath.Join(cfg.Paths.StateRoot, "factoryd.lock"))
	if err != nil {
		return nil, err
	}
	store, err := durablesqlite.Open(cfg.Paths.StateDB)
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	linearClient, err := linearadapter.New(linearadapter.Options{
		Endpoint: cfg.Linear.Endpoint, APIKey: apiKey, TeamID: cfg.Scope.TeamID, ProjectID: cfg.Scope.ProjectID,
		IssueAllowlist: cfg.Scope.IssueAllowlist, ReadyName: cfg.Lifecycle.Ready, InProgressName: cfg.Lifecycle.InProgress, DoneName: cfg.Lifecycle.Done,
	})
	if err != nil {
		_ = store.Close()
		_ = lock.Close()
		return nil, err
	}
	openclawTimeout, _ := time.ParseDuration(cfg.OpenClaw.Timeout)
	shutdownTimeout, _ := time.ParseDuration(cfg.Budgets.ShutdownTimeout)
	leaseDuration, _ := time.ParseDuration(cfg.Budgets.LeaseDuration)
	safetyMargin := leaseDuration / 10
	if safetyMargin > time.Second {
		safetyMargin = time.Second
	}
	if safetyMargin < time.Millisecond {
		safetyMargin = time.Millisecond
	}
	safeTimeout := leaseDuration - shutdownTimeout - safetyMargin
	if safeTimeout <= 0 {
		_ = store.Close()
		_ = lock.Close()
		return nil, errors.New("lease duration must exceed the OpenClaw shutdown allowance")
	}
	if openclawTimeout > safeTimeout {
		openclawTimeout = safeTimeout
	}
	executor, err := openclawadapter.New(openclawadapter.Options{
		Executable: cfg.OpenClaw.Executable, Agent: cfg.OpenClaw.Agent, SessionPrefix: cfg.OpenClaw.SessionPrefix,
		Timeout: openclawTimeout, ShutdownTimeout: shutdownTimeout, PromptRoot: cfg.Paths.StateRoot,
		ArtifactRoot: cfg.Paths.ArtifactRoot, MaxOutputBytes: cfg.Limits.MaxOutputBytes, MaxArtifactBytes: cfg.Limits.MaxArtifactBytes, MaxArtifacts: cfg.Limits.MaxArtifacts,
		StripEnvironment: []string{strings.TrimPrefix(cfg.Linear.APIKey, "env:")},
	})
	if err != nil {
		_ = store.Close()
		_ = lock.Close()
		return nil, err
	}
	maxRunDuration, _ := time.ParseDuration(cfg.Budgets.MaxRunDuration)
	pollInterval, _ := time.ParseDuration(cfg.Budgets.PollInterval)
	initialBackoff, _ := time.ParseDuration(cfg.Budgets.InitialBackoff)
	maxBackoff, _ := time.ParseDuration(cfg.Budgets.MaxBackoff)
	var holderNonce [8]byte
	if _, err := rand.Read(holderNonce[:]); err != nil {
		_ = store.Close()
		_ = lock.Close()
		return nil, err
	}
	holder := fmt.Sprintf("factoryd:%d:%x", os.Getpid(), holderNonce)
	return NewSupervisor(SupervisorOptions{
		Store: store, Linear: linearClient, OpenClaw: executor,
		Claim: func(ctx context.Context, runID, issueID, key string) error {
			return linearClient.Claim(ctx, linearadapter.ClaimRequest{RunID: runID, IssueID: issueID, IdempotencyKey: key})
		},
		RunID: StableRunID(cfg), ProjectID: cfg.Scope.ProjectID, IssueID: cfg.Scope.IssueID, Holder: holder,
		Policy:       domain.Policy{LeaseDuration: leaseDuration, MaxRunDuration: maxRunDuration, MaxAttempts: cfg.Budgets.MaxAttempts, MaxConsecutiveFailures: cfg.Budgets.MaxConsecutiveFailures},
		PollInterval: pollInterval, InitialBackoff: initialBackoff, MaxBackoff: maxBackoff, Lock: lock,
	})
}

func ensurePrivateDatabase(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		file, createErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if createErr != nil {
			return createErr
		}
		return file.Close()
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("state database is not a private regular file")
	}
	return nil
}
