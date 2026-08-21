# Dark Factory

Dark Factory is an open-source toolkit for running issue-backed autonomous agent
workflows through a portable, fail-closed controller. The initial product target is
Linear plus OpenClaw, with the controller—not prompts—enforcing safety and durability.

## Status

This repository contains the first credential-free controller vertical slice. It is not
yet a production daemon: persistence and live Linear/OpenClaw adapters remain future
milestones. The current code executes the core behavior against in-memory adapters.

Implemented controller invariants include:

- expiring leases with monotonically increasing fencing tokens;
- rejection of stale, expired, and non-monotonic mutations;
- evidence-bearing checkpoints as the only lease-renewal path;
- bounded agent attempts, consecutive failures, and wall-clock duration;
- deterministic advancement by priority, creation time, then issue ID;
- a frozen pending advancement and adapter idempotency key for safe retry;
- completion gated on approved, immutable, hash-bound, single-consumer review evidence.

See [docs/architecture.md](docs/architecture.md) for the boundaries and transition model.

## Quick Start

Requires Go 1.24 or newer.

```console
make check
go run ./cmd/factoryctl dry-run
go run ./cmd/factoryd --once
```

Both executable examples run the same local fixture: start a run, acquire a lease,
execute one bounded OpenClaw turn, bind immutable review evidence, complete the current
issue, and deterministically adopt the next issue. They make no network calls.

Available development commands:

```console
make fmt        # rewrite Go files with gofmt
make fmt-check  # fail if formatting differs
make test       # run unit tests
make test-race  # run unit tests with the race detector
make lint       # run go vet
make build      # compile all packages and commands
make smoke      # execute credential-free command smoke tests
make check      # run the local merge gate
```

## Repository Layout

```text
cmd/factoryd/              controller daemon entry point (bootstrap --once mode)
cmd/factoryctl/            operator CLI entry point
internal/domain/           state, issue, review, and deterministic selection model
internal/factory/          fail-closed controller transitions
internal/ports/            Linear, OpenClaw, state, review, and artifact contracts
internal/adapters/memory/  credential-free adapter implementations and test fakes
internal/app/              executable vertical-slice composition
```

## License

MIT
