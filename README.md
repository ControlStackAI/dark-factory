# Dark Factory

Dark Factory is an open-source toolkit for running issue-backed autonomous agent
workflows through a portable, fail-closed controller. The initial product target is
Linear plus OpenClaw, with the controller—not prompts—enforcing safety and durability.

## Status

This repository contains the M1 operator surface around the credential-free controller
vertical slice and restart-safe SQLite recovery kernel. It is not yet a production daemon:
live Linear arrives in M2 and the OpenClaw executor/continuous supervisor in M3. M1 proves
configuration, filesystem policy, offline diagnostics, and the live/apply interlock without
contacting either service.

Implemented controller invariants include:

- expiring leases with monotonically increasing fencing tokens;
- schema-v1 SQLite state with strict integrity/version checks and an append-only phase journal;
- rejection of stale, expired, and non-monotonic mutations;
- evidence-bearing checkpoints as the only lease-renewal path;
- bounded agent attempts, consecutive failures, and wall-clock duration;
- deterministic advancement by priority, creation time, then issue ID;
- a frozen pending advancement and adapter idempotency key for safe retry;
- durable attempt reservations, review consumption, and atomic advancement receipts;
- exactly-once local reference mutations after timeout/commit ambiguity;
- completion gated on approved, immutable, hash-bound, single-consumer review evidence.

See [docs/architecture.md](docs/architecture.md) for the boundaries and transition model.

## Quick Start

Requires Go 1.24 or newer.

```console
make check
config_dir=$(mktemp -d)
chmod 700 "$config_dir"
go run ./cmd/factoryctl init --config "$config_dir/factory.json"
LINEAR_API_KEY=local-placeholder go run ./cmd/factoryctl validate --config "$config_dir/factory.json" --json
go run ./cmd/factoryctl doctor --config "$config_dir/factory.json" --json
go run ./cmd/factoryctl dry-run --config "$config_dir/factory.json" --json
go run ./cmd/factoryctl status --config "$config_dir/factory.json" --json
go run ./cmd/factoryd --once --config "$config_dir/factory.json"
go run ./cmd/factoryd --once --state "$config_dir/recovery.db"
```

`init` creates the config as `0600` and its state, artifact, and review directories as `0700`.
It refuses every existing final path and uses an atomic no-replace install. `validate` is
strict and requires the referenced environment variable to exist; use a real secret only in
an eventual service environment, never in the config itself. See
[docs/config-reference.md](docs/config-reference.md) for every M1 field and path rule.

The dry-run executables run the same in-memory fixture: start a run, acquire a lease,
execute one bounded OpenClaw turn, bind immutable review evidence, complete the current
issue, and deterministically adopt the next issue. This is a fake adapter, not an OpenClaw
process call. Both `factoryctl recover STATE_DB` and the published M0-compatible
`factoryd --once --state STATE_DB` entry point retain the restart-safe one-issue SQLite fixture;
`--state` is mutually exclusive with configured or applied execution. `validate`, `doctor`,
`status`, `dry-run`, and dry-mode `factoryd --once` make no network or executor calls; the M1
dry run does not create durable live-run state.

Live mode is deliberately unusable at M1. Both keys are required before the live branch is
even selected:

```console
# mode: live without --apply: refused
go run ./cmd/factoryd --once --config "$config_dir/factory.json"

# mode: live plus --apply: interlock proven, then fails closed
go run ./cmd/factoryd --once --apply --config "$config_dir/factory.json"
# live execution is not implemented until M2/M3; no external action was taken
```

`doctor --json` reports config, state DB, Linear, OpenClaw, review root, artifact root, and
service environment separately. Linear and OpenClaw cannot be `ready` while their production
adapters are absent; this is expected degraded/not-ready M1 behavior.

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
cmd/factoryd/              shared-composition --once entry point and live interlock
cmd/factoryctl/            init/validate/doctor/dry-run/status operator CLI
internal/config/           strict config decoding, path policy, and no-replace init
internal/domain/           state, issue, review, and deterministic selection model
internal/factory/          fail-closed controller transitions
internal/ports/            Linear, OpenClaw, state, review, and artifact contracts
internal/adapters/memory/  credential-free adapter implementations and test fakes
internal/adapters/sqlite/  schema-v1 durable store and local recovery adapter
internal/app/              executable vertical-slice composition
```

## License

MIT
