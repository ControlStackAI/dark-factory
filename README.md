# Dark Factory

Dark Factory is an open-source toolkit for running issue-backed autonomous agent
workflows through a portable, fail-closed controller. The initial product target is
Linear plus OpenClaw, with the controller—not prompts—enforcing safety and durability.

## Status

This repository contains the credential-free controller kernel, the M1 operator surface,
the M2 bounded Linear GraphQL adapter, the M3 argv-safe OpenClaw executor plus foreground
continuous supervisor, the M4 filesystem review-packet importer, and the M5 forced-restart
release-composition proof. Packaging remains a later milestone, so this is not yet a v0.1
release candidate.

Implemented controller invariants include:

- expiring leases with monotonically increasing fencing tokens;
- schema-v1 SQLite state with strict integrity/version checks and an append-only phase journal;
- rejection of stale, expired, and non-monotonic mutations;
- evidence-bearing checkpoints as the only lease-renewal path;
- bounded agent attempts, consecutive failures, and wall-clock duration;
- deterministic advancement by normalized priority, creation time, then Linear identifier;
- a frozen pending advancement and adapter idempotency key for safe retry;
- durable attempt reservations, review consumption, and atomic advancement receipts;
- durable reserved/started dispatch state that blocks ambiguous process-loss replay;
- private prompt files, bounded independent stdout/stderr capture, strict versioned results,
  and durable response snapshots;
- single-instance foreground supervision, bounded backoff, signal-aware shutdown, and
  reconciliation-before-dispatch;
- exactly-once local reference mutations after timeout/commit ambiguity;
- completion gated on approved, immutable, hash-bound, single-consumer review evidence;
- canonical content-addressed review packets with descriptor-relative no-follow reads;
- independently recomputed Git commit plus a filter-independent raw worktree snapshot and
  full-index binary diff; every ignored/untracked regular file is bound, while untracked links,
  FIFOs, sockets/devices, hard links, and nested repositories fail closed before diffing;
  raw bytes, scan bytes, diff bytes, paths, packet members, and packet totals are bounded;
- different-provider/different-model review enforcement, controller-owned snapshots, and
  atomic run/fence-bound consumption intents and receipts.

See [docs/architecture.md](docs/architecture.md) for the boundaries and transition model and
[docs/review-packets.md](docs/review-packets.md) for the exact M4 packet/receipt contract.

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

Live mode requires both keys before the production composition is selected:

```console
# mode: live without --apply: refused
go run ./cmd/factoryd --once --config "$config_dir/factory.json"

# foreground continuous supervisor (does not daemonize)
LINEAR_API_KEY=... go run ./cmd/factoryd --apply --config "$config_dir/factory.json"
```

The supervisor executes OpenClaw only as direct argv: `openclaw agent --agent ID
--session-key KEY --message-file PRIVATE_FILE --json --timeout SECONDS`. It never uses a
shell, never puts the prompt in argv, and never
adds delivery/routing flags. `SIGTERM` cancels the bounded child process group, records the
spent attempt when the lease remains valid, closes SQLite, and exits. A persisted reserved or
started dispatch after abrupt process loss is never replayed: the run becomes explicitly
Blocked for manual reconciliation.

`doctor --json` stays offline and reports config, state DB, Linear, OpenClaw, review root,
artifact root, and service environment separately. `doctor --online` is the sole M2 opt-in
network diagnostic: it performs bounded, query-only checks of the exact configured Linear
team, project, issue, and lifecycle states. The offline OpenClaw check only verifies that the
configured executable is discoverable; it never invokes it.

The Linear adapter completely paginates issues, relationships, lifecycle states, and keyed
comments; enforces the configured team/project/allowlist; and reconciles each frozen claim or
advancement suboperation after ambiguous responses. Linear does not provide an atomic
complete-and-adopt transaction. A crash may expose temporary partial remote state, while a
restart using the same frozen intent converges without duplicate controller comments or a
different next issue.

Review packets are finalized and independently verified without credentials or network access:

```console
go run ./cmd/factoryctl packet finalize --config "$config_dir/factory.json" \
  --packet "$config_dir/reviews/.pending-123" --json
go run ./cmd/factoryctl packet verify --config "$config_dir/factory.json" \
  --packet "$config_dir/reviews/packet-<digest>" --json
```

The daemon ignores `.pending-*` directories. A bounded manifest that claims the wanted review
is fully verified, so a malformed wanted packet is observable. Foreign claims and unattributable
JSON are skipped and can never become approval. Accepted bytes are copied into the private state root before
SQLite can expose the review; source packet modes and names are not treated as immutability.

Available development commands:

```console
make fmt        # rewrite Go files with gofmt
make fmt-check  # fail if formatting differs
make test       # run unit tests
make test-race  # run unit tests with the race detector
make m5         # run the retained 20-seed/40-scenario forced-restart race matrix
make lint       # run go vet
make build      # compile all packages and commands
make smoke      # execute credential-free command smoke tests
make check      # run the local merge gate
```

`make m5` builds the real `factoryd` composition with a test-only fault-injection build tag,
starts fake Linear HTTP and fake OpenClaw as independent OS processes, and sends `SIGKILL`
only after a named phase witness has been fsynced. Every before/after row restarts against the
same SQLite database and filesystem roots. The retained seed list, matrix receipt, full race
log, and SHA-256 manifest are written under `test-results/m5/`. Normal binaries do not include
the fault injector.

## Repository Layout

```text
cmd/factoryd/              foreground supervisor, --once compatibility, live interlock
cmd/factoryctl/            init/validate/doctor/dry-run/status operator CLI
internal/config/           strict config decoding, path policy, and no-replace init
internal/domain/           state, issue, review, and deterministic selection model
internal/factory/          fail-closed controller transitions
internal/ports/            Linear, OpenClaw, state, review, and artifact contracts
internal/adapters/memory/  credential-free adapter implementations and test fakes
internal/adapters/sqlite/  schema-v1 durable store and local recovery adapter
internal/adapters/linear/  bounded GraphQL client and strict fake-server tests
internal/adapters/openclaw/ bounded argv-only executor and fake executable matrix
internal/adapters/filesystem/ strict packet verification, snapshot, and receipt adapter
internal/app/              executable vertical-slice composition
```

## License

MIT
