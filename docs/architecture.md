# Controller and Durable Recovery Architecture

## Product Boundary

`factoryd` is the controller and enforcement boundary. `factoryctl` is the operator
surface. Linear is an issue-control-plane adapter and OpenClaw is an agent-execution
adapter; neither adapter decides whether a controller transition is valid. Workflow
policies and skills may guide an agent, but their claims never substitute for controller
state or review evidence.

The credential-free slices use the same ports with either in-memory implementations or a
SQLite adapter, so recovery is executable without credentials or network access.

## Layers

```text
factoryd / factoryctl
        |
        v
internal/factory  ---- validates every state transition
        |
        v
internal/ports    ---- capability contracts
        |
        +---- Linear issue control plane
        +---- OpenClaw bounded agent turns
        +---- durable run state (compare-and-swap)
        +---- immutable review metadata
        +---- artifact digest verification
```

The controller persists mutations with versioned compare-and-swap. Adapter implementations
must preserve the contracts documented on each port; the in-memory implementations are
reference fakes, not production persistence.

## Transition Invariants

### Lease and fencing

Acquiring an available or expired lease creates a token greater than every prior token for
that run. Controller mutations require the current token. A matching token is still rejected
once its lease reaches expiration. A stale worker therefore cannot checkpoint, bind a review,
record a turn, or finalize advancement after another worker acquires the run.

Only a checkpoint containing a concrete result renews a lease. A successful bounded
OpenClaw turn becomes such a checkpoint only after the controller validates its step and
evidence. Heartbeats and vague status text are rejected.

### Retry and budget

Every OpenClaw dispatch reserves one attempt before execution. The controller blocks the
run before dispatch when the attempt limit or wall-clock deadline is exhausted, and rejects
results that return after either the lease or wall-clock deadline. Failed turns and invalid
progress evidence count against a separate consecutive-failure limit. Exhaustion is terminal
for this bootstrap and expires the lease.

### Review and completion

Completion requires review metadata that is:

- approved and explicitly immutable;
- bound to the current project and issue;
- bound to a non-empty artifact reference and SHA-256 digest;
- independently re-hashed when bound and immediately before completion;
- atomically consumed by one run, with idempotent retry allowed only for that run.

Any missing, mutable, mismatched, consumed, unavailable, or changed evidence rejects the
transition before issue advancement.

### Deterministic advancement and retry

The next issue is chosen only from unblocked Ready issues in the same project. Ordering is
priority (with zero/unset last), then creation time, then Linear identifier; the current issue is never
eligible to select itself. Before calling Linear, the controller persists the exact current
issue, next issue, evidence, review ID, and stable idempotency key as a pending advancement.
Retries reuse that frozen decision, and no new checkpoint, agent turn, or review binding is
accepted until it is reconciled. An expired worker cannot finalize the controller state after
an adapter call; a newly fenced worker retries the same idempotent advancement.

The M2 Linear adapter treats completion plus adoption as one logical controller operation,
but does not claim Linear makes those calls atomic. It queries structured keyed comments
containing only an evidence digest rather than raw review evidence,
before creating them, reconciles every ambiguous comment/state mutation, refuses conflicting
keys or scope, and verifies final Done/In Progress state plus the intended comments before it
returns success. A crash may leave temporary partial remote state; restart converges using the
same frozen intent and never selects a replacement during reconciliation.

## Durable storage

Schema version 1 stores validated run snapshots, an append-only phase journal, attempt
reservations, immutable review metadata and consumption, artifact bytes and digests, local
issue fixtures, and advancement receipts. Every run mutation uses an atomic conditional
update on the expected version. The updated snapshot, attempt reservation when applicable,
and journal entry commit in the same transaction. Review consumption commits with its
journal record. The credential-free Linear reference adapter commits current/next issue
state, the idempotency receipt, and its journal record together.

The database uses WAL mode, `synchronous=FULL`, foreign keys, a 5-second busy timeout, and
one pooled connection. This is intentionally a single-controller design: goroutines are
serialized through one writer connection, while a second process receives an explicit busy
error after the bounded wait. Controller CAS retries version conflicts within its fixed retry
limit; it does not multiply SQLite's bounded busy wait.

The SQLite driver is `modernc.org/sqlite`, a pure-Go implementation. Builds therefore do not
require CGO or a system SQLite library, at the cost of a larger dependency and binary than a
CGO-backed driver.

### Open and migration policy

The database carries both SQLite `application_id` and `user_version` markers plus a format
record. Every open runs `quick_check`, verifies all required tables, foreign keys, run JSON,
counter/reservation agreement, journal metadata, artifact hashes, review records, issues,
and advancement receipts. Unknown fields in persisted JSON are rejected.

Only an empty version-0 database may be initialized as schema v1. A non-empty unversioned
database, wrong application ID, incomplete schema, invalid record, corruption, or a schema
newer than the binary is a hard startup error. Schema creation is transactional; failures
are returned and the database is never deleted, reset, downgraded, or partially accepted.
There are no historical migrations yet. Future migrations must preserve this transactional,
forward-only, fail-closed policy.

### Crash reconciliation

An OpenClaw attempt is reserved and journaled before dispatch. Losing the process after that
commit consumes the attempt but cannot dispatch it twice; recovery uses the next attempt.
Lease fence counters live in the run snapshot, so a newly acquired lease always exceeds all
pre-restart tokens and stale workers remain rejected.

Completion first freezes its exact issue decision, evidence, review, and idempotency key.
Review consumption is durable and idempotent only for the same run/digest. If the remote
reference mutation commits but its acknowledgement is lost, restart finds the frozen
operation and repeats the same key. The receipt proves the mutation already happened, so the
adapter returns success without changing issues again; only then does the controller clear
the pending operation and advance local state. A lost acknowledgement of that final local
commit is also safe because reopen observes the reconciled snapshot.

Deterministic after-commit hooks test each persisted controller and advancement boundary by
returning an error only after the transaction is durable, modeling abrupt termination at the
point of maximum ambiguity.

## Current Limitations

- `factoryd` supports only `--once`; continuous scheduling, recovery supervision, and service
  installation are not implemented.
- The bounded Linear Cloud adapter exists and is fake-tested, but live daemon composition is
  unavailable until the OpenClaw executor and supervisor arrive in M3.
- Linear cannot atomically complete one issue and adopt another. SQLite freezes controller
  intent; independently reconcilable remote suboperations and final verification make restart
  convergence honest but cannot eliminate temporary partial Linear state.
- Review artifacts are stored as SQLite blobs only for the credential-free slice. Production
  artifact retention and backup policy are not defined.
- The phase journal is append-only in schema v1; retention, compaction, and archive growth
  monitoring are not implemented yet. Startup validation scans durable records and will grow
  more expensive with database size.
- Online backup, multi-controller replication, historical migrations, observability,
  configuration, workflow/skill packaging, and signed release
  artifacts remain future milestones.

These limitations are intentional: this milestone establishes an executable safety kernel
without implying production readiness.
