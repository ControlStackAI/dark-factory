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

An OpenClaw attempt is reserved and journaled before dispatch, then marked started immediately
before `exec`. Losing the process with either durable state consumes that attempt and cannot
dispatch it twice. M3 does not guess whether the external run started: restart explicitly
blocks the run for reconciliation/manual resolution instead of silently using another attempt.
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

M5 extends this proof through the release process boundary. A dedicated test build tag wires
the existing SQLite and filesystem hooks plus named OpenClaw-snapshot and Linear-suboperation
hooks into `factoryd`; ordinary builds compile a zero-hook implementation. The injected daemon
fsyncs a one-shot phase/PID witness and stops itself at the exact boundary. The harness observes
that witness, sends `SIGKILL`, proves the old PID is gone, and launches a new daemon over the
same database, packet, artifact, and receipt roots. Fault placement therefore depends on an
observable named phase rather than a scheduling sleep.

The retained matrix covers before-commit and after-durable-commit-before-acknowledgement for
run creation, lease acquisition, attempt reservation, dispatch start, response snapshot,
checkpoint, review import/consumption/receipt, advancement freeze, all claim/completion/adoption
comment and state mutations, verified remote receipt, local reconciliation, and completion.
Each scenario runs SQLite integrity/cardinality inspection, fake-Linear audit comparison,
strict fence/attempt/review/selection invariants, child-process inspection, and independent
packet re-verification before its receipt is accepted.

## M3 execution and supervision

The production OpenClaw adapter invokes the configured executable directly, never through a
shell. The prompt is written mode `0600` relative to an anchored private directory and passed
as `/proc/self/fd/3/<name>`; it is removed after the child is reaped. Each attempt uses a
deterministic unique session key. Delivery and reply-routing flags are absent. Stdout and
stderr have independent fixed capture bounds; surfaced diagnostics report only stderr size and
truncation while withholding its untrusted content. Timeout/cancellation terminates the process group. A successful CLI envelope
must contain exactly one payload whose text is the strict version-1 controller result. Raw
bounded stdout is atomically snapshotted mode `0600` in the configured artifact root before
the checkpoint is accepted. The Linear credential environment variable is removed from the
child environment before exec; it is available only to the controller's Linear adapter.

`factoryd` owns a nonblocking advisory lock next to the configured database and remains in the
foreground. It resumes the one configured durable run, reacquires expired leases with a higher
fence, handles pending advancement before claims or turns, reserves before execution, polls
the filesystem review adapter without increasing attempts, and uses bounded exponential backoff.
The OpenClaw timeout is capped below the configured lease by the shutdown allowance and a
safety margin, preserving the rule that only an accepted result renews a lease. `SIGTERM` is
context-driven and does not daemonize or depend on a lane-owned background child.

## M4 review-packet boundary

The production evidence boundary wraps the SQLite store with a Linux filesystem adapter.
Submitted packets are canonical, flat, content-addressed directories finalized by an atomic
no-replace rename. The adapter treats directory names, manifest fields, permissions, and
reviewer assertions as untrusted. Directory-relative `O_NOFOLLOW` opens plus before/after
`fstat` checks reject links, nonregular objects, ownership/mode violations, concurrent file or
directory changes, and hard-link surprises. Exact membership, per-member/aggregate byte and
count limits, 64-level canonical JSON, and every SHA-256 are recomputed.

Git source inspection runs without a shell and disables hooks, external diffs, text conversion,
optional locks, ambient system/global configuration, system attributes, environment-injected
config entries, diff/pathspec environment variables, and line-ending conversion. It securely
reads raw worktree bytes and types without following path components, independently hashes them
as Git blobs, then writes only changed blobs to a private temporary object database and synthetic
index. Git diffs that immutable index—not live paths—against the resolved commit. Clean/process
filters therefore cannot hide a tracked payload or transform emitted evidence. Every ignored or
untracked regular file is discovered by an independent filesystem walk; untracked links, FIFOs,
sockets/devices, multiply linked files, and nested repositories fail before any diff command.
Deletion, executable mode, and tracked symlink-target changes are represented. Submodules,
unmerged/sparse or unusual index entries, and unsupported Git object formats fail closed; SHA-1
and SHA-256 repositories are supported deliberately. Raw changed bytes and the final diff share
the configured member bound; path count and total tracked scan bytes are separately capped.
Repository config and per-worktree `info/attributes` paths have draining inotify guards that fail
on every target event, queue overflow, or truncated event. `info/attributes` must remain absent or
empty, and repository-local config includes are rejected because the included files would be
outside those guards. Working-tree `.gitattributes` bytes are captured into the synthetic index before diffing,
so transient later edits cannot alter the immutable diff input. Import requires the exact `HEAD`
commit, raw full-index binary-capable diff, and changed-file set. Review metadata must bind
the exact project, issue, run, checkpoint, source, and response artifact, explicitly approve,
list performed checks, and name an author and reviewer whose provider and model both differ.

Verified bytes are staged and fsynced through anchored directory descriptors, source-rechecked
at the install boundary, renamed without replacement, and verified again under the controller's
private state root before SQLite records the artifact or immutable review. This copied snapshot—not
the submitted path—is the artifact implementation rehashed by the controller. SQLite remains
the atomic single-consumer boundary and journal source. A canonical pre-consumption intent plus
final filesystem receipt preserve the packet/source digests and original run/fence across a
crash or later lease reacquisition. An unconsumed lower-fence intent is safely replaced at the
current fence, while a post-commit intent keeps its original fence and any future-fence intent
fails closed. A crash on either side of snapshot, consumption, or receipt acknowledgement
converges idempotently. See
`docs/review-packets.md` for the wire format.

## Current Limitations

- Continuous foreground supervision and live Linear/OpenClaw composition are implemented;
  service installation and systemd unit packaging are not.
- Linear cannot atomically complete one issue and adopt another. SQLite freezes controller
  intent; independently reconcilable remote suboperations and final verification make restart
  convergence honest but cannot eliminate temporary partial Linear state.
- M4 supports a private local Linux filesystem only. Network filesystems, remote object stores,
  cross-uid packet submission, and cryptographic proof of claimed model identity are not supported.
- The phase journal is append-only in schema v1; retention, compaction, and archive growth
  monitoring are not implemented yet. Startup validation scans durable records and will grow
  more expensive with database size.
- Online backup, multi-controller replication, historical migrations, observability,
  workflow/skill packaging, and signed release
  artifacts remain future milestones.

These limitations are intentional: this milestone establishes an executable safety kernel
without implying production readiness.
