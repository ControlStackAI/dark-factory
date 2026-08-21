# Bootstrap Architecture

## Product Boundary

`factoryd` is the controller and enforcement boundary. `factoryctl` is the operator
surface. Linear is an issue-control-plane adapter and OpenClaw is an agent-execution
adapter; neither adapter decides whether a controller transition is valid. Workflow
policies and skills may guide an agent, but their claims never substitute for controller
state or review evidence.

The bootstrap uses the same ports with in-memory implementations so the complete slice is
executable without credentials or network access.

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
priority (with zero/unset last), then creation time, then issue ID; the current issue is never
eligible to select itself. Before calling Linear, the controller persists the exact current
issue, next issue, evidence, review ID, and stable idempotency key as a pending advancement.
Retries reuse that frozen decision, and no new checkpoint, agent turn, or review binding is
accepted until it is reconciled. An expired worker cannot finalize the controller state after
an adapter call; a newly fenced worker retries the same idempotent advancement.

The Linear adapter contract requires advancement to be idempotent for the key and to treat
completion plus adoption as one logical operation. A production Linear adapter will need to
reconcile Linear's API behavior with this contract explicitly.

## Current Limitations

- Run state, issue state, review metadata, and artifacts are memory-only.
- `factoryd` supports only `--once`; scheduling, recovery supervision, and service install
  are not implemented.
- Linear and OpenClaw contracts exist, but live adapters and credential handling do not.
- Configuration, migrations, observability, workflow/skill packaging, and signed release
  artifacts remain future milestones.

These limitations are intentional: this milestone establishes an executable safety kernel
without implying production readiness.
