# M1 Configuration Reference

Dark Factory M1 accepts exactly `config_version: 1`. Unknown fields, duplicate keys at any
mapping depth, schema versions 0/2, multiple documents, invalid values, and missing required
environment references fail closed. JSON is accepted (and is the format emitted by `init`).
A conservative YAML subset is also accepted: two-space mappings, scalar lists, quoted or plain
scalars, booleans, integers, and empty `[]`/`{}`. YAML anchors, aliases, merge keys, tags,
multiline scalars, tabs, and flow collections are intentionally rejected.

The generated file contains references only; it never contains a resolved secret. This is an
annotated YAML-equivalent example (replace every `/srv/example` path and scope placeholder):

```yaml
config_version: 1
mode: dry-run
paths:
  state_db: /srv/example/state/factory.db
  state_root: /srv/example/state
  artifact_root: /srv/example/artifacts
  review_root: /srv/example/reviews
  workspace_root: /srv/example/workspace
  allowed_roots:
    - /srv/example
scope:
  team_id: 8f3f-exact-team-id
  project_id: 34a2-exact-project-id
  issue_id: 912c-exact-current-issue-id
  issue_allowlist:
    - 912c-exact-current-issue-id
linear:
  endpoint: https://api.linear.app/graphql
  api_key: env:LINEAR_API_KEY
openclaw:
  executable: /usr/bin/openclaw
  agent: main
  session_prefix: agent:main:dark-factory
  model: ""
  timeout: 15m
  delivery: false
budgets:
  lease_duration: 2m
  max_attempts: 8
  max_consecutive_failures: 3
  max_run_duration: 24h
  poll_interval: 5s
  initial_backoff: 1s
  max_backoff: 1m
  shutdown_timeout: 30s
lifecycle:
  ready: Ready
  in_progress: In Progress
  done: Done
limits:
  max_output_bytes: 1048576
  max_artifact_bytes: 67108864
  max_packet_bytes: 268435456
  max_artifacts: 256
```

## Field contract

- `mode` is exactly `dry-run` or `live`. Live mutation requires both `mode: live` and the
  daemon process flag `--apply`; only that pair constructs the production supervisor.
- `paths.state_db` must be inside `paths.state_root`. The state, artifact, and review roots
  must be distinct and non-overlapping. The workspace and every controller path must resolve
  inside at least one `allowed_roots` entry.
- Paths may be relative to the config directory or explicitly use `~/` and `$NAME`/
  `${NAME}` expansion. Undefined variables, empty paths, any `..` component, symlink escape,
  foreign ownership, group/world-writable ancestry within an allowed root, non-regular DB or
  config targets, and non-directory roots fail validation. Validated paths are canonical
  absolute paths in memory.
- `scope.team_id`, `project_id`, and `issue_id` are exact opaque IDs, not names or search
  terms. `issue_allowlist` is optional; when present it must contain the configured issue and
  may not contain blanks or duplicates.
- `linear.endpoint` must be HTTPS and may not contain URL user information. `api_key` accepts
  only `env:NAME`, where `NAME` is an uppercase environment identifier. `validate` requires
  that variable to be nonempty but never prints its value. M1 never sends it anywhere.
- `openclaw.executable`, `agent`, and whitespace-free isolated `session_prefix` are required.
  `model` remains an optional configuration field for compatibility, but live M3 composition
  rejects a nonempty override so its argv stays exact. `timeout` is a positive Go duration and is capped when necessary so
  shutdown plus a safety margin fit inside the lease. `delivery` must remain `false`. The M3
  executor uses direct argv, a private message file, JSON output, an isolated session key, and
  no delivery or reply-routing arguments.
- Every duration in `budgets` is positive. `max_backoff` is at least `initial_backoff`;
  `max_attempts` is 1–1000; consecutive failures are 1 through `max_attempts`.
- Lifecycle values are preferred names only. M2 resolves Linear states by lifecycle type and
  uses these names solely to disambiguate multiple states of the same type. A missing or still
  ambiguous type fails closed.
- Output bytes are bounded to 1 KiB–16 MiB, artifact bytes to 1 KiB–1 GiB, aggregate packet
  bytes to 4 KiB–1 GiB, and artifact/member count to 4–10,000. `max_packet_bytes` must be at
  least `max_artifact_bytes`. M4 requires at least four member slots for the mandatory artifact,
  test receipt, review receipt, and source diff; it checks both per-member and aggregate byte
  limits before reading member contents into controller memory.
  Schema-v1 files created before M4 may omit `max_packet_bytes`; loading derives 256 MiB or
  `max_artifact_bytes`, whichever is larger. Existing schema-v1 values of 1–3 for
  `max_artifacts` are compatibility-raised to the safe M4 minimum of four before validation.
  The validation diagnostic's 4–10,000 range describes the accepted post-normalization value;
  newly generated files always emit both fields explicitly.

## Command behavior through M4

- `factoryctl init --config PATH` creates missing private roots and atomically installs a
  `0600` config without replacing any existing file, symlink, or concurrent winner.
- `validate` and default `doctor` are read-only and offline. `doctor --online` explicitly opts
  into a bounded, query-only Linear scope/lifecycle probe and invokes no mutation operation.
- `dry-run` uses only in-memory fakes and does not create the configured state DB.
- `status` inspects the configured state path without opening SQLite or creating files.
- `packet verify` independently and read-only verifies a finalized packet; `packet finalize`
  verifies a direct `.pending-*` child of `review_root` and atomically renames it to its
  content-addressed final name without replacement.
- `factoryd --once` uses the same `internal/app` composition root as the operator commands.

The Linear and OpenClaw clients are connected only when config mode is `live` and `factoryd`
also receives `--apply`. `factoryd` then remains in the foreground, holds the database's
single-instance lock, reconciles/retries durably, polls finalized filesystem review packets,
snapshots accepted bytes before exposing an immutable SQLite review, and handles SIGTERM
cleanly. Service installation and release packaging remain later milestones. The exact packet,
review receipt, source recomputation, and consumption receipt contract is documented in
`docs/review-packets.md`.
