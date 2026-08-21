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
  max_artifacts: 256
```

## Field contract

- `mode` is exactly `dry-run` or `live`. Live mutation requires both `mode: live` and the
  daemon process flag `--apply`. At M2, satisfying both keys proves the gate and then fails
  closed because the OpenClaw executor is not implemented until M3.
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
  `model` is optional. `timeout` is a positive Go duration. `delivery` must remain `false`.
  M1 never executes this command; M3 will define the argv contract.
- Every duration in `budgets` is positive. `max_backoff` is at least `initial_backoff`;
  `max_attempts` is 1–1000; consecutive failures are 1 through `max_attempts`.
- Lifecycle values are preferred names only. M2 resolves Linear states by lifecycle type and
  uses these names solely to disambiguate multiple states of the same type. A missing or still
  ambiguous type fails closed.
- Output bytes are bounded to 1 KiB–16 MiB, artifact bytes to 1 KiB–1 GiB, and artifact count
  to 1–10,000. M4 will enforce packet membership and receipt semantics.

## Command behavior through M2

- `factoryctl init --config PATH` creates missing private roots and atomically installs a
  `0600` config without replacing any existing file, symlink, or concurrent winner.
- `validate` and default `doctor` are read-only and offline. `doctor --online` explicitly opts
  into a bounded, query-only Linear scope/lifecycle probe and invokes no mutation operation.
- `dry-run` uses only in-memory fakes and does not create the configured state DB.
- `status` inspects the configured state path without opening SQLite or creating files.
- `factoryd --once` uses the same `internal/app` composition root as the operator commands.

The Linear client exists but is not connected to live daemon execution. OpenClaw execution,
continuous scheduling, review packet import, service installation, and release packaging
remain later milestones.
