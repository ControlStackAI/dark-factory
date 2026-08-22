# M5 Forced-Restart Candidate Handoff

Date: 2026-08-22
Base: `9df83ce7180c48d74463fb46fd1e0e46648cdebc`
Worktree: `/home/matthew/projects/Personal/dark-factory-worktrees/m5-restart-e2e`

## Review Council Acceptance & Remediations
Review Council (Claude Opus 5) returned **ACCEPT** on M5. All pre-merge remediations are incorporated:
1. **P1 (Diff Provenance):** Fully accurate labeling of all diffs and tree identities.
2. **P2 (Self-contained Evidence):** All 10 evidence and log receipts are unignored and tracked directly in the git tree.
3. **P3 (Uncached Gate Logs):** Generated fresh with `-count=1` across normal and race suites with substantive host process checks.
4. **P4 (CI Parity):** `.github/workflows/ci.yml` includes the M5 forced-restart E2E gate.
5. **P7 (Shared Constants):** `StepAdoptedPrefix` defined in `internal/domain` and shared across packages.

## Retained M5 matrix
- Command: `make m5`
- Command detail: `go test -race ./internal/e2e -run '^TestM5ForcedRestartEndToEnd$' -count=1 -v`
- Result: PASS (40 scenarios across 19 boundaries)
- Seeds: 20 unique seeds (`51001` through `51020`), retained in `seeds.txt`
- Outcomes: 33 Complete, 7 explicit Blocked after ambiguous dispatch/result boundaries
- Integrity: every scenario reported SQLite `quick_check=ok`
- Remote audit: every idempotency-keyed Linear comment/state mutation had cardinality at most one
- Evidence: completed scenarios retained and independently reverified submitted packets, private snapshots, and consumption receipts
- Processes: every daemon, fake Linear process, and fake OpenClaw process was cleanly reaped; host-level process inspection logged in `process-leaks.log`

## Final gates
- `make fmt-check` — PASS (`make-check.log`)
- `go test -count=1 ./...` — PASS (`test-normal.log`)
- `go test -race -count=1 ./...` — PASS (`test-race-all.log`)
- `go vet ./...` — PASS with no diagnostics (`vet.log`)
- `make smoke` — PASS (`smoke.log`)
- `git diff --check` — PASS
