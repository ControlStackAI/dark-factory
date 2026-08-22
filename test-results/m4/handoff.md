# M4 Raw-Source-Parity Candidate Handoff

Date: 2026-08-21
Base: `a5bf5711f36be1858befd29145dd95da8f59abb2`
Rejected tree: `8f098b88c29ba3650cdb3c4e2a1181ddf33f1e12`
Rejected full-index binary diff SHA-256:
`582a8f97a388b82bcc5c8b9ff9eba048fc24e64f7e4a0741fa2f805ead1ea3cd`
Worktree: `/home/matthew/projects/Personal/dark-factory-worktrees/m4-review-packets`

No commit, push, canonical-main change, credential read, network contact, retained-evidence
regeneration, live-state mutation, or M5 work was performed. The candidate is uncommitted.

## Final-review remediation

- Source evidence is now derived from a secure raw filesystem capture instead of a live Git
  worktree diff. HEAD and stage-zero index entries are bounded and parsed explicitly. Raw file
  bytes and tracked symlink targets are independently hashed using the repository object format.
  SHA-1 and SHA-256 are supported; other formats fail closed.
- Raw changed blobs are written only to a private temporary object database and synthetic index.
  Git produces the name set and `--binary --full-index` patch from those immutable objects against
  the resolved commit. Clean/process/text/working-tree filters cannot hide a path or transform its
  payload. The independent raw change set must exactly match the emitted diff name set.
- The committed `.gitattributes` plus repository-local `filter.blank.clean` PoC makes ordinary Git
  report no `victim.txt` change. The candidate still binds `victim.txt` and the literal
  `MALICIOUS PAYLOAD` in `source.diff`.
- A separate filesystem walk discovers ignored/untracked files even when Git omits special
  entries. Untracked symlinks, multiply-linked files, FIFOs, sockets/devices, other nonregular
  entries, and nested repositories fail before any diff command. Descriptor-anchored, no-follow,
  nonblocking reads check type, identity, size, timestamps, and contents before/after. Deletions,
  executable-bit changes, tracked symlink changes, and regular additions are supported.
  Submodules and sparse, unmerged, or unusual index entries deliberately fail closed.
- Raw paths are capped at 100,000. Each source path, aggregate changed raw bytes, and final diff
  are bounded by `limits.max_artifact_bytes`; total tracked bytes scanned are capped at 16 times
  that limit or 1 GiB, whichever is smaller. The complete raw capture is repeated after diffing
  and HEAD is re-resolved, rejecting stable source/index mutations around the read window.
- Inotify guards now cover linked-worktree-aware `info/attributes`, common config, and worktree
  config paths. Verification drains 64 KiB reads until `EAGAIN`, parses every complete event, and
  rejects target activity, `IN_Q_OVERFLOW`, empty/unattributable events, and truncation. The
  400-decoy write/restore regression proves the target event is not hidden behind the first read.
  Repository-local `include`/`includeIf` directives are rejected because their external targets
  cannot share the guard. Working-tree `.gitattributes` is raw-captured into the synthetic index,
  so later transient edits cannot alter the immutable diff input.
- Every Git invocation pins `core.autocrlf=false`, `core.eol=lf`, `core.safecrlf=false`,
  `core.filemode=true`, and `diff.interHunkContext=0` along with the prior deterministic diff
  settings. Hostile local line-ending settings cannot strip CR bytes. `GIT_DIFF_OPTS`, ambient
  pathspec modes, discovery ceilings, and related source-enumeration variables are stripped.
- `VerifyPacket` now preserves `os.ErrNotExist` through its malformed-packet wrapping, making
  `Store.SHA256`'s absent private-snapshot mapping to `ports.ErrNotFound` reachable and tested.
- All earlier M4 behavior remains: submission-only attribution, full private-snapshot verification,
  durable consumption fences, authorization, canonical JSON, anchored packet writes, schema-v1
  compatibility, and supervisor terminal/transient classifications.

## Verification

The following completed successfully after the implementation changes:

```console
test -z "$(gofmt -l cmd internal)"
go mod verify
go test -count=1 ./...
go test -race -count=1 ./...
go vet ./...
go build ./...
make smoke
make check
git diff --check
```

Focused and retained regression gates also completed successfully:

```console
go test -count=20 -run '<all new raw/filter/config/inotify/nonregular/object-format/absence PoCs>' ./internal/adapters/filesystem
go test -race -count=5 -run '<all new raw/filter/config/inotify/nonregular/object-format/absence PoCs>' ./internal/adapters/filesystem
go test -count=5 ./internal/adapters/filesystem ./cmd/factoryctl
go test -race -count=3 ./internal/adapters/filesystem ./cmd/factoryctl
go test -race -count=1 -shuffle=20260821 ./internal/adapters/filesystem ./cmd/factoryctl ./internal/app
go test -race -count=10 -run 'TestSupervisor(ReportsMalformedWantedReviewInsteadOfWaiting|BacksOffTransientReviewStoreFailure|ReportsConsumptionConflict|ReportsMalformedConsumptionReceipt)$' ./internal/app
go test -count=20 -run 'TestPreM4SchemaOneConfigRaisesArtifactSlots$' ./internal/config
```

The focused matrix includes the 400-decoy info-attributes race, config write/restore and include
rejection, committed clean-filter hiding, raw CR retention, hostile ambient Git variables,
deletion/mode/symlink handling, sparse/submodule bounds, FIFO prompt rejection, symlink
non-exfiltration, aggregate bounds, SHA-256 repositories, raw post-capture mutation, and absent
snapshot classification.

## Candidate identity and retained evidence

This file intentionally does not embed a tree or diff hash for the full candidate containing
itself. The executing lane reports the exact temporary-index tree and the SHA-256 of the complete
temporary-index `--binary --full-index` diff out of band after the last write. The parent lane must
independently recompute both after any later edit.

The opt-in retained evidence root was not run or modified. Existing retained M4 roots bind older
candidates and are not evidence for this tree; the parent regenerates external evidence only after
audit.

The adapter remains Linux/private-local-filesystem only. Inotify queue semantics and local inode
metadata are part of the boundary. Network filesystems, cross-uid submission, remote artifact
stores, cryptographic reviewer identity, submodules, sparse checkout, and unmerged indexes remain
outside v0.1 M4. No M5 restart-composition matrix, packaging, service installation, live canary,
release, tag, commit, push, or publication was performed.
