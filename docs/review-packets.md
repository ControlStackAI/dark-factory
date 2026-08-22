# Filesystem Review Packets

Dark Factory treats every filesystem packet as an untrusted claim. A reviewer or packet
producer writes a complete directory directly under `paths.review_root` using a name that
starts with `.pending-`. `factoryctl packet finalize` verifies the complete packet and uses
Linux `renameat2(RENAME_NOREPLACE)` to publish it as `packet-<manifest-sha256>`. `factoryd`
ignores `.pending-*`. A bounded, securely read manifest that claims the review currently wanted
by a run is fully verified, and any malformed wanted packet is an observable, fail-closed error.

## Packet format

A v1 packet is a flat directory. Subdirectories, links, devices, sockets, FIFOs, unknown
members, and unmanifested members are rejected. Every directory and file must be owned by
the daemon uid, directories may not grant group/other access, files may not grant
group/other access, and every file must have exactly one hard link.

The canonical `manifest.json` has this shape:

```json
{"packet_version":1,"review_id":"review:run-id:issue-id:1","project_id":"project-id","issue_id":"issue-id","run_id":"run-id","checkpoint_sequence":1,"source":{"commit":"0123456789abcdef0123456789abcdef01234567","diff_sha256":"<64 lowercase hex>","changed_files":["path/to/file.go"]},"members":[{"path":"response.json","kind":"artifact","sha256":"<64 lowercase hex>","size":123},{"path":"review.json","kind":"review_receipt","sha256":"<64 lowercase hex>","size":456},{"path":"source.diff","kind":"source_diff","sha256":"<64 lowercase hex>","size":789},{"path":"tests.txt","kind":"test_receipt","sha256":"<64 lowercase hex>","size":42}]}
```

JSON is canonical compact encoding with one trailing newline. Fields and members appear in
the documented order, member paths are sorted, and changed paths are unique and sorted.
Unknown fields, duplicate JSON keys, a second JSON value, alternate whitespace, or a missing
newline are rejected, and JSON nesting is capped at 64 levels. Member paths are single safe
filenames; `.`/`..`, slashes, backslashes, absolute paths, and traversal are rejected.

Member kinds are `artifact`, `test_receipt`, `review_receipt`, and `source_diff`. A packet
requires at least one artifact and test receipt, and exactly one review receipt and source
diff. The configured `limits.max_artifacts` bounds member count,
`limits.max_artifact_bytes` bounds every member, and `limits.max_packet_bytes` bounds the
manifest plus every member before contents are retained in controller memory.

The canonical review receipt is:

```json
{"receipt_version":1,"review_id":"review:run-id:issue-id:1","project_id":"project-id","issue_id":"issue-id","run_id":"run-id","checkpoint_sequence":1,"source_commit":"0123456789abcdef0123456789abcdef01234567","source_digest":"<64 lowercase hex>","artifact_path":"response.json","artifact_sha256":"<64 lowercase hex>","verdict":"approved","checks":["go test ./...","go test -race ./..."],"author":{"provider":"openai","model":"author-model"},"reviewer":{"provider":"google","model":"reviewer-model"}}
```

`verdict` must be exactly `approved`; checks must be explicit, nonempty, and unique. Both
reviewer provider and reviewer model must differ from the author's values (case-insensitive).
The receipt repeats the exact project, issue, run, checkpoint, source, and reviewed-artifact
bindings. `review_id` is exactly `review:<run_id>:<issue_id>:<checkpoint_sequence>`; run and
issue IDs containing `:` are rejected so that binding is unambiguous. A model assertion is
evidence metadata, not cryptographic proof of model identity.

## Source recomputation

The importer does not trust source fields or Git's filtered view of the worktree. It resolves
`HEAD^{commit}`, accepts only repositories whose object format is SHA-1 or SHA-256, and parses the
HEAD tree and stage-zero index under explicit mode/type bounds. Submodules, sparse/skip-worktree,
unmerged, and unusual entries fail closed. It independently walks the workspace (excluding only
the top-level `.git` administrative entry) and securely reads every tracked path and every
untracked regular file, including paths matched by `.gitignore`, `.git/info/exclude`, or
`core.excludesFile`. Opens are anchored beneath the workspace with no-follow directory traversal;
files are checked before and after bounded reads. Tracked deletions, executable-bit transitions,
and symlink targets are supported. Untracked symlinks, hard links, FIFOs, sockets/devices, other
nonregular entries, and nested repositories are rejected before any Git diff command can read or
block on them.

Raw bytes are independently hashed with the repository's Git blob algorithm. Changed blobs are
written to a private temporary object database and synthetic index, and Git diffs those immutable
objects against the resolved commit; it never diffs a live worktree path. Thus committed clean or
process filters cannot hide a raw modification, and text/working-tree conversions cannot rewrite
the payload in `source.diff`. The synthetic diff's name set must exactly equal the independent raw
parity set. A second complete raw capture and a final HEAD check reject stable mutations around
the read/diff window. The temporary object database is removed and does not add unreachable
objects to the repository.

Every Git command runs without a shell, with hooks, external diff drivers, text conversion,
optional locks, system/global configuration, system attributes, environment-injected config,
`GIT_DIFF_OPTS`, ambient pathspec controls, discovery ceilings, and line-ending conversion
disabled. Diff output pins three context lines, zero inter-hunk context, ordinary `a/` and `b/`
prefixes, Myers, full object IDs, nonrelative paths, ordinary blank context lines, and color off.

Git's linked-worktree-aware `info/attributes`, common config, and worktree config paths are
resolved with `git rev-parse --git-path`. The attributes file must be absent or empty. Draining
inotify guards parse every queued event until `EAGAIN` and reject target changes, queue overflow,
truncation, replacement, or write-and-restore races. Config files may exist but are guarded for
the full capture/diff window; repository-local `include`/`includeIf` directives fail closed
because their external targets cannot share that guard. A committed `.gitattributes` is
commit-bound; working-tree
`.gitattributes` is raw-captured into the synthetic index, so it is itself evidence and cannot be
transiently swapped underneath diffing. Attributes can still select textual hunks versus a
`GIT binary patch`; either representation operates on the exact raw blobs. The resulting raw
diff, source claim, `source.diff`, SHA-256, and changed-file membership must all match.

At most 100,000 raw paths are accepted. Each path, all raw changed payloads in aggregate, and the
final `source.diff` are bounded by `limits.max_artifact_bytes`; total tracked bytes scanned are
bounded at 16 times that value, capped at 1 GiB. Packet member and aggregate bounds still apply.
The calculation repeats immediately before snapshot commit, so later source, attributes, config,
or packet mutation fails closed.

`source_digest` is the SHA-256 of the canonical source-claim JSON plus its trailing newline.
`packet_digest` is the SHA-256 of canonical `manifest.json`. Because the manifest binds every
member's bytes and size, the finalized directory name addresses the entire packet.

## Finalize and verify

```console
factoryctl packet finalize --config /absolute/factory.json \
  --packet /absolute/reviews/.pending-123 --json

factoryctl packet verify --config /absolute/factory.json \
  --packet /absolute/reviews/packet-<digest> --json
```

`verify` is read-only and independently repeats membership, hash, receipt, independence, and
source checks. `finalize` is the only operator packet command that mutates the filesystem; it
performs the same verification before and after the no-replace rename.

## Import, snapshot, and consumption

While a run waits for `review:<run-id>:<issue-id>:<checkpoint-sequence>`, `factoryd` scans
only finalized packet directories. Before full verification, it performs a bounded attribution
read. A manifest naming another review is skipped even if that foreign packet is otherwise
malformed. Malformed JSON, a missing/invalid `review_id`, or duplicate `review_id` keys cannot be
attributed and are deterministically treated as unclaimed: ignored for this lookup and never
accepted as approval. If no healthy wanted packet exists, the run continues waiting. A parsable
manifest naming the wanted review is fully verified, so malformed wanted claims fail visibly.
The attribution exception applies only to the untrusted submission root. Controller-owned private
snapshots are all fully verified, so private-state corruption remains terminal.
The importer walks every path component without following
symlinks, opens packet children and members relative to anchored directory descriptors,
validates `fstat` before and after each bounded read, and rechecks membership and directory metadata.
It requires exactly one packet for the wanted review and exact durable run/checkpoint fields.

Accepted bytes are staged and fsynced through descriptor-relative temporary directories under
`paths.state_root/review-packets`, source-rechecked immediately before a no-replace rename,
and independently verified after installation before SQLite receives the immutable artifact and
review binding. Completion rehashes the controller-owned bytes; later changes to the submitted
packet cannot influence completion. A crash after snapshot commit is reconciled from that
same snapshot without creating a second copy.

Review consumption remains atomic and single-consumer in SQLite. Before that commit, the
adapter durably stages a canonical `pending_consumption` intent containing the packet/source
digests and current run/fence. After SQLite commits, it atomically writes the final canonical
receipt under `paths.state_root/review-receipts/`, verifies it, and removes the intent. This
preserves the original consuming fence even if the daemon dies after SQLite commit and later
reacquires the run under a higher fence. If SQLite is still unconsumed, a stale lower-fence
intent is replaced at the current fence before retry; a future-fence intent is rejected. The
final receipt records packet digest, source digest, original run/fence, UTC adapter-clock
consumption-attempt time, and `approved_and_consumed`; that timestamp is metadata, not
authenticated wall-clock provenance. Same-run replay is idempotent; another run, a changed
packet, or corrupt intent/receipt is rejected.

The state root is the immutability boundary. Read-only source modes, a content-addressed
submitted name, or a reviewer's statement alone are never trusted as immutable storage.
