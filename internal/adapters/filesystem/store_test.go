//go:build linux

package filesystem

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	durablesqlite "github.com/ControlStackAI/dark-factory/internal/adapters/sqlite"
	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/factory"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

const (
	testReviewID = "review:run-1:ISSUE-1:1"
	testArtifact = "response.json"
)

type fixture struct {
	root       string
	packetRoot string
	stateRoot  string
	workspace  string
	database   string
	backend    *durablesqlite.Store
	store      *Store
	run        domain.Run
	limits     Limits
}

func TestFinalizeImportSnapshotConsumptionAndRestart(t *testing.T) {
	fx := newFixture(t, nil)
	pending := buildPacket(t, fx, nil, nil)
	finalPath, finalized, err := FinalizePacket(context.Background(), fx.packetRoot, pending, fx.workspace, fx.limits)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyPacket(context.Background(), finalPath, VerifyOptions{Workspace: fx.workspace, Limits: fx.limits, ExpectedUID: os.Geteuid()})
	if err != nil {
		t.Fatal(err)
	}
	if verified.Digest != finalized.Digest || verified.SourceDigest != finalized.SourceDigest {
		t.Fatal("independent verification changed packet identity")
	}
	review, err := fx.store.GetReview(context.Background(), testReviewID)
	if err != nil {
		t.Fatal(err)
	}
	if review.ArtifactSHA256 != digest([]byte("reviewed response\n")) {
		t.Fatalf("artifact digest=%s", review.ArtifactSHA256)
	}
	// The submitted path is untrusted even after acceptance. Mutation cannot affect the
	// controller-owned snapshot or the completion digest.
	mutateFile(t, filepath.Join(finalPath, testArtifact), []byte("changed after import\n"))
	if got, err := fx.store.SHA256(context.Background(), review.ArtifactRef); err != nil || got != review.ArtifactSHA256 {
		t.Fatalf("snapshot digest=%s error=%v", got, err)
	}
	controller := factory.New(fx.store, fx.backend, nil, fx.store, fx.store)
	if err := controller.BindReview(context.Background(), fx.run.ID, fx.run.Lease.Fence, testReviewID); err != nil {
		t.Fatal(err)
	}
	completed, err := controller.CompleteAndAdvance(context.Background(), fx.run.ID, fx.run.Lease.Fence, "independent filesystem packet approved")
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != domain.RunComplete {
		t.Fatalf("status=%s", completed.Status)
	}
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err != nil {
		t.Fatal(err)
	}
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, "different-run", review.ArtifactSHA256); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("different-run replay error=%v", err)
	}
	receipts := regularNames(t, filepath.Join(fx.stateRoot, "review-receipts"))
	if len(receipts) != 1 {
		t.Fatalf("consumption receipts=%v", receipts)
	}
	receiptBefore := readFile(t, filepath.Join(fx.stateRoot, "review-receipts", receipts[0]))
	if err := fx.store.Close(); err != nil {
		t.Fatal(err)
	}
	fx.backend = openBackend(t, fx.database)
	fx.store = openEvidenceStore(t, fx, nil)
	replayed, err := fx.store.GetReview(context.Background(), testReviewID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ConsumedByRun != fx.run.ID {
		t.Fatalf("consumer=%q", replayed.ConsumedByRun)
	}
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err != nil {
		t.Fatal(err)
	}
	receiptAfter := readFile(t, filepath.Join(fx.stateRoot, "review-receipts", receipts[0]))
	if !bytes.Equal(receiptBefore, receiptAfter) {
		t.Fatal("idempotent replay rewrote the consumption receipt")
	}
}

func TestSHA256MapsAbsentSnapshotToNotFound(t *testing.T) {
	fx := newFixture(t, nil)
	ref := artifactReference(strings.Repeat("a", 64), testArtifact)
	if _, err := fx.store.SHA256(context.Background(), ref); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("error=%v", err)
	}
	_, err := VerifyPacket(context.Background(), filepath.Join(fx.stateRoot, "review-packets", "packet-"+strings.Repeat("a", 64)), VerifyOptions{Limits: fx.limits, ExpectedUID: os.Geteuid(), SkipSource: true})
	if !errors.Is(err, os.ErrNotExist) || !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("VerifyPacket did not preserve absence through wrapping: %v", err)
	}
}

func TestMutationBeforeAndDuringImportFailsClosed(t *testing.T) {
	t.Run("source before snapshot", func(t *testing.T) {
		var changed atomic.Bool
		fx := newFixture(t, nil)
		fx.store.hook = func(phase string) error {
			if phase == "before_snapshot_source_check" && changed.CompareAndSwap(false, true) {
				return os.WriteFile(filepath.Join(fx.workspace, "candidate.txt"), []byte("mutated before snapshot\n"), 0o600)
			}
			return nil
		}
		finalizeFixturePacket(t, fx, nil, nil)
		if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("error=%v", err)
		}
		if names := regularNames(t, filepath.Join(fx.stateRoot, "review-packets")); len(names) != 0 {
			t.Fatalf("unexpected snapshot=%v", names)
		}
	})

	t.Run("packet during read", func(t *testing.T) {
		fx := newFixture(t, nil)
		finalPath := finalizeFixturePacket(t, fx, nil, nil)
		var mutated atomic.Bool
		fx.store.hook = func(phase string) error {
			if phase == "before_read:"+testArtifact && mutated.CompareAndSwap(false, true) {
				mutateFile(t, filepath.Join(finalPath, testArtifact), []byte("raced response\n"))
			}
			return nil
		}
		if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("packet before import", func(t *testing.T) {
		fx := newFixture(t, nil)
		finalPath := finalizeFixturePacket(t, fx, nil, nil)
		mutateFile(t, filepath.Join(finalPath, testArtifact), []byte("mutated response\n"))
		if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("packet before snapshot commit", func(t *testing.T) {
		fx := newFixture(t, nil)
		finalPath := finalizeFixturePacket(t, fx, nil, nil)
		var mutated atomic.Bool
		fx.store.hook = func(phase string) error {
			if phase == "before_snapshot_commit" && mutated.CompareAndSwap(false, true) {
				mutateFile(t, filepath.Join(finalPath, testArtifact), []byte("mutated before snapshot commit\n"))
			}
			return nil
		}
		if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("error=%v", err)
		}
	})

	t.Run("source while snapshot is staged", func(t *testing.T) {
		fx := newFixture(t, nil)
		finalizeFixturePacket(t, fx, nil, nil)
		var mutated atomic.Bool
		fx.store.hook = func(phase string) error {
			if phase == "before_snapshot_install" && mutated.CompareAndSwap(false, true) {
				return os.WriteFile(filepath.Join(fx.workspace, "candidate.txt"), []byte("mutated during snapshot staging\n"), 0o600)
			}
			return nil
		}
		if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("error=%v", err)
		}
		if names := regularNames(t, filepath.Join(fx.stateRoot, "review-packets")); len(names) != 0 {
			t.Fatalf("unexpected snapshot=%v", names)
		}
	})
}

func TestPacketFilesystemAttackMatrix(t *testing.T) {
	tests := []struct {
		name   string
		attack func(*testing.T, *fixture, string)
	}{
		{"missing-member", func(t *testing.T, _ *fixture, packet string) { mustRemove(t, filepath.Join(packet, "tests.txt")) }},
		{"extra-member", func(t *testing.T, _ *fixture, packet string) {
			writePrivate(t, filepath.Join(packet, "extra.txt"), []byte("extra"))
		}},
		{"symlink", func(t *testing.T, _ *fixture, packet string) {
			mustRemove(t, filepath.Join(packet, testArtifact))
			mustSymlink(t, "/dev/null", filepath.Join(packet, testArtifact))
		}},
		{"hard-link", func(t *testing.T, fx *fixture, packet string) {
			external := filepath.Join(fx.root, "external")
			writePrivate(t, external, []byte("reviewed response\n"))
			mustRemove(t, filepath.Join(packet, testArtifact))
			mustLink(t, external, filepath.Join(packet, testArtifact))
		}},
		{"insecure-mode", func(t *testing.T, _ *fixture, packet string) {
			mustChmod(t, filepath.Join(packet, testArtifact), 0o644)
		}},
		{"insecure-directory-mode", func(t *testing.T, _ *fixture, packet string) {
			mustChmod(t, packet, 0o711)
		}},
		{"fifo", func(t *testing.T, _ *fixture, packet string) {
			mustRemove(t, filepath.Join(packet, testArtifact))
			if err := exec.Command("mkfifo", filepath.Join(packet, testArtifact)).Run(); err != nil {
				t.Fatal(err)
			}
			mustChmod(t, filepath.Join(packet, testArtifact), 0o600)
		}},
		{"traversal-claim", func(t *testing.T, _ *fixture, packet string) {
			replaceManifestText(t, packet, `"path":"response.json"`, `"path":"../escape"`)
		}},
		{"unknown-manifest-field", func(t *testing.T, _ *fixture, packet string) {
			replaceManifestText(t, packet, `{"packet_version":1`, `{"unknown":true,"packet_version":1`)
		}},
		{"duplicate-manifest-field", func(t *testing.T, _ *fixture, packet string) {
			replaceManifestText(t, packet, `{"packet_version":1`, `{"packet_version":1,"packet_version":1`)
		}},
		{"duplicate-member", func(t *testing.T, _ *fixture, packet string) {
			mutateManifest(t, packet, func(manifest *Manifest) { manifest.Members = append(manifest.Members, manifest.Members[0]) })
		}},
		{"unknown-review-field", func(t *testing.T, _ *fixture, packet string) {
			rewriteReviewText(t, packet, `{"receipt_version":1`, `{"unknown":true,"receipt_version":1`)
		}},
		{"duplicate-review-field", func(t *testing.T, _ *fixture, packet string) {
			rewriteReviewText(t, packet, `{"receipt_version":1`, `{"receipt_version":1,"receipt_version":1`)
		}},
		{"noncanonical-manifest", func(t *testing.T, _ *fixture, packet string) {
			path := filepath.Join(packet, ManifestName)
			data := readFile(t, path)
			mutateFile(t, path, append([]byte(" "), data...))
			renameForManifest(t, packet)
		}},
		{"member-hash", func(t *testing.T, _ *fixture, packet string) {
			mutateFile(t, filepath.Join(packet, "tests.txt"), []byte("different\n"))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fx := newFixture(t, nil)
			packet := finalizeFixturePacket(t, fx, nil, nil)
			test.attack(t, fx, packet)
			if _, err := fx.store.GetReview(context.Background(), testReviewID); err == nil || errors.Is(err, ports.ErrNotFound) {
				t.Fatalf("attack was not observable: %v", err)
			}
		})
	}

	t.Run("owner", func(t *testing.T) {
		fx := newFixture(t, nil)
		packet := finalizeFixturePacket(t, fx, nil, nil)
		if _, err := VerifyPacket(context.Background(), packet, VerifyOptions{Workspace: fx.workspace, Limits: fx.limits, ExpectedUID: os.Geteuid() + 1}); err == nil {
			t.Fatal("accepted wrong owner")
		}
	})
}

func TestPacketDirectoryMutationDuringImportFailsClosed(t *testing.T) {
	fx := newFixture(t, nil)
	packet := finalizeFixturePacket(t, fx, nil, nil)
	var mutated atomic.Bool
	fx.store.hook = func(phase string) error {
		if phase == "before_read:"+testArtifact && mutated.CompareAndSwap(false, true) {
			return os.Chmod(packet, 0o711)
		}
		return nil
	}
	if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("error=%v", err)
	}
}

func TestPacketPathRejectsSymlinkAncestor(t *testing.T) {
	fx := newFixture(t, nil)
	packet := finalizeFixturePacket(t, fx, nil, nil)
	link := filepath.Join(fx.root, "packet-root-link")
	mustSymlink(t, fx.packetRoot, link)
	throughLink := filepath.Join(link, filepath.Base(packet))
	if _, err := VerifyPacket(context.Background(), throughLink, VerifyOptions{Workspace: fx.workspace, Limits: fx.limits, ExpectedUID: os.Geteuid()}); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("error=%v", err)
	}
}

func TestManifestReviewAndSourceBindingMatrix(t *testing.T) {
	tests := []struct {
		name     string
		review   func(*ReviewReceipt)
		manifest func(*Manifest)
	}{
		{"self-review", func(r *ReviewReceipt) { r.Reviewer = r.Author }, nil},
		{"same-provider", func(r *ReviewReceipt) { r.Reviewer.Provider = r.Author.Provider }, nil},
		{"same-model", func(r *ReviewReceipt) { r.Reviewer.Model = r.Author.Model }, nil},
		{"rejected-verdict", func(r *ReviewReceipt) { r.Verdict = "rejected" }, nil},
		{"missing-checks", func(r *ReviewReceipt) { r.Checks = nil }, nil},
		{"artifact-binding", func(r *ReviewReceipt) { r.ArtifactSHA256 = strings.Repeat("a", 64) }, nil},
		{"wrong-project", nil, func(m *Manifest) { m.ProjectID = "other" }},
		{"wrong-issue", nil, func(m *Manifest) { m.IssueID = "other" }},
		{"wrong-run", nil, func(m *Manifest) { m.RunID = "other" }},
		{"wrong-checkpoint", nil, func(m *Manifest) { m.CheckpointSequence++ }},
		{"source-commit", nil, func(m *Manifest) { m.Source.Commit = strings.Repeat("a", 40) }},
		{"diff-hash", nil, func(m *Manifest) { m.Source.DiffSHA256 = strings.Repeat("b", 64) }},
		{"changed-files", nil, func(m *Manifest) { m.Source.ChangedFiles = []string{"different.txt"} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fx := newFixture(t, nil)
			packet := buildPacket(t, fx, test.review, test.manifest)
			forceFinalize(t, fx.packetRoot, packet)
			if _, err := fx.store.GetReview(context.Background(), testReviewID); err == nil || errors.Is(err, ports.ErrNotFound) {
				t.Fatalf("binding mismatch was not observable: %v", err)
			}
		})
	}
}

func TestWantedReviewCannotNameAnotherAvailableRun(t *testing.T) {
	fx := newFixture(t, nil)
	controller := factory.New(fx.backend, fx.backend, nil, nil, nil)
	policy := domain.Policy{LeaseDuration: time.Hour, MaxRunDuration: 24 * time.Hour, MaxAttempts: 3, MaxConsecutiveFailures: 2}
	if _, err := controller.Start(context.Background(), "run-other", fx.run.ProjectID, fx.run.IssueID, "other run", policy); err != nil {
		t.Fatal(err)
	}
	lease, err := controller.AcquireLease(context.Background(), "run-other", "other-worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Checkpoint(context.Background(), "run-other", lease.Fence, 1, "candidate ready", "other evidence"); err != nil {
		t.Fatal(err)
	}
	pending := buildPacket(t, fx, nil, func(manifest *Manifest) { manifest.RunID = "run-other" })
	forceFinalize(t, fx.packetRoot, pending)
	if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("error=%v", err)
	}
}

func TestPacketSizeAndCountLimits(t *testing.T) {
	fx := newFixture(t, nil)
	packet := finalizeFixturePacket(t, fx, nil, nil)
	if _, err := VerifyPacket(context.Background(), packet, VerifyOptions{Workspace: fx.workspace, Limits: Limits{MaxMemberBytes: 8, MaxMembers: 32}, ExpectedUID: os.Geteuid()}); err == nil {
		t.Fatal("accepted oversized member")
	}
	manifestBytes := readFile(t, filepath.Join(packet, ManifestName))
	var aggregate Manifest
	if err := json.Unmarshal(manifestBytes, &aggregate); err != nil {
		t.Fatal(err)
	}
	totalBytes := int64(len(manifestBytes))
	maxMemberBytes := int64(len(manifestBytes))
	for _, member := range aggregate.Members {
		totalBytes += member.Size
		if member.Size > maxMemberBytes {
			maxMemberBytes = member.Size
		}
	}
	if _, err := VerifyPacket(context.Background(), packet, VerifyOptions{Workspace: fx.workspace, Limits: Limits{MaxMemberBytes: maxMemberBytes, MaxPacketBytes: totalBytes - 1, MaxMembers: 32}, ExpectedUID: os.Geteuid()}); err == nil {
		t.Fatal("accepted excess aggregate packet bytes")
	}

	fx = newFixture(t, nil)
	pending := buildPacket(t, fx, nil, nil)
	extra := []byte("extra test output\n")
	writePrivate(t, filepath.Join(pending, "tests-2.txt"), extra)
	manifestPath := filepath.Join(pending, ManifestName)
	var manifest Manifest
	if err := json.Unmarshal(readFile(t, manifestPath), &manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Members = append(manifest.Members, Member{Path: "tests-2.txt", Kind: "test_receipt", SHA256: digest(extra), Size: int64(len(extra))})
	sort.Slice(manifest.Members, func(i, j int) bool { return manifest.Members[i].Path < manifest.Members[j].Path })
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	mutateFile(t, manifestPath, append(encoded, '\n'))
	packet = forceFinalize(t, fx.packetRoot, pending)
	if _, err := VerifyPacket(context.Background(), packet, VerifyOptions{Workspace: fx.workspace, Limits: Limits{MaxMemberBytes: 4 << 20, MaxMembers: 4}, ExpectedUID: os.Geteuid()}); err == nil {
		t.Fatal("accepted excess member count")
	}
}

func TestCrashRecoveryAroundSnapshotAndConsumptionReceipt(t *testing.T) {
	t.Run("before-snapshot", func(t *testing.T) {
		fx := newFixture(t, func(phase string) error {
			if phase == "before_snapshot_commit" {
				return errors.New("injected crash")
			}
			return nil
		})
		finalizeFixturePacket(t, fx, nil, nil)
		if _, err := fx.store.GetReview(context.Background(), testReviewID); err == nil {
			t.Fatal("injected crash was hidden")
		}
		if len(regularNames(t, filepath.Join(fx.stateRoot, "review-packets"))) != 0 {
			t.Fatal("snapshot committed before injected crash")
		}
	})

	t.Run("after-snapshot", func(t *testing.T) {
		fx := newFixture(t, func(phase string) error {
			if phase == "after_snapshot_commit" {
				return errors.New("lost snapshot acknowledgement")
			}
			return nil
		})
		finalizeFixturePacket(t, fx, nil, nil)
		if _, err := fx.store.GetReview(context.Background(), testReviewID); err == nil {
			t.Fatal("injected crash was hidden")
		}
		fx.store.hook = nil
		if _, err := fx.store.GetReview(context.Background(), testReviewID); err != nil {
			t.Fatal(err)
		}
		if len(regularNames(t, filepath.Join(fx.stateRoot, "review-packets"))) != 1 {
			t.Fatal("snapshot was repeated")
		}
	})

	for _, phase := range []string{"before_consumption_intent", "after_consumption_intent", "before_consumption_receipt", "after_consumption_receipt"} {
		t.Run(phase, func(t *testing.T) {
			fx := newFixture(t, nil)
			finalizeFixturePacket(t, fx, nil, nil)
			review, err := fx.store.GetReview(context.Background(), testReviewID)
			if err != nil {
				t.Fatal(err)
			}
			fx.store.hook = func(got string) error {
				if got == phase {
					return errors.New("injected receipt crash")
				}
				return nil
			}
			if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err == nil {
				t.Fatal("injected crash was hidden")
			}
			fx.store.hook = nil
			if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err != nil {
				t.Fatal(err)
			}
			if len(regularNames(t, filepath.Join(fx.stateRoot, "review-receipts"))) != 1 {
				t.Fatal("receipt cardinality differs")
			}
		})
	}
}

func TestConsumptionIntentPreservesOriginalFenceAcrossReacquire(t *testing.T) {
	fx := newFixture(t, nil)
	finalizeFixturePacket(t, fx, nil, nil)
	review, err := fx.store.GetReview(context.Background(), testReviewID)
	if err != nil {
		t.Fatal(err)
	}
	originalFence := fx.run.Lease.Fence
	fx.store.hook = func(phase string) error {
		if phase == "before_consumption_receipt" {
			return errors.New("lost acknowledgement after durable consumption")
		}
		return nil
	}
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err == nil {
		t.Fatal("fault was hidden")
	}
	current, err := fx.backend.Get(context.Background(), fx.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	current.Version++
	current.Lease.ExpiresAt = time.Now().UTC().Add(-time.Second)
	if err := fx.backend.CompareAndSwap(context.Background(), current.ID, current.Version-1, current); err != nil {
		t.Fatal(err)
	}
	controller := factory.New(fx.backend, fx.backend, nil, nil, nil)
	lease, err := controller.AcquireLease(context.Background(), fx.run.ID, "replacement-worker")
	if err != nil {
		t.Fatal(err)
	}
	if lease.Fence <= originalFence {
		t.Fatalf("replacement fence=%d original=%d", lease.Fence, originalFence)
	}
	fx.store.hook = nil
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err != nil {
		t.Fatal(err)
	}
	names := regularNames(t, filepath.Join(fx.stateRoot, "review-receipts"))
	if len(names) != 1 || strings.HasPrefix(names[0], "pending-") {
		t.Fatalf("receipts=%v", names)
	}
	receipt, err := fx.store.readConsumption(names[0])
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Fence != originalFence {
		t.Fatalf("receipt fence=%d original=%d replacement=%d", receipt.Fence, originalFence, lease.Fence)
	}
}

func TestUnconsumedStaleIntentIsRewrittenAtReacquiredFence(t *testing.T) {
	fx := newFixture(t, nil)
	finalizeFixturePacket(t, fx, nil, nil)
	review, err := fx.store.GetReview(context.Background(), testReviewID)
	if err != nil {
		t.Fatal(err)
	}
	originalFence := fx.run.Lease.Fence
	fx.store.hook = func(phase string) error {
		if phase == "after_consumption_intent" {
			return errors.New("process lost after durable intent before SQLite consumption")
		}
		return nil
	}
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err == nil {
		t.Fatal("fault was hidden")
	}
	durable, err := fx.backend.GetReview(context.Background(), testReviewID)
	if err != nil || durable.ConsumedByRun != "" {
		t.Fatalf("review was consumed before injected crash: review=%+v error=%v", durable, err)
	}
	lease := reacquireFixtureLease(t, fx)
	if lease.Fence <= originalFence {
		t.Fatalf("replacement fence=%d original=%d", lease.Fence, originalFence)
	}
	fx.store.hook = nil
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err != nil {
		t.Fatal(err)
	}
	names := regularNames(t, filepath.Join(fx.stateRoot, "review-receipts"))
	if len(names) != 1 || strings.HasPrefix(names[0], "pending-") {
		t.Fatalf("receipts=%v", names)
	}
	receipt, err := fx.store.readConsumption(names[0])
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Fence != lease.Fence {
		t.Fatalf("receipt fence=%d replacement=%d original=%d", receipt.Fence, lease.Fence, originalFence)
	}
}

func TestFutureFenceConsumptionIntentFailsClosed(t *testing.T) {
	fx := newFixture(t, nil)
	finalizeFixturePacket(t, fx, nil, nil)
	review, err := fx.store.GetReview(context.Background(), testReviewID)
	if err != nil {
		t.Fatal(err)
	}
	fx.store.hook = func(phase string) error {
		if phase == "after_consumption_intent" {
			return errors.New("stop before SQLite consumption")
		}
		return nil
	}
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err == nil {
		t.Fatal("fault was hidden")
	}
	fx.store.hook = nil
	names := regularNames(t, filepath.Join(fx.stateRoot, "review-receipts"))
	if len(names) != 1 || !strings.HasPrefix(names[0], "pending-") {
		t.Fatalf("intents=%v", names)
	}
	intent, err := fx.store.readConsumption(names[0])
	if err != nil {
		t.Fatal(err)
	}
	intent.Fence = fx.run.Lease.Fence + 1
	encoded, err := canonicalConsumption(intent)
	if err != nil {
		t.Fatal(err)
	}
	mutateFile(t, filepath.Join(fx.stateRoot, "review-receipts", names[0]), encoded)
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("error=%v", err)
	}
	durable, err := fx.backend.GetReview(context.Background(), testReviewID)
	if err != nil || durable.ConsumedByRun != "" {
		t.Fatalf("future intent consumed review: review=%+v error=%v", durable, err)
	}
}

func TestCorruptConsumptionIntentFailsClosed(t *testing.T) {
	fx := newFixture(t, nil)
	finalizeFixturePacket(t, fx, nil, nil)
	review, err := fx.store.GetReview(context.Background(), testReviewID)
	if err != nil {
		t.Fatal(err)
	}
	fx.store.hook = func(phase string) error {
		if phase == "before_consumption_receipt" {
			return errors.New("stop after durable consumption")
		}
		return nil
	}
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err == nil {
		t.Fatal("fault was hidden")
	}
	fx.store.hook = nil
	names := regularNames(t, filepath.Join(fx.stateRoot, "review-receipts"))
	if len(names) != 1 || !strings.HasPrefix(names[0], "pending-") {
		t.Fatalf("intents=%v", names)
	}
	mutateFile(t, filepath.Join(fx.stateRoot, "review-receipts", names[0]), []byte("{}\n"))
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("error=%v", err)
	}
}

func TestCorruptConsumptionReceiptFailsClosed(t *testing.T) {
	fx := newFixture(t, nil)
	finalizeFixturePacket(t, fx, nil, nil)
	review, err := fx.store.GetReview(context.Background(), testReviewID)
	if err != nil {
		t.Fatal(err)
	}
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err != nil {
		t.Fatal(err)
	}
	receipts := regularNames(t, filepath.Join(fx.stateRoot, "review-receipts"))
	path := filepath.Join(fx.stateRoot, "review-receipts", receipts[0])
	mutateFile(t, path, []byte("{}\n"))
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("error=%v", err)
	}
}

func newFixture(t *testing.T, hook Hook) *fixture {
	t.Helper()
	root := t.TempDir()
	for _, path := range []string{root, filepath.Join(root, "packets"), filepath.Join(root, "state"), filepath.Join(root, "workspace")} {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			t.Fatal(err)
		}
		mustChmod(t, path, 0o700)
	}
	workspace := filepath.Join(root, "workspace")
	runGit(t, workspace, "init", "-q")
	writePrivate(t, filepath.Join(workspace, "baseline.txt"), []byte("baseline\n"))
	runGit(t, workspace, "add", "baseline.txt")
	runGit(t, workspace, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "baseline")
	writePrivate(t, filepath.Join(workspace, "candidate.txt"), []byte("candidate diff\n"))
	database := filepath.Join(root, "state", "factory.db")
	backend := openBackend(t, database)
	ctx := context.Background()
	issue := domain.Issue{ID: "ISSUE-1", Identifier: "DF-1", ProjectID: "PROJECT-1", Title: "packet test", State: domain.IssueReady, CreatedAt: time.Now().UTC()}
	if err := backend.EnsureIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	controller := factory.New(backend, backend, nil, nil, nil)
	policy := domain.Policy{LeaseDuration: time.Hour, MaxRunDuration: 24 * time.Hour, MaxAttempts: 3, MaxConsecutiveFailures: 2}
	if _, err := controller.Start(ctx, "run-1", issue.ProjectID, issue.ID, "started", policy); err != nil {
		t.Fatal(err)
	}
	lease, err := controller.AcquireLease(ctx, "run-1", "worker")
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Checkpoint(ctx, "run-1", lease.Fence, 1, "candidate ready", "candidate filesystem evidence"); err != nil {
		t.Fatal(err)
	}
	run, err := backend.Get(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	fx := &fixture{root: root, packetRoot: filepath.Join(root, "packets"), stateRoot: filepath.Join(root, "state"), workspace: workspace,
		database: database, backend: backend, run: run, limits: Limits{MaxMemberBytes: 4 << 20, MaxMembers: 32}}
	fx.store = openEvidenceStore(t, fx, hook)
	return fx
}

func openBackend(t *testing.T, path string) *durablesqlite.Store {
	t.Helper()
	store, err := durablesqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func openEvidenceStore(t *testing.T, fx *fixture, hook Hook) *Store {
	t.Helper()
	store, err := Open(Options{PacketRoot: fx.packetRoot, StateRoot: fx.stateRoot, WorkspaceRoot: fx.workspace, Limits: fx.limits,
		Backend: fx.backend, ExpectedUID: os.Geteuid(), Hook: hook, Now: func() time.Time { return time.Unix(1_800_000_000, 123).UTC() }})
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func reacquireFixtureLease(t *testing.T, fx *fixture) domain.Lease {
	t.Helper()
	current, err := fx.backend.Get(context.Background(), fx.run.ID)
	if err != nil {
		t.Fatal(err)
	}
	current.Version++
	current.Lease.ExpiresAt = time.Now().UTC().Add(-time.Second)
	if err := fx.backend.CompareAndSwap(context.Background(), current.ID, current.Version-1, current); err != nil {
		t.Fatal(err)
	}
	controller := factory.New(fx.backend, fx.backend, nil, nil, nil)
	lease, err := controller.AcquireLease(context.Background(), fx.run.ID, "replacement-worker")
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func buildPacket(t *testing.T, fx *fixture, mutateReview func(*ReviewReceipt), mutateManifest func(*Manifest)) string {
	t.Helper()
	state, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	sourceDigest, err := SourceDigest(state.Claim)
	if err != nil {
		t.Fatal(err)
	}
	review := ReviewReceipt{ReceiptVersion: ReceiptVersion, ReviewID: testReviewID, ProjectID: fx.run.ProjectID, IssueID: fx.run.IssueID, RunID: fx.run.ID,
		CheckpointSequence: fx.run.CheckpointSequence, SourceCommit: state.Claim.Commit, SourceDigest: sourceDigest, ArtifactPath: testArtifact,
		ArtifactSHA256: digest([]byte("reviewed response\n")), Verdict: "approved", Checks: []string{"go test ./...", "go test -race ./..."},
		Author: Identity{Provider: "openai", Model: "gpt-author"}, Reviewer: Identity{Provider: "google", Model: "gemini-reviewer"}}
	if mutateReview != nil {
		mutateReview(&review)
	}
	reviewBytes, _ := json.Marshal(review)
	reviewBytes = append(reviewBytes, '\n')
	files := map[string][]byte{
		testArtifact: []byte("reviewed response\n"), "tests.txt": []byte("all focused checks passed\n"),
		"source.diff": append([]byte(nil), state.Diff...), "review.json": reviewBytes,
	}
	members := []Member{
		{Path: "response.json", Kind: "artifact"}, {Path: "review.json", Kind: "review_receipt"},
		{Path: "source.diff", Kind: "source_diff"}, {Path: "tests.txt", Kind: "test_receipt"},
	}
	for index := range members {
		members[index].Size = int64(len(files[members[index].Path]))
		members[index].SHA256 = digest(files[members[index].Path])
	}
	sort.Slice(members, func(i, j int) bool { return members[i].Path < members[j].Path })
	manifest := Manifest{PacketVersion: PacketVersion, ReviewID: testReviewID, ProjectID: fx.run.ProjectID, IssueID: fx.run.IssueID, RunID: fx.run.ID,
		CheckpointSequence: fx.run.CheckpointSequence, Source: state.Claim, Members: members}
	if mutateManifest != nil {
		mutateManifest(&manifest)
		// Keep the review receipt target/source claim in sync unless the test deliberately
		// supplied its own receipt mutation.
		if mutateReview == nil {
			sourceDigest, _ = SourceDigest(manifest.Source)
			review.ProjectID, review.IssueID, review.RunID, review.CheckpointSequence = manifest.ProjectID, manifest.IssueID, manifest.RunID, manifest.CheckpointSequence
			review.SourceCommit, review.SourceDigest = manifest.Source.Commit, sourceDigest
			reviewBytes, _ = json.Marshal(review)
			reviewBytes = append(reviewBytes, '\n')
			files["review.json"] = reviewBytes
			for index := range manifest.Members {
				if manifest.Members[index].Path == "review.json" {
					manifest.Members[index].Size = int64(len(reviewBytes))
					manifest.Members[index].SHA256 = digest(reviewBytes)
				}
			}
		}
	}
	manifestBytes, _ := json.Marshal(manifest)
	manifestBytes = append(manifestBytes, '\n')
	pending, err := os.MkdirTemp(fx.packetRoot, ".pending-")
	if err != nil {
		t.Fatal(err)
	}
	mustChmod(t, pending, 0o700)
	files[ManifestName] = manifestBytes
	for name, data := range files {
		writePrivate(t, filepath.Join(pending, name), data)
	}
	return pending
}

func finalizeFixturePacket(t *testing.T, fx *fixture, review func(*ReviewReceipt), manifest func(*Manifest)) string {
	t.Helper()
	pending := buildPacket(t, fx, review, manifest)
	path, _, err := FinalizePacket(context.Background(), fx.packetRoot, pending, fx.workspace, fx.limits)
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func forceFinalize(t *testing.T, root, pending string) string {
	t.Helper()
	manifest := readFile(t, filepath.Join(pending, ManifestName))
	final := filepath.Join(root, "packet-"+digest(manifest))
	if err := os.Rename(pending, final); err != nil {
		t.Fatal(err)
	}
	return final
}

func replaceManifestText(t *testing.T, packet, old, replacement string) {
	t.Helper()
	path := filepath.Join(packet, ManifestName)
	data := string(readFile(t, path))
	if !strings.Contains(data, old) {
		t.Fatalf("manifest does not contain %q", old)
	}
	mutateFile(t, path, []byte(strings.Replace(data, old, replacement, 1)))
	renameForManifest(t, packet)
}

func rewriteReviewText(t *testing.T, packet, old, replacement string) {
	t.Helper()
	reviewPath := filepath.Join(packet, "review.json")
	data := string(readFile(t, reviewPath))
	if !strings.Contains(data, old) {
		t.Fatalf("review does not contain %q", old)
	}
	updated := []byte(strings.Replace(data, old, replacement, 1))
	mutateFile(t, reviewPath, updated)
	mutateManifest(t, packet, func(manifest *Manifest) {
		for index := range manifest.Members {
			if manifest.Members[index].Path == "review.json" {
				manifest.Members[index].SHA256 = digest(updated)
				manifest.Members[index].Size = int64(len(updated))
			}
		}
	})
}

func mutateManifest(t *testing.T, packet string, mutate func(*Manifest)) {
	t.Helper()
	path := filepath.Join(packet, ManifestName)
	var manifest Manifest
	if err := json.Unmarshal(readFile(t, path), &manifest); err != nil {
		t.Fatal(err)
	}
	mutate(&manifest)
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	mutateFile(t, path, append(encoded, '\n'))
	renameForManifest(t, packet)
}

func renameForManifest(t *testing.T, packet string) {
	t.Helper()
	final := filepath.Join(filepath.Dir(packet), "packet-"+digest(readFile(t, filepath.Join(packet, ManifestName))))
	if final != packet {
		if err := os.Rename(packet, final); err != nil {
			t.Fatal(err)
		}
	}
}

func runGit(t *testing.T, directory string, args ...string) {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
}

func runGitOutput(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", args...)
	command.Dir = directory
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %v: %s", args, err, output)
	}
	return string(output)
}

func writePrivate(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	mustChmod(t, path, 0o600)
}

func mutateFile(t *testing.T, path string, data []byte) {
	t.Helper()
	mustChmod(t, path, 0o600)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
func mustRemove(t *testing.T, path string) {
	t.Helper()
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
}
func mustChmod(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}
func mustSymlink(t *testing.T, target, path string) {
	t.Helper()
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
}
func mustLink(t *testing.T, target, path string) {
	t.Helper()
	if err := os.Link(target, path); err != nil {
		t.Fatal(err)
	}
}
func regularNames(t *testing.T, root string) []string {
	t.Helper()
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	sort.Strings(names)
	return names
}
func TestCanonicalEncodingRejectsUnknownAndDuplicateFields(t *testing.T) {
	var target Manifest
	for _, input := range []string{`{"packet_version":1,"unknown":true}`, `{"packet_version":1,"packet_version":1}`} {
		if err := strictCanonicalJSON([]byte(input), &target, func() ([]byte, error) { return CanonicalManifest(target) }); err == nil {
			t.Fatalf("accepted %s", input)
		}
	}
}

func TestCanonicalEncodingRejectsExcessiveNesting(t *testing.T) {
	input := strings.Repeat("[", maxCanonicalJSONDepth+1) + "0" + strings.Repeat("]", maxCanonicalJSONDepth+1)
	var target Manifest
	if err := strictCanonicalJSON([]byte(input), &target, func() ([]byte, error) { return CanonicalManifest(target) }); err == nil || !strings.Contains(err.Error(), "nesting") {
		t.Fatalf("error=%v", err)
	}
}

func TestAtomicStateWritesUseExactModes(t *testing.T) {
	fx := newFixture(t, nil)
	finalizeFixturePacket(t, fx, nil, nil)
	review, err := fx.store.GetReview(context.Background(), testReviewID)
	if err != nil {
		t.Fatal(err)
	}
	if err := fx.store.ConsumeReview(context.Background(), testReviewID, fx.run.ID, review.ArtifactSHA256); err != nil {
		t.Fatal(err)
	}
	for _, root := range []string{filepath.Join(fx.stateRoot, "review-packets"), filepath.Join(fx.stateRoot, "review-receipts")} {
		entries, err := os.ReadDir(root)
		if err != nil || len(entries) != 1 {
			t.Fatalf("root=%s entries=%v error=%v", root, entries, err)
		}
		info, err := entries[0].Info()
		if err != nil {
			t.Fatal(err)
		}
		want := os.FileMode(0o400)
		if info.IsDir() {
			want = 0o700
		}
		if info.Mode().Perm() != want {
			t.Fatalf("%s mode=%#o want=%#o", filepath.Join(root, entries[0].Name()), info.Mode().Perm(), want)
		}
		if info.IsDir() {
			members, err := os.ReadDir(filepath.Join(root, entries[0].Name()))
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range members {
				memberInfo, err := member.Info()
				if err != nil {
					t.Fatal(err)
				}
				if !memberInfo.Mode().IsRegular() || memberInfo.Mode().Perm() != 0o400 {
					t.Fatalf("snapshot member %s mode=%s", member.Name(), memberInfo.Mode())
				}
			}
		}
	}
}

func TestConsumptionReceiptRequiresCanonicalUTC(t *testing.T) {
	receipt := ConsumptionReceipt{ReceiptVersion: ReceiptVersion, ReviewID: testReviewID, PacketDigest: strings.Repeat("a", 64), SourceDigest: strings.Repeat("b", 64), RunID: "run-1", Fence: 1, ConsumedAt: "2027-01-15T09:00:00+01:00", Outcome: "approved_and_consumed"}
	if _, err := canonicalConsumption(receipt); err == nil {
		t.Fatal("accepted non-UTC consumption time")
	}
}

func TestPendingPacketsAreIgnored(t *testing.T) {
	fx := newFixture(t, nil)
	_ = buildPacket(t, fx, nil, nil)
	if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("error=%v", err)
	}
}

func TestDuplicateFinalPacketsForWantedReviewFailClosed(t *testing.T) {
	fx := newFixture(t, nil)
	first := finalizeFixturePacket(t, fx, nil, nil)
	secondPending := buildPacket(t, fx, func(r *ReviewReceipt) { r.Checks = append(r.Checks, "go vet ./...") }, nil)
	second := forceFinalize(t, fx.packetRoot, secondPending)
	if first == second {
		t.Fatal("packet identities unexpectedly equal")
	}
	if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("error=%v", err)
	}
}

func TestMalformedForeignFinalPacketsDoNotHideHealthyWantedReview(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{name: "parsable-foreign-claim", mutate: func(t *testing.T, pending string) {
			path := filepath.Join(pending, ManifestName)
			var manifest Manifest
			if err := json.Unmarshal(readFile(t, path), &manifest); err != nil {
				t.Fatal(err)
			}
			manifest.ReviewID = "review:foreign-run:FOREIGN-1:1"
			manifest.ProjectID = "FOREIGN-PROJECT"
			manifest.IssueID = "FOREIGN-1"
			manifest.RunID = "foreign-run"
			encoded, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			mutateFile(t, path, append(encoded, '\n'))
		}},
		{name: "malformed-json-unattributable", mutate: func(t *testing.T, pending string) {
			mutateFile(t, filepath.Join(pending, ManifestName), []byte("{not-json\n"))
		}},
		{name: "missing-review-id-unattributable", mutate: func(t *testing.T, pending string) {
			contents := readFile(t, filepath.Join(pending, ManifestName))
			var object map[string]any
			if err := json.Unmarshal(contents, &object); err != nil {
				t.Fatal(err)
			}
			delete(object, "review_id")
			encoded, err := json.Marshal(object)
			if err != nil {
				t.Fatal(err)
			}
			mutateFile(t, filepath.Join(pending, ManifestName), append(encoded, '\n'))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t, nil)
			foreign := buildPacket(t, fx, nil, nil)
			tc.mutate(t, foreign)
			forceFinalize(t, fx.packetRoot, foreign)
			finalizeFixturePacket(t, fx, nil, nil)
			review, err := fx.store.GetReview(context.Background(), testReviewID)
			if err != nil {
				t.Fatal(err)
			}
			if review.ID != testReviewID {
				t.Fatalf("review=%+v", review)
			}
		})
	}
}

func TestUnattributableFinalPacketIsIgnoredAsUnclaimed(t *testing.T) {
	fx := newFixture(t, nil)
	pending := buildPacket(t, fx, nil, nil)
	mutateFile(t, filepath.Join(pending, ManifestName), []byte("{}\n"))
	forceFinalize(t, fx.packetRoot, pending)
	if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("error=%v", err)
	}
}

func TestUnattributablePrivateSnapshotFailsClosed(t *testing.T) {
	fx := newFixture(t, nil)
	submitted := finalizeFixturePacket(t, fx, nil, nil)
	if _, err := fx.store.GetReview(context.Background(), testReviewID); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(submitted, filepath.Join(fx.packetRoot, ".pending-retained-source")); err != nil {
		t.Fatal(err)
	}
	snapshots := regularNames(t, filepath.Join(fx.stateRoot, "review-packets"))
	if len(snapshots) != 1 {
		t.Fatalf("snapshots=%v", snapshots)
	}
	manifest := filepath.Join(fx.stateRoot, "review-packets", snapshots[0], ManifestName)
	mutateFile(t, manifest, []byte("{}\n"))
	if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("error=%v", err)
	}
}

func TestMalformedWantedClaimFailsClosedBesideHealthyPacket(t *testing.T) {
	fx := newFixture(t, nil)
	finalizeFixturePacket(t, fx, nil, nil)
	malformed := buildPacket(t, fx, func(receipt *ReviewReceipt) {
		receipt.Checks = append(receipt.Checks, "go vet ./...")
	}, nil)
	malformed = forceFinalize(t, fx.packetRoot, malformed)
	mutateFile(t, filepath.Join(malformed, testArtifact), []byte("corrupt wanted artifact\n"))
	if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
		t.Fatalf("error=%v", err)
	}
}

func TestPacketRunBackendErrorsAreClassified(t *testing.T) {
	t.Run("not-found-is-malformed", func(t *testing.T) {
		fx := newFixture(t, nil)
		finalizeFixturePacket(t, fx, nil, nil)
		fx.store.backend = &getErrorBackend{Backend: fx.backend, err: ports.ErrNotFound}
		if _, err := fx.store.GetReview(context.Background(), testReviewID); !errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("transient-retains-cause", func(t *testing.T) {
		fx := newFixture(t, nil)
		finalizeFixturePacket(t, fx, nil, nil)
		transient := errors.New("temporary backend outage")
		fx.store.backend = &getErrorBackend{Backend: fx.backend, err: transient}
		_, err := fx.store.GetReview(context.Background(), testReviewID)
		if !errors.Is(err, transient) || errors.Is(err, ErrMalformedPacket) {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestSourceInspectionIsDeterministic(t *testing.T) {
	fx := newFixture(t, nil)
	first, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	second, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !equalSource(first.Claim, second.Claim) || !bytes.Equal(first.Diff, second.Diff) {
		t.Fatal("source recomputation changed")
	}
	if fmt.Sprint(first.Claim.ChangedFiles) != "[candidate.txt]" {
		t.Fatalf("changed=%v", first.Claim.ChangedFiles)
	}
}

func TestSourceInspectionIncludesIgnoredUntrackedPaths(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		configure func(*testing.T, *fixture, string)
	}{
		{name: "gitignore", path: "ignored-by-worktree", configure: func(t *testing.T, fx *fixture, path string) {
			writePrivate(t, filepath.Join(fx.workspace, ".gitignore"), []byte(path+"\n"))
		}},
		{name: "info-exclude", path: "ignored-by-info", configure: func(t *testing.T, fx *fixture, path string) {
			writePrivate(t, filepath.Join(fx.workspace, ".git", "info", "exclude"), []byte(path+"\n"))
		}},
		{name: "core-excludes-file", path: "ignored-by-core-config", configure: func(t *testing.T, fx *fixture, path string) {
			excludes := filepath.Join(fx.root, "global-excludes")
			writePrivate(t, excludes, []byte(path+"\n"))
			runGit(t, fx.workspace, "config", "core.excludesFile", excludes)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t, nil)
			tc.configure(t, fx, tc.path)
			writePrivate(t, filepath.Join(fx.workspace, tc.path), []byte("ignored evidence\n"))
			state, err := InspectSource(context.Background(), fx.workspace)
			if err != nil {
				t.Fatal(err)
			}
			index := sort.SearchStrings(state.Claim.ChangedFiles, tc.path)
			if index == len(state.Claim.ChangedFiles) || state.Claim.ChangedFiles[index] != tc.path || !bytes.Contains(state.Diff, []byte("ignored evidence")) {
				t.Fatalf("ignored path was not bound: changed=%v", state.Claim.ChangedFiles)
			}
		})
	}
}

func TestSourceInspectionRejectsNestedUntrackedRepository(t *testing.T) {
	fx := newFixture(t, nil)
	nested := filepath.Join(fx.workspace, "nested")
	if err := os.Mkdir(nested, 0o700); err != nil {
		t.Fatal(err)
	}
	runGit(t, nested, "init", "-q")
	writePrivate(t, filepath.Join(nested, "candidate.txt"), []byte("nested candidate\n"))
	if _, err := InspectSource(context.Background(), fx.workspace); err == nil || !strings.Contains(err.Error(), "nested repository") {
		t.Fatalf("error=%v", err)
	}
}

func TestSourceDiffIgnoresRepositoryDiffConfiguration(t *testing.T) {
	fx := newFixture(t, nil)
	writePrivate(t, filepath.Join(fx.workspace, "baseline.txt"), []byte("baseline\n\nchanged\n"))
	baseline, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	settings := map[string]string{
		"diff.context": "0", "diff.noprefix": "true", "diff.mnemonicPrefix": "true",
		"diff.algorithm": "patience", "core.abbrev": "7", "diff.relative": "true",
		"diff.suppressBlankEmpty": "true", "color.ui": "always", "core.autocrlf": "true",
		"core.eol": "crlf", "core.safecrlf": "true", "diff.interHunkContext": "99",
	}
	for key, value := range settings {
		runGit(t, fx.workspace, "config", key, value)
	}
	after, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !equalSource(baseline.Claim, after.Claim) || !bytes.Equal(baseline.Diff, after.Diff) {
		t.Fatal("repository-local diff configuration changed evidence bytes")
	}
	if bytes.Contains(after.Diff, []byte("\x1b[")) {
		t.Fatal("ANSI color escaped into evidence bytes")
	}
}

func TestRawSourceParityDefeatsCommittedCleanFilter(t *testing.T) {
	fx := newFixture(t, nil)
	writePrivate(t, filepath.Join(fx.workspace, ".gitattributes"), []byte("victim.txt filter=blank\n"))
	writePrivate(t, filepath.Join(fx.workspace, "victim.txt"), []byte("baseline\n"))
	runGit(t, fx.workspace, "add", ".gitattributes", "victim.txt")
	runGit(t, fx.workspace, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "filter baseline")
	runGit(t, fx.workspace, "config", "filter.blank.clean", "sed '/MALICIOUS PAYLOAD/d'")
	mutateFile(t, filepath.Join(fx.workspace, "victim.txt"), []byte("baseline\nMALICIOUS PAYLOAD\n"))
	if hidden := runGitOutput(t, fx.workspace, "diff", "--name-only", "HEAD", "--", "victim.txt"); strings.TrimSpace(hidden) != "" {
		t.Fatalf("PoC precondition failed: ordinary Git exposed filter-hidden path: %q", hidden)
	}
	state, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(state.Claim.ChangedFiles, "victim.txt") || !bytes.Contains(state.Diff, []byte("MALICIOUS PAYLOAD")) {
		t.Fatalf("raw payload was not bound: changed=%v diff=%q", state.Claim.ChangedFiles, state.Diff)
	}
}

func TestRawSourcePinsCRLFAndAmbientGitEnvironment(t *testing.T) {
	fx := newFixture(t, nil)
	writePrivate(t, filepath.Join(fx.workspace, "crlf.txt"), []byte("baseline\r\n"))
	runGit(t, fx.workspace, "add", "crlf.txt")
	runGit(t, fx.workspace, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "crlf baseline")
	mutateFile(t, filepath.Join(fx.workspace, "crlf.txt"), []byte("candidate\r\n"))
	baseline, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(baseline.Diff, []byte("+candidate\r\n")) {
		t.Fatalf("raw CR byte missing from diff: %q", baseline.Diff)
	}
	for key, value := range map[string]string{"core.autocrlf": "true", "core.eol": "crlf", "core.safecrlf": "true", "diff.interHunkContext": "99"} {
		runGit(t, fx.workspace, "config", key, value)
	}
	for name, value := range map[string]string{
		"GIT_DIFF_OPTS": "-U0", "GIT_LITERAL_PATHSPECS": "1", "GIT_GLOB_PATHSPECS": "1",
		"GIT_NOGLOB_PATHSPECS": "1", "GIT_ICASE_PATHSPECS": "1", "GIT_CEILING_DIRECTORIES": fx.workspace,
		"GIT_DISCOVERY_ACROSS_FILESYSTEM": "1", "GIT_PREFIX": "hostile/",
	} {
		t.Setenv(name, value)
	}
	after, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !equalSource(baseline.Claim, after.Claim) || !bytes.Equal(baseline.Diff, after.Diff) {
		t.Fatal("ambient Git diff/pathspec/discovery environment changed raw evidence")
	}
}

func TestRawSourceTracksDeletionModeAndSymlinkBytes(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *fixture)
		path   string
		want   []byte
	}{
		{name: "deletion", path: "baseline.txt", mutate: func(t *testing.T, fx *fixture) {
			t.Helper()
			mustRemove(t, filepath.Join(fx.workspace, "baseline.txt"))
		}, want: []byte("deleted file mode")},
		{name: "executable-bit", path: "baseline.txt", mutate: func(t *testing.T, fx *fixture) {
			t.Helper()
			mustChmod(t, filepath.Join(fx.workspace, "baseline.txt"), 0o700)
		}, want: []byte("old mode 100644")},
		{name: "tracked-symlink", path: "tracked-link", mutate: func(t *testing.T, fx *fixture) {
			t.Helper()
			if err := os.Symlink("first-target", filepath.Join(fx.workspace, "tracked-link")); err != nil {
				t.Fatal(err)
			}
			runGit(t, fx.workspace, "add", "tracked-link")
			runGit(t, fx.workspace, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "tracked link")
			if err := os.Remove(filepath.Join(fx.workspace, "tracked-link")); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink("second-target", filepath.Join(fx.workspace, "tracked-link")); err != nil {
				t.Fatal(err)
			}
		}, want: []byte("second-target")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fx := newFixture(t, nil)
			tc.mutate(t, fx)
			state, err := InspectSource(context.Background(), fx.workspace)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(state.Claim.ChangedFiles, tc.path) || !bytes.Contains(state.Diff, tc.want) {
				t.Fatalf("raw change absent: changed=%v diff=%q", state.Claim.ChangedFiles, state.Diff)
			}
		})
	}
}

func TestRawSourceRejectsSparseAndSubmoduleEntries(t *testing.T) {
	t.Run("sparse-index-flag", func(t *testing.T) {
		fx := newFixture(t, nil)
		runGit(t, fx.workspace, "update-index", "--skip-worktree", "baseline.txt")
		if _, err := InspectSource(context.Background(), fx.workspace); err == nil || !strings.Contains(err.Error(), "sparse") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("submodule", func(t *testing.T) {
		fx := newFixture(t, nil)
		oid := strings.TrimSpace(runGitOutput(t, fx.workspace, "rev-parse", "HEAD"))
		runGit(t, fx.workspace, "update-index", "--add", "--cacheinfo", "160000", oid, "submodule")
		if _, err := InspectSource(context.Background(), fx.workspace); err == nil || !strings.Contains(err.Error(), "submodule") {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestUntrackedNonregularPathsFailBeforeDiff(t *testing.T) {
	t.Run("fifo-promptly", func(t *testing.T) {
		fx := newFixture(t, nil)
		fifo := filepath.Join(fx.workspace, "untracked-fifo")
		if err := exec.Command("mkfifo", fifo).Run(); err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, err := inspectSourceLimited(ctx, fx.workspace, fx.limits.MaxMemberBytes, func(phase string) error {
			if phase == "before_diff" {
				t.Fatal("Git diff ran before FIFO rejection")
			}
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "FIFOs") || time.Since(started) >= 2*time.Second {
			t.Fatalf("error=%v elapsed=%s", err, time.Since(started))
		}
	})
	t.Run("symlink-no-exfiltration", func(t *testing.T) {
		fx := newFixture(t, nil)
		secret := filepath.Join(fx.root, "outside-secret")
		marker := "EXFILTRATION-MARKER-DO-NOT-READ"
		writePrivate(t, secret, []byte(marker))
		if err := os.Symlink(secret, filepath.Join(fx.workspace, "untracked-link")); err != nil {
			t.Fatal(err)
		}
		_, err := inspectSourceLimited(context.Background(), fx.workspace, fx.limits.MaxMemberBytes, func(phase string) error {
			if phase == "before_diff" {
				t.Fatal("Git diff ran before symlink rejection")
			}
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "symlink") || strings.Contains(err.Error(), marker) {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestRawSourceAggregateLimit(t *testing.T) {
	fx := newFixture(t, nil)
	mutateFile(t, filepath.Join(fx.workspace, "candidate.txt"), bytes.Repeat([]byte("x"), 65))
	if _, err := inspectSourceLimited(context.Background(), fx.workspace, 64, nil); err == nil || !strings.Contains(err.Error(), "exceed") {
		t.Fatalf("error=%v", err)
	}
}

func TestRawSourceMutationAfterCaptureFailsClosed(t *testing.T) {
	fx := newFixture(t, nil)
	_, err := inspectSource(context.Background(), fx.workspace, func(phase string) error {
		if phase == "before_diff" {
			mutateFile(t, filepath.Join(fx.workspace, "candidate.txt"), []byte("changed after raw capture\n"))
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "raw source changed") {
		t.Fatalf("error=%v", err)
	}
}

func TestRawSourceSupportsSHA256ObjectFormatAndRejectsUnknownFormats(t *testing.T) {
	workspace := t.TempDir()
	runGit(t, workspace, "init", "-q", "--object-format=sha256")
	writePrivate(t, filepath.Join(workspace, "tracked.txt"), []byte("baseline\n"))
	runGit(t, workspace, "add", "tracked.txt")
	runGit(t, workspace, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "sha256 baseline")
	mutateFile(t, filepath.Join(workspace, "tracked.txt"), []byte("candidate\n"))
	state, err := InspectSource(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.Claim.Commit) != 64 || !slices.Contains(state.Claim.ChangedFiles, "tracked.txt") || !bytes.Contains(state.Diff, []byte("candidate")) {
		t.Fatalf("state=%+v diff=%q", state.Claim, state.Diff)
	}
	if err := validateObjectIdentity("sha512", strings.Repeat("a", 128)); err == nil || !strings.Contains(err.Error(), "only sha1 and sha256") {
		t.Fatalf("error=%v", err)
	}
}

func TestSourceInspectionRejectsInfoAttributesAndMutation(t *testing.T) {
	t.Run("nonempty", func(t *testing.T) {
		fx := newFixture(t, nil)
		path, err := gitPath(context.Background(), fx.workspace, "info/attributes")
		if err != nil {
			t.Fatal(err)
		}
		writePrivate(t, path, []byte("*.txt binary\n"))
		if _, err := InspectSource(context.Background(), fx.workspace); err == nil || !strings.Contains(err.Error(), "info attributes") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("mutation-during-inspection", func(t *testing.T) {
		fx := newFixture(t, nil)
		path, err := gitPath(context.Background(), fx.workspace, "info/attributes")
		if err != nil {
			t.Fatal(err)
		}
		writePrivate(t, path, nil)
		_, err = inspectSource(context.Background(), fx.workspace, func(phase string) error {
			if phase == "before_diff" {
				mutateFile(t, path, []byte("*.txt binary\n"))
				mutateFile(t, path, nil)
			}
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "info attributes") {
			t.Fatalf("error=%v", err)
		}
	})
	t.Run("drains-decoys-before-write-restore", func(t *testing.T) {
		fx := newFixture(t, nil)
		path, err := gitPath(context.Background(), fx.workspace, "info/attributes")
		if err != nil {
			t.Fatal(err)
		}
		writePrivate(t, path, nil)
		_, err = inspectSource(context.Background(), fx.workspace, func(phase string) error {
			if phase != "before_diff" {
				return nil
			}
			for index := 0; index < 400; index++ {
				decoy := filepath.Join(filepath.Dir(path), fmt.Sprintf("decoy-%03d", index))
				writePrivate(t, decoy, []byte("decoy\n"))
			}
			mutateFile(t, path, []byte("*.txt binary\n"))
			mutateFile(t, path, nil)
			return nil
		})
		if err == nil || !strings.Contains(err.Error(), "info attributes") {
			t.Fatalf("error=%v", err)
		}
	})
}

func TestSourceInspectionRejectsTransientRepositoryConfigMutation(t *testing.T) {
	fx := newFixture(t, nil)
	path, err := gitPath(context.Background(), fx.workspace, "config")
	if err != nil {
		t.Fatal(err)
	}
	original := readFile(t, path)
	_, err = inspectSource(context.Background(), fx.workspace, func(phase string) error {
		if phase == "before_diff" {
			mutateFile(t, path, append(append([]byte(nil), original...), []byte("\n[diff]\n\tcontext = 0\n")...))
			mutateFile(t, path, original)
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "Git config") {
		t.Fatalf("error=%v", err)
	}
}

func TestSourceInspectionRejectsRepositoryConfigIncludes(t *testing.T) {
	fx := newFixture(t, nil)
	included := filepath.Join(fx.root, "included-git-config")
	writePrivate(t, included, []byte("[diff]\n\tcontext = 0\n"))
	runGit(t, fx.workspace, "config", "include.path", included)
	if _, err := InspectSource(context.Background(), fx.workspace); err == nil || !strings.Contains(err.Error(), "includes are unsupported") {
		t.Fatalf("error=%v", err)
	}
}

func TestSourceInspectionUsesLinkedWorktreeInfoAttributesPath(t *testing.T) {
	repository := t.TempDir()
	runGit(t, repository, "init", "-q")
	writePrivate(t, filepath.Join(repository, "tracked.txt"), []byte("baseline\n"))
	runGit(t, repository, "add", "tracked.txt")
	runGit(t, repository, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "baseline")
	linked := filepath.Join(t.TempDir(), "linked")
	runGit(t, repository, "worktree", "add", "-q", "-b", "linked-test", linked)
	mutateFile(t, filepath.Join(linked, "tracked.txt"), []byte("candidate\n"))
	path, err := gitPath(context.Background(), linked, "info/attributes")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	writePrivate(t, path, []byte("*.txt binary\n"))
	if _, err := InspectSource(context.Background(), linked); err == nil || !strings.Contains(err.Error(), "info attributes") {
		t.Fatalf("path=%s error=%v", path, err)
	}
}

func TestCommittedAndWorkingTreeAttributesRemainSourceBound(t *testing.T) {
	fx := newFixture(t, nil)
	writePrivate(t, filepath.Join(fx.workspace, ".gitattributes"), []byte("*.dat binary\n"))
	writePrivate(t, filepath.Join(fx.workspace, "sample.dat"), []byte("baseline\x00data\n"))
	runGit(t, fx.workspace, "add", ".gitattributes", "sample.dat")
	runGit(t, fx.workspace, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "attributes")
	mutateFile(t, filepath.Join(fx.workspace, "sample.dat"), []byte("candidate\x00data\n"))
	committed, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(committed.Diff, []byte("GIT binary patch")) {
		t.Fatalf("committed binary attribute did not control reproducible diff: %q", committed.Diff)
	}
	repeated, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !equalSource(committed.Claim, repeated.Claim) || !bytes.Equal(committed.Diff, repeated.Diff) {
		t.Fatal("committed attributes did not produce repeatable evidence")
	}
	mutateFile(t, filepath.Join(fx.workspace, ".gitattributes"), []byte("*.dat diff\n"))
	working, err := InspectSource(context.Background(), fx.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if equalSource(committed.Claim, working.Claim) || !slices.Contains(working.Claim.ChangedFiles, ".gitattributes") {
		t.Fatalf("working-tree attributes were not source-bound: before=%+v after=%+v", committed.Claim, working.Claim)
	}
}

func TestVerifyPacketRequiresAbsoluteWorkspaceWhenCheckingSource(t *testing.T) {
	limits := Limits{MaxMemberBytes: 4096, MaxPacketBytes: 4096, MaxMembers: 4}
	for _, workspace := range []string{"", "relative/workspace"} {
		_, err := VerifyPacket(context.Background(), "/does/not/matter", VerifyOptions{Workspace: workspace, Limits: limits, ExpectedUID: os.Geteuid()})
		if !errors.Is(err, ErrMalformedPacket) || !strings.Contains(err.Error(), "workspace must be") {
			t.Fatalf("workspace=%q error=%v", workspace, err)
		}
	}
	if err := validateVerifyOptions(VerifyOptions{Limits: limits, ExpectedUID: os.Geteuid(), SkipSource: true}); err != nil {
		t.Fatalf("snapshot verification unexpectedly requires workspace: %v", err)
	}
}

func TestGitConfigInjectionEnvironmentIsRemoved(t *testing.T) {
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.quotePath")
	t.Setenv("GIT_CONFIG_VALUE_0", "false")
	environment := sanitizedGitEnvironment()
	foundNoSystemAttributes := false
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if name == "GIT_CONFIG_COUNT" || name == "GIT_CONFIG_KEY_0" || name == "GIT_CONFIG_VALUE_0" {
			t.Fatalf("ambient Git config survived sanitization: %s", name)
		}
		if entry == "GIT_ATTR_NOSYSTEM=1" {
			foundNoSystemAttributes = true
		}
	}
	if !foundNoSystemAttributes {
		t.Fatal("system Git attributes were not disabled")
	}
}

type getErrorBackend struct {
	Backend
	err error
}

func (b *getErrorBackend) Get(context.Context, string) (domain.Run, error) {
	return domain.Run{}, b.err
}

// TestCandidatePacketEvidence is opt-in because it writes a retained credential-free sample
// outside the repository. The sample binds the real uncommitted candidate diff and exercises
// finalize, independent verify, import, completion, consumption, close, and reopen. Its
// reviewer identities are explicit test fixtures, not an independent release approval.
func TestCandidatePacketEvidence(t *testing.T) {
	evidenceRoot := os.Getenv("DARK_FACTORY_M4_EVIDENCE_ROOT")
	if evidenceRoot == "" {
		t.Skip("set DARK_FACTORY_M4_EVIDENCE_ROOT to retain the candidate packet proof")
	}
	if !filepath.IsAbs(evidenceRoot) {
		t.Fatal("evidence root must be absolute")
	}
	if err := os.Mkdir(evidenceRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"packets", "state"} {
		if err := os.Mkdir(filepath.Join(evidenceRoot, name), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	workspace, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(evidenceRoot, "state", "factory.db")
	databaseFile, err := os.OpenFile(database, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := databaseFile.Close(); err != nil {
		t.Fatal(err)
	}
	backend := openBackend(t, database)
	ctx := context.Background()
	issue := domain.Issue{ID: "ISSUE-1", Identifier: "DF-M4", ProjectID: "PROJECT-1", Title: "M4 candidate packet evidence", State: domain.IssueReady, CreatedAt: time.Now().UTC()}
	if err := backend.EnsureIssue(ctx, issue); err != nil {
		t.Fatal(err)
	}
	controller := factory.New(backend, backend, nil, nil, nil)
	policy := domain.Policy{LeaseDuration: time.Hour, MaxRunDuration: 24 * time.Hour, MaxAttempts: 3, MaxConsecutiveFailures: 2}
	if _, err := controller.Start(ctx, "run-1", issue.ProjectID, issue.ID, "M4 evidence started", policy); err != nil {
		t.Fatal(err)
	}
	lease, err := controller.AcquireLease(ctx, "run-1", "forge-evidence")
	if err != nil {
		t.Fatal(err)
	}
	if err := controller.Checkpoint(ctx, "run-1", lease.Fence, 1, "candidate ready", "M4 common and attack gates passed"); err != nil {
		t.Fatal(err)
	}
	run, err := backend.Get(ctx, "run-1")
	if err != nil {
		t.Fatal(err)
	}
	fx := &fixture{root: evidenceRoot, packetRoot: filepath.Join(evidenceRoot, "packets"), stateRoot: filepath.Join(evidenceRoot, "state"), workspace: workspace,
		database: database, backend: backend, run: run, limits: Limits{MaxMemberBytes: 64 << 20, MaxMembers: 256}}
	fx.store = openEvidenceStore(t, fx, nil)
	fx.store.now = func() time.Time { return time.Now().UTC() }
	packetPath := finalizeFixturePacket(t, fx, func(receipt *ReviewReceipt) {
		receipt.Author = Identity{Provider: "fixture-author-provider", Model: "fixture-author-model"}
		receipt.Reviewer = Identity{Provider: "fixture-reviewer-provider", Model: "fixture-reviewer-model"}
		receipt.Checks = []string{"go mod verify", "go test -count=1 ./...", "go test -race -count=1 ./...", "go vet ./...", "go build ./...", "make smoke", "filesystem attack matrix"}
	}, nil)
	verified, err := VerifyPacket(ctx, packetPath, VerifyOptions{Workspace: workspace, Limits: fx.limits, ExpectedUID: os.Geteuid()})
	if err != nil {
		t.Fatal(err)
	}
	review, err := fx.store.GetReview(ctx, testReviewID)
	if err != nil {
		t.Fatal(err)
	}
	if review.ArtifactSHA256 != digest([]byte("reviewed response\n")) {
		t.Fatalf("review artifact digest=%s", review.ArtifactSHA256)
	}
	controller = factory.New(fx.store, backend, nil, fx.store, fx.store)
	if err := controller.BindReview(ctx, run.ID, lease.Fence, testReviewID); err != nil {
		t.Fatal(err)
	}
	completed, err := controller.CompleteAndAdvance(ctx, run.ID, lease.Fence, "retained M4 candidate packet approved by test fixture")
	if err != nil || completed.Status != domain.RunComplete {
		t.Fatalf("completed=%+v error=%v", completed, err)
	}
	if err := fx.store.Close(); err != nil {
		t.Fatal(err)
	}
	fx.backend = openBackend(t, database)
	fx.store = openEvidenceStore(t, fx, nil)
	reopened, err := fx.store.GetReview(ctx, testReviewID)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.ConsumedByRun != run.ID {
		t.Fatalf("consumer=%q", reopened.ConsumedByRun)
	}
	receipts := regularNames(t, filepath.Join(fx.stateRoot, "review-receipts"))
	snapshots := regularNames(t, filepath.Join(fx.stateRoot, "review-packets"))
	summary := struct {
		BaseCommit          string   `json:"base_commit"`
		PacketDigest        string   `json:"packet_digest"`
		SourceDigest        string   `json:"source_digest"`
		ChangedFiles        []string `json:"changed_files"`
		SnapshotCount       int      `json:"snapshot_count"`
		ConsumptionReceipts int      `json:"consumption_receipts"`
		ConsumedByRun       string   `json:"consumed_by_run"`
		Outcome             string   `json:"outcome"`
		ReviewerNotice      string   `json:"reviewer_notice"`
	}{verified.Manifest.Source.Commit, verified.Digest, verified.SourceDigest, verified.Manifest.Source.ChangedFiles, len(snapshots), len(receipts), reopened.ConsumedByRun,
		"candidate_packet_verified_imported_consumed_and_reopened", "fixture identities only; independent owner review remains required"}
	encoded, err := json.MarshalIndent(summary, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(evidenceRoot, "summary.json"), append(encoded, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%s", encoded)
	if err := fx.store.Close(); err != nil {
		t.Fatal(err)
	}
}
