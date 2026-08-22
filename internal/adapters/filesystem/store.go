//go:build linux

package filesystem

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
)

type Backend interface {
	ports.RunStore
	GetReview(context.Context, string) (domain.ReviewEvidence, error)
	ConsumeReview(context.Context, string, string, string) error
	SHA256(context.Context, string) (string, error)
	EnsureArtifact(context.Context, string, []byte) (string, error)
	EnsureReview(context.Context, domain.ReviewEvidence) error
	Close() error
}

type Hook func(string) error

type Options struct {
	PacketRoot    string
	StateRoot     string
	WorkspaceRoot string
	Limits        Limits
	Backend       Backend
	ExpectedUID   int
	Hook          Hook
	Now           func() time.Time
}

type Store struct {
	mu            sync.Mutex
	packetRoot    string
	snapshotRoot  string
	receiptRoot   string
	workspaceRoot string
	limits        Limits
	backend       Backend
	uid           int
	hook          Hook
	now           func() time.Time
}

func Open(options Options) (*Store, error) {
	options.Limits = normalizedLimits(options.Limits)
	if options.Backend == nil || options.PacketRoot == "" || options.StateRoot == "" || options.WorkspaceRoot == "" || options.Limits.MaxMemberBytes <= 0 || options.Limits.MaxPacketBytes < options.Limits.MaxMemberBytes || options.Limits.MaxMembers < 4 {
		return nil, errors.New("invalid filesystem evidence options")
	}
	if options.ExpectedUID == 0 && os.Geteuid() != 0 {
		options.ExpectedUID = os.Geteuid()
	}
	if options.ExpectedUID < 0 {
		return nil, errors.New("invalid filesystem evidence owner")
	}
	if options.Now == nil {
		options.Now = func() time.Time { return time.Now().UTC() }
	}
	packetRoot, err := filepath.Abs(options.PacketRoot)
	if err != nil {
		return nil, err
	}
	stateRoot, err := filepath.Abs(options.StateRoot)
	if err != nil {
		return nil, err
	}
	workspace, err := filepath.Abs(options.WorkspaceRoot)
	if err != nil {
		return nil, err
	}
	if err := ensurePrivateRoot(packetRoot, options.ExpectedUID, false); err != nil {
		return nil, fmt.Errorf("review packet root: %w", err)
	}
	if err := ensurePrivateRoot(stateRoot, options.ExpectedUID, false); err != nil {
		return nil, fmt.Errorf("state root: %w", err)
	}
	snapshotRoot := filepath.Join(stateRoot, "review-packets")
	receiptRoot := filepath.Join(stateRoot, "review-receipts")
	for _, root := range []string{snapshotRoot, receiptRoot} {
		if err := ensurePrivateRoot(root, options.ExpectedUID, true); err != nil {
			return nil, err
		}
	}
	return &Store{packetRoot: packetRoot, snapshotRoot: snapshotRoot, receiptRoot: receiptRoot, workspaceRoot: workspace,
		limits: options.Limits, backend: options.Backend, uid: options.ExpectedUID, hook: options.Hook, now: options.Now}, nil
}

func (s *Store) Close() error { return s.backend.Close() }

func (s *Store) Create(ctx context.Context, run domain.Run) error {
	return s.backend.Create(ctx, run)
}

func (s *Store) Get(ctx context.Context, id string) (domain.Run, error) {
	return s.backend.Get(ctx, id)
}

func (s *Store) CompareAndSwap(ctx context.Context, id string, version uint64, run domain.Run) error {
	return s.backend.CompareAndSwap(ctx, id, version, run)
}

func (s *Store) GetReview(ctx context.Context, id string) (domain.ReviewEvidence, error) {
	if err := s.callHook("before_review_import"); err != nil {
		return domain.ReviewEvidence{}, err
	}
	review, err := s.getReview(ctx, id)
	if err != nil {
		return domain.ReviewEvidence{}, err
	}
	if err := s.callHook("after_review_import"); err != nil {
		return domain.ReviewEvidence{}, err
	}
	return review, nil
}

func (s *Store) getReview(ctx context.Context, id string) (domain.ReviewEvidence, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	packet, err := s.findSnapshot(ctx, id)
	if errors.Is(err, ports.ErrNotFound) {
		packet, err = s.findSubmitted(ctx, id)
		if err != nil {
			return domain.ReviewEvidence{}, err
		}
		if err := s.installSnapshot(ctx, packet); err != nil {
			return domain.ReviewEvidence{}, err
		}
		packet.Path = filepath.Join(s.snapshotRoot, "packet-"+packet.Digest)
	}
	if err != nil {
		return domain.ReviewEvidence{}, err
	}
	ref := artifactReference(packet.Digest, packet.ArtifactMember.Path)
	review := domain.ReviewEvidence{ID: packet.Manifest.ReviewID, ProjectID: packet.Manifest.ProjectID, IssueID: packet.Manifest.IssueID,
		Status: domain.ReviewApproved, Immutable: true, ArtifactRef: ref, ArtifactSHA256: packet.ArtifactMember.SHA256}
	if stored, getErr := s.backend.GetReview(ctx, id); getErr == nil {
		if stored.ID != review.ID || stored.ProjectID != review.ProjectID || stored.IssueID != review.IssueID || stored.Status != review.Status || !stored.Immutable || stored.ArtifactRef != review.ArtifactRef || stored.ArtifactSHA256 != review.ArtifactSHA256 {
			return domain.ReviewEvidence{}, fmt.Errorf("%w: durable review differs from immutable packet", ports.ErrConflict)
		}
		return stored, nil
	} else if !errors.Is(getErr, ports.ErrNotFound) {
		return domain.ReviewEvidence{}, getErr
	}
	if digestValue, err := s.backend.EnsureArtifact(ctx, ref, packet.Bytes[packet.ArtifactMember.Path]); err != nil {
		return domain.ReviewEvidence{}, err
	} else if digestValue != review.ArtifactSHA256 {
		return domain.ReviewEvidence{}, fmt.Errorf("%w: durable snapshot artifact digest differs", ports.ErrConflict)
	}
	if err := s.backend.EnsureReview(ctx, review); err != nil {
		return domain.ReviewEvidence{}, err
	}
	return s.backend.GetReview(ctx, id)
}

func (s *Store) ConsumeReview(ctx context.Context, id, runID, artifactSHA256 string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	packet, err := s.findSnapshot(ctx, id)
	if err != nil {
		return err
	}
	if packet.Manifest.RunID != runID || packet.ArtifactMember.SHA256 != artifactSHA256 {
		return ports.ErrConflict
	}
	run, err := s.backend.Get(ctx, runID)
	if err != nil {
		return err
	}
	if run.ProjectID != packet.Manifest.ProjectID || run.IssueID != packet.Manifest.IssueID || run.CheckpointSequence != packet.Manifest.CheckpointSequence || run.Lease.Fence == 0 {
		return ports.ErrConflict
	}
	durableReview, err := s.backend.GetReview(ctx, id)
	if err != nil {
		return err
	}
	receiptName := digest([]byte(id)) + ".json"
	intentName := "pending-" + receiptName
	if existing, readErr := s.readConsumption(receiptName); readErr == nil {
		if durableReview.ConsumedByRun != runID || !matchesConsumption(existing, packet, runID, "approved_and_consumed") || existing.Fence > run.NextFence {
			return fmt.Errorf("%w: consumption receipt conflicts with durable review", ports.ErrConflict)
		}
		return removeAtomicFile(s.receiptRoot, intentName, s.uid)
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return readErr
	}
	intent, readErr := s.readConsumption(intentName)
	if readErr == nil {
		if !matchesConsumption(intent, packet, runID, "pending_consumption") || intent.Fence > run.Lease.Fence {
			return fmt.Errorf("%w: consumption intent conflicts with durable review", ports.ErrConflict)
		}
		if durableReview.ConsumedByRun == "" && intent.Fence < run.Lease.Fence {
			if err := removeAtomicFile(s.receiptRoot, intentName, s.uid); err != nil {
				return err
			}
			readErr = os.ErrNotExist
		}
	}
	if errors.Is(readErr, os.ErrNotExist) {
		if durableReview.ConsumedByRun != "" {
			return fmt.Errorf("%w: consumed review has neither receipt nor durable consumption intent", ErrMalformedPacket)
		}
		intent = ConsumptionReceipt{ReceiptVersion: ReceiptVersion, ReviewID: id, PacketDigest: packet.Digest, SourceDigest: packet.SourceDigest,
			RunID: runID, Fence: run.Lease.Fence, ConsumedAt: s.now().UTC().Format(time.RFC3339Nano), Outcome: "pending_consumption"}
		contents, encodeErr := canonicalConsumption(intent)
		if encodeErr != nil {
			return encodeErr
		}
		if err := s.callHook("before_consumption_intent"); err != nil {
			return err
		}
		if err := writeAtomicFile(s.receiptRoot, intentName, contents, s.uid); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		intent, readErr = s.readConsumption(intentName)
	}
	if readErr != nil {
		return readErr
	}
	if !matchesConsumption(intent, packet, runID, "pending_consumption") || intent.Fence > run.Lease.Fence || (durableReview.ConsumedByRun == "" && intent.Fence != run.Lease.Fence) {
		return fmt.Errorf("%w: consumption intent conflicts with durable review", ports.ErrConflict)
	}
	if err := s.callHook("after_consumption_intent"); err != nil {
		return err
	}
	if durableReview.ConsumedByRun != "" && durableReview.ConsumedByRun != runID {
		return ports.ErrConflict
	}
	if err := s.backend.ConsumeReview(ctx, id, runID, artifactSHA256); err != nil {
		return err
	}
	receipt := intent
	receipt.Outcome = "approved_and_consumed"
	contents, err := canonicalConsumption(receipt)
	if err != nil {
		return err
	}
	if err := s.callHook("before_consumption_receipt"); err != nil {
		return err
	}
	if err := writeAtomicFile(s.receiptRoot, receiptName, contents, s.uid); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, readErr := s.readConsumption(receiptName)
		if readErr != nil || !matchesConsumption(existing, packet, runID, "approved_and_consumed") || existing.Fence > run.NextFence {
			return fmt.Errorf("%w: existing consumption receipt differs", ports.ErrConflict)
		}
	}
	written, err := s.readConsumption(receiptName)
	if err != nil || written.ReviewID != receipt.ReviewID || written.PacketDigest != receipt.PacketDigest || written.SourceDigest != receipt.SourceDigest || written.RunID != receipt.RunID || written.Fence != receipt.Fence || written.ConsumedAt != receipt.ConsumedAt || written.Outcome != receipt.Outcome {
		return fmt.Errorf("%w: committed consumption receipt differs", ErrMalformedPacket)
	}
	if err := s.callHook("after_consumption_receipt"); err != nil {
		return err
	}
	return removeAtomicFile(s.receiptRoot, intentName, s.uid)
}

func (s *Store) SHA256(ctx context.Context, ref string) (string, error) {
	packetDigest, member, ok := parseArtifactReference(ref)
	if !ok {
		return "", ports.ErrNotFound
	}
	packet, err := VerifyPacket(ctx, filepath.Join(s.snapshotRoot, "packet-"+packetDigest), VerifyOptions{Limits: s.limits, ExpectedUID: s.uid, SkipSource: true})
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", ports.ErrNotFound
		}
		return "", err
	}
	claimed, found := memberByPath(packet.Manifest.Members, member)
	if !found || claimed.Kind != "artifact" {
		return "", ports.ErrNotFound
	}
	return claimed.SHA256, nil
}

func (s *Store) installSnapshot(ctx context.Context, packet VerifiedPacket) error {
	if err := s.callHook("before_snapshot_source_check"); err != nil {
		return err
	}
	state, err := inspectSourceLimited(ctx, s.workspaceRoot, s.limits.MaxMemberBytes, nil)
	if err != nil || !equalSource(state.Claim, packet.Manifest.Source) {
		return fmt.Errorf("%w: source changed before immutable snapshot", ErrMalformedPacket)
	}
	if err := s.callHook("before_snapshot_commit"); err != nil {
		return err
	}
	fresh, err := VerifyPacket(ctx, packet.Path, VerifyOptions{Workspace: s.workspaceRoot, Limits: s.limits, ExpectedUID: s.uid})
	if err != nil || fresh.Digest != packet.Digest || len(fresh.Bytes) != len(packet.Bytes) {
		return fmt.Errorf("%w: submitted packet changed before immutable snapshot", ErrMalformedPacket)
	}
	for name, contents := range packet.Bytes {
		if !sameBytes(contents, fresh.Bytes[name]) {
			return fmt.Errorf("%w: submitted member %q changed before immutable snapshot", ErrMalformedPacket, name)
		}
	}
	finalName := "packet-" + packet.Digest
	beforeCommit := func() error {
		if err := s.callHook("before_snapshot_install"); err != nil {
			return err
		}
		state, err := inspectSourceLimited(ctx, s.workspaceRoot, s.limits.MaxMemberBytes, nil)
		if err != nil || !equalSource(state.Claim, packet.Manifest.Source) {
			return fmt.Errorf("%w: source changed while immutable snapshot was staged", ErrMalformedPacket)
		}
		return nil
	}
	if err := writeTreeAtomic(s.snapshotRoot, finalName, packet.Bytes, s.uid, beforeCommit); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
		existing, verifyErr := VerifyPacket(ctx, filepath.Join(s.snapshotRoot, finalName), VerifyOptions{Limits: s.limits, ExpectedUID: s.uid, SkipSource: true})
		if verifyErr != nil || existing.Digest != packet.Digest {
			return fmt.Errorf("%w: existing snapshot is corrupt", ports.ErrConflict)
		}
	}
	snapshotPath := filepath.Join(s.snapshotRoot, finalName)
	committed, err := VerifyPacket(ctx, snapshotPath, VerifyOptions{Limits: s.limits, ExpectedUID: s.uid, SkipSource: true})
	if err != nil || committed.Digest != packet.Digest || committed.SourceDigest != packet.SourceDigest || len(committed.Bytes) != len(packet.Bytes) {
		return fmt.Errorf("%w: committed snapshot failed verification", ErrMalformedPacket)
	}
	for name, contents := range packet.Bytes {
		if !sameBytes(contents, committed.Bytes[name]) {
			return fmt.Errorf("%w: committed snapshot member %q differs", ErrMalformedPacket, name)
		}
	}
	return s.callHook("after_snapshot_commit")
}

func (s *Store) findSubmitted(ctx context.Context, reviewID string) (VerifiedPacket, error) {
	return s.findPacket(ctx, s.packetRoot, reviewID, false)
}

func (s *Store) findSnapshot(ctx context.Context, reviewID string) (VerifiedPacket, error) {
	return s.findPacket(ctx, s.snapshotRoot, reviewID, true)
}

func (s *Store) findPacket(ctx context.Context, root, reviewID string, snapshot bool) (VerifiedPacket, error) {
	directory, err := openSecureDirectory(root, s.uid)
	if err != nil {
		return VerifiedPacket{}, err
	}
	defer directory.Close()
	names, err := directory.names(s.limits.MaxMembers*4 + 1024)
	if err != nil {
		return VerifiedPacket{}, err
	}
	var matches []VerifiedPacket
	for _, name := range names {
		if strings.HasPrefix(name, ".pending-") {
			continue
		}
		if !strings.HasPrefix(name, "packet-") || len(name) != len("packet-")+64 || !validDigest(strings.TrimPrefix(name, "packet-")) {
			return VerifiedPacket{}, fmt.Errorf("%w: unexpected finalized entry %q", ErrMalformedPacket, name)
		}
		packetPath := filepath.Join(root, name)
		packetDirectory, openErr := openSecureDirectoryAt(directory, name, s.uid)
		if openErr != nil {
			return VerifiedPacket{}, fmt.Errorf("%w: open finalized packet %q: %v", ErrMalformedPacket, name, openErr)
		}
		if !snapshot {
			claimedReviewID, attributed := packetReviewID(packetDirectory, s.limits)
			if !attributed || claimedReviewID != reviewID {
				if closeErr := packetDirectory.Close(); closeErr != nil {
					return VerifiedPacket{}, closeErr
				}
				continue
			}
		}
		packet, verifyErr := verifyDirectory(ctx, packetDirectory, name, packetPath, VerifyOptions{Workspace: s.workspaceRoot, Limits: s.limits, ExpectedUID: s.uid, SkipSource: true,
			BeforeRead: func(member string) error { return s.callHook("before_read:" + member) }})
		if closeErr := packetDirectory.Close(); verifyErr == nil && closeErr != nil {
			verifyErr = closeErr
		}
		if verifyErr != nil {
			return VerifiedPacket{}, verifyErr
		}
		if !snapshot {
			packetDirectory, openErr = openSecureDirectoryAt(directory, name, s.uid)
			if openErr != nil {
				return VerifiedPacket{}, fmt.Errorf("%w: reopen finalized packet %q: %v", ErrMalformedPacket, name, openErr)
			}
			packet, verifyErr = verifyDirectory(ctx, packetDirectory, name, packetPath, VerifyOptions{Workspace: s.workspaceRoot, Limits: s.limits, ExpectedUID: s.uid,
				BeforeRead: func(member string) error { return s.callHook("before_read:" + member) }})
			if closeErr := packetDirectory.Close(); verifyErr == nil && closeErr != nil {
				verifyErr = closeErr
			}
			if verifyErr != nil {
				return VerifiedPacket{}, verifyErr
			}
		}
		if packet.Manifest.ReviewID != reviewID && snapshot {
			continue
		}
		if packet.Manifest.ReviewID != reviewID {
			return VerifiedPacket{}, fmt.Errorf("%w: packet review attribution changed during verification", ErrMalformedPacket)
		}
		run, runErr := s.backend.Get(ctx, packet.Manifest.RunID)
		if runErr != nil {
			if errors.Is(runErr, ports.ErrNotFound) {
				return VerifiedPacket{}, fmt.Errorf("%w: packet references unavailable run", ErrMalformedPacket)
			}
			return VerifiedPacket{}, fmt.Errorf("resolve packet durable run: %w", runErr)
		}
		expected := Expected{ReviewID: reviewID, ProjectID: run.ProjectID, IssueID: run.IssueID, RunID: run.ID, CheckpointSequence: run.CheckpointSequence}
		if packet.Manifest.ProjectID != expected.ProjectID || packet.Manifest.IssueID != expected.IssueID || packet.Manifest.RunID != expected.RunID || packet.Manifest.CheckpointSequence != expected.CheckpointSequence {
			return VerifiedPacket{}, fmt.Errorf("%w: packet does not match durable run/checkpoint", ErrMalformedPacket)
		}
		matches = append(matches, packet)
	}
	if len(matches) == 0 {
		return VerifiedPacket{}, ports.ErrNotFound
	}
	if len(matches) != 1 {
		return VerifiedPacket{}, fmt.Errorf("%w: duplicate finalized packets for review %q", ErrMalformedPacket, reviewID)
	}
	return matches[0], nil
}

// packetReviewID performs only a bounded attribution probe. An unattributable packet is
// never approval evidence; findPacket ignores it so corrupt evidence that cannot claim the
// wanted review cannot deny availability to a separate healthy review.
func packetReviewID(directory *secureDirectory, limits Limits) (string, bool) {
	contents, _, err := directory.readRegular(ManifestName, min64(limits.MaxMemberBytes, 4<<20))
	if err != nil {
		return "", false
	}
	if structureErr := rejectDuplicateJSONKeys(contents); structureErr != nil && strings.Contains(structureErr.Error(), "nesting") {
		return "", false
	}
	decoder := json.NewDecoder(bytes.NewReader(contents))
	token, err := decoder.Token()
	if err != nil || token != json.Delim('{') {
		return "", false
	}
	var rawReviewID json.RawMessage
	for decoder.More() {
		keyToken, err := decoder.Token()
		key, ok := keyToken.(string)
		if err != nil || !ok {
			return "", false
		}
		var raw json.RawMessage
		if err := decoder.Decode(&raw); err != nil {
			return "", false
		}
		if key == "review_id" {
			if rawReviewID != nil {
				return "", false
			}
			rawReviewID = raw
		}
	}
	if _, err := decoder.Token(); err != nil {
		return "", false
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return "", false
	}
	var reviewID string
	if len(rawReviewID) == 0 || json.Unmarshal(rawReviewID, &reviewID) != nil || strings.TrimSpace(reviewID) == "" || strings.TrimSpace(reviewID) != reviewID || strings.ContainsRune(reviewID, 0) {
		return "", false
	}
	return reviewID, true
}

func (s *Store) readConsumption(name string) (ConsumptionReceipt, error) {
	directory, err := openSecureDirectory(s.receiptRoot, s.uid)
	if err != nil {
		return ConsumptionReceipt{}, err
	}
	defer directory.Close()
	contents, _, err := directory.readRegular(name, 16<<10)
	if err != nil {
		return ConsumptionReceipt{}, err
	}
	var receipt ConsumptionReceipt
	if err := strictCanonicalJSON(contents, &receipt, func() ([]byte, error) { return canonicalConsumption(receipt) }); err != nil {
		return ConsumptionReceipt{}, fmt.Errorf("%w: corrupt consumption receipt: %v", ErrMalformedPacket, err)
	}
	return receipt, nil
}

func matchesConsumption(receipt ConsumptionReceipt, packet VerifiedPacket, runID, outcome string) bool {
	return receipt.ReviewID == packet.Manifest.ReviewID && receipt.PacketDigest == packet.Digest && receipt.SourceDigest == packet.SourceDigest && receipt.RunID == runID && receipt.Outcome == outcome
}

func (s *Store) callHook(phase string) error {
	if s.hook == nil {
		return nil
	}
	return s.hook(phase)
}

func artifactReference(packetDigest, member string) string {
	return "packet:" + packetDigest + ":" + member
}

func parseArtifactReference(ref string) (string, string, bool) {
	parts := strings.Split(ref, ":")
	if len(parts) != 3 || parts[0] != "packet" || !validDigest(parts[1]) || !safeMember(parts[2]) {
		return "", "", false
	}
	return parts[1], parts[2], true
}

func sameBytes(left, right []byte) bool { return bytes.Equal(left, right) }
