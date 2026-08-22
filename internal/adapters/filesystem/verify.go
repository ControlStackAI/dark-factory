//go:build linux

package filesystem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

type VerifyOptions struct {
	Workspace   string
	Limits      Limits
	Expected    *Expected
	ExpectedUID int
	SkipSource  bool
	BeforeRead  func(string) error
}

func VerifyPacket(ctx context.Context, packetPath string, options VerifyOptions) (VerifiedPacket, error) {
	options.Limits = normalizedLimits(options.Limits)
	if err := validateVerifyOptions(options); err != nil {
		return VerifiedPacket{}, err
	}
	directory, err := openSecureDirectory(packetPath, options.ExpectedUID)
	if err != nil {
		return VerifiedPacket{}, fmt.Errorf("%w: open packet: %w", ErrMalformedPacket, err)
	}
	defer directory.Close()
	return verifyDirectory(ctx, directory, filepath.Base(packetPath), packetPath, options)
}

func validateVerifyOptions(options VerifyOptions) error {
	if options.ExpectedUID < 0 {
		return fmt.Errorf("%w: invalid expected uid", ErrMalformedPacket)
	}
	if !options.SkipSource && (options.Workspace == "" || !filepath.IsAbs(options.Workspace)) {
		return fmt.Errorf("%w: workspace must be a non-empty absolute path when source verification is enabled", ErrMalformedPacket)
	}
	if options.Limits.MaxMemberBytes <= 0 || options.Limits.MaxPacketBytes < options.Limits.MaxMemberBytes || options.Limits.MaxMembers < 4 {
		return fmt.Errorf("%w: invalid packet limits", ErrMalformedPacket)
	}
	return nil
}

func verifyDirectory(ctx context.Context, directory *secureDirectory, base, packetPath string, options VerifyOptions) (VerifiedPacket, error) {
	statBefore, err := directory.stat()
	if err != nil {
		return VerifiedPacket{}, err
	}
	namesBefore, err := directory.names(options.Limits.MaxMembers + 1)
	if err != nil {
		return VerifiedPacket{}, err
	}
	if options.BeforeRead != nil {
		if err := options.BeforeRead(ManifestName); err != nil {
			return VerifiedPacket{}, err
		}
	}
	manifestBytes, _, err := directory.readRegular(ManifestName, min64(options.Limits.MaxMemberBytes, 4<<20))
	if err != nil {
		return VerifiedPacket{}, err
	}
	var manifest Manifest
	if err := strictCanonicalJSON(manifestBytes, &manifest, func() ([]byte, error) { return CanonicalManifest(manifest) }); err != nil {
		return VerifiedPacket{}, fmt.Errorf("%w: manifest: %v", ErrMalformedPacket, err)
	}
	if len(manifest.Members) > options.Limits.MaxMembers {
		return VerifiedPacket{}, fmt.Errorf("%w: member count exceeds limit", ErrMalformedPacket)
	}
	totalBytes := int64(len(manifestBytes))
	for _, member := range manifest.Members {
		if member.Size > options.Limits.MaxMemberBytes || member.Size > options.Limits.MaxPacketBytes-totalBytes {
			return VerifiedPacket{}, fmt.Errorf("%w: packet byte limit exceeded", ErrMalformedPacket)
		}
		totalBytes += member.Size
	}
	digestValue := PacketDigest(manifestBytes)
	if !strings.HasPrefix(base, ".pending-") && base != "packet-"+digestValue {
		return VerifiedPacket{}, fmt.Errorf("%w: content-addressed packet name does not match manifest", ErrMalformedPacket)
	}
	wantNames := append([]string{ManifestName}, sortedMemberPaths(manifest.Members)...)
	sort.Strings(wantNames)
	if !equalStrings(namesBefore, wantNames) {
		return VerifiedPacket{}, fmt.Errorf("%w: packet membership differs from manifest", ErrMalformedPacket)
	}
	if options.Expected != nil {
		if manifest.ReviewID != options.Expected.ReviewID || manifest.ProjectID != options.Expected.ProjectID || manifest.IssueID != options.Expected.IssueID || manifest.RunID != options.Expected.RunID || manifest.CheckpointSequence != options.Expected.CheckpointSequence {
			return VerifiedPacket{}, fmt.Errorf("%w: packet target does not match wanted review", ErrMalformedPacket)
		}
	}
	packet := VerifiedPacket{Path: packetPath, Digest: digestValue, Manifest: manifest, Bytes: map[string][]byte{ManifestName: append([]byte(nil), manifestBytes...)}}
	var reviewMember *Member
	var sourceDiffMember *Member
	for index := range manifest.Members {
		member := manifest.Members[index]
		if member.Size > options.Limits.MaxMemberBytes {
			return VerifiedPacket{}, fmt.Errorf("%w: member %q exceeds configured size", ErrMalformedPacket, member.Path)
		}
		if options.BeforeRead != nil {
			if err := options.BeforeRead(member.Path); err != nil {
				return VerifiedPacket{}, err
			}
		}
		data, _, err := directory.readRegular(member.Path, options.Limits.MaxMemberBytes)
		if err != nil {
			return VerifiedPacket{}, err
		}
		if int64(len(data)) != member.Size || digest(data) != member.SHA256 {
			return VerifiedPacket{}, fmt.Errorf("%w: size or digest mismatch for %q", ErrMalformedPacket, member.Path)
		}
		packet.Bytes[member.Path] = append([]byte(nil), data...)
		switch member.Kind {
		case "review_receipt":
			reviewMember = &manifest.Members[index]
		case "source_diff":
			sourceDiffMember = &manifest.Members[index]
		}
	}
	namesAfter, err := directory.names(options.Limits.MaxMembers + 1)
	if err != nil || !equalStrings(namesBefore, namesAfter) {
		return VerifiedPacket{}, fmt.Errorf("%w: packet membership changed during import", ErrMalformedPacket)
	}
	statAfter, err := directory.stat()
	if err != nil || !sameStat(statBefore, statAfter) {
		return VerifiedPacket{}, fmt.Errorf("%w: packet directory changed during import", ErrMalformedPacket)
	}
	if err := directory.validateSelf(); err != nil {
		return VerifiedPacket{}, err
	}
	if reviewMember == nil || sourceDiffMember == nil {
		return VerifiedPacket{}, fmt.Errorf("%w: required receipt members are missing", ErrMalformedPacket)
	}
	var review ReviewReceipt
	reviewBytes := packet.Bytes[reviewMember.Path]
	if err := strictCanonicalJSON(reviewBytes, &review, func() ([]byte, error) { return CanonicalReviewReceipt(review) }); err != nil {
		return VerifiedPacket{}, fmt.Errorf("%w: review receipt: %v", ErrMalformedPacket, err)
	}
	sourceDigestValue, err := SourceDigest(manifest.Source)
	if err != nil {
		return VerifiedPacket{}, err
	}
	if err := bindReview(manifest, review, sourceDigestValue); err != nil {
		return VerifiedPacket{}, err
	}
	artifactMember, ok := memberByPath(manifest.Members, review.ArtifactPath)
	if !ok || artifactMember.Kind != "artifact" || artifactMember.SHA256 != review.ArtifactSHA256 {
		return VerifiedPacket{}, fmt.Errorf("%w: reviewed artifact binding does not name a claimed artifact", ErrMalformedPacket)
	}
	if sourceDiffMember.SHA256 != manifest.Source.DiffSHA256 {
		return VerifiedPacket{}, fmt.Errorf("%w: source diff member does not match source claim", ErrMalformedPacket)
	}
	if !options.SkipSource {
		state, err := inspectSourceLimited(ctx, options.Workspace, options.Limits.MaxMemberBytes, nil)
		if err != nil {
			return VerifiedPacket{}, fmt.Errorf("%w: inspect source: %v", ErrMalformedPacket, err)
		}
		if !equalSource(state.Claim, manifest.Source) || !bytes.Equal(state.Diff, packet.Bytes[sourceDiffMember.Path]) {
			return VerifiedPacket{}, fmt.Errorf("%w: independently recomputed source commit, diff, or changed files differ", ErrMalformedPacket)
		}
	}
	packet.SourceDigest = sourceDigestValue
	packet.Review = review
	packet.ArtifactMember = artifactMember
	return packet, nil
}

func FinalizePacket(ctx context.Context, packetRoot, pendingPath, workspace string, limits Limits) (string, VerifiedPacket, error) {
	limits = normalizedLimits(limits)
	options := VerifyOptions{Workspace: workspace, Limits: limits, ExpectedUID: os.Geteuid()}
	if err := validateVerifyOptions(options); err != nil {
		return "", VerifiedPacket{}, err
	}
	root, err := filepath.Abs(packetRoot)
	if err != nil {
		return "", VerifiedPacket{}, err
	}
	pending, err := filepath.Abs(pendingPath)
	if err != nil {
		return "", VerifiedPacket{}, err
	}
	pendingName := filepath.Base(pending)
	if filepath.Dir(pending) != root || !strings.HasPrefix(pendingName, ".pending-") || len(pendingName) > 160 || strings.ContainsAny(pendingName, `/\\`) {
		return "", VerifiedPacket{}, fmt.Errorf("%w: pending packet must be a direct .pending-* child", ErrMalformedPacket)
	}
	directory, err := openSecureDirectory(root, os.Geteuid())
	if err != nil {
		return "", VerifiedPacket{}, err
	}
	defer directory.Close()
	pendingDirectory, err := openSecureDirectoryAt(directory, pendingName, os.Geteuid())
	if err != nil {
		return "", VerifiedPacket{}, err
	}
	packet, err := verifyDirectory(ctx, pendingDirectory, pendingName, pending, options)
	if closeErr := pendingDirectory.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	if err != nil {
		return "", VerifiedPacket{}, err
	}
	finalName := "packet-" + packet.Digest
	if err := unix.Renameat2(directory.FD(), filepath.Base(pending), directory.FD(), finalName, unix.RENAME_NOREPLACE); err != nil {
		if errors.Is(err, unix.EEXIST) {
			return "", VerifiedPacket{}, os.ErrExist
		}
		return "", VerifiedPacket{}, err
	}
	if err := unix.Fsync(directory.FD()); err != nil {
		return "", VerifiedPacket{}, err
	}
	finalPath := filepath.Join(root, finalName)
	finalDirectory, err := openSecureDirectoryAt(directory, finalName, os.Geteuid())
	if err != nil {
		return finalPath, VerifiedPacket{}, err
	}
	verified, err := verifyDirectory(ctx, finalDirectory, finalName, finalPath, options)
	if closeErr := finalDirectory.Close(); err == nil && closeErr != nil {
		err = closeErr
	}
	return finalPath, verified, err
}

func normalizedLimits(limits Limits) Limits {
	if limits.MaxPacketBytes == 0 {
		limits.MaxPacketBytes = limits.MaxMemberBytes
	}
	return limits
}

func bindReview(manifest Manifest, review ReviewReceipt, sourceDigest string) error {
	if review.ReviewID != manifest.ReviewID || review.ProjectID != manifest.ProjectID || review.IssueID != manifest.IssueID || review.RunID != manifest.RunID || review.CheckpointSequence != manifest.CheckpointSequence || review.SourceCommit != manifest.Source.Commit || review.SourceDigest != sourceDigest {
		return fmt.Errorf("%w: review receipt target/source binding differs from manifest", ErrMalformedPacket)
	}
	return nil
}

func memberByPath(members []Member, path string) (Member, bool) {
	for _, member := range members {
		if member.Path == path {
			return member, true
		}
	}
	return Member{}, false
}

func equalSource(left, right SourceClaim) bool {
	return left.Commit == right.Commit && left.DiffSHA256 == right.DiffSHA256 && equalStrings(left.ChangedFiles, right.ChangedFiles)
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}

func min64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}
