package filesystem

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

const (
	PacketVersion         = 1
	ReceiptVersion        = 1
	ManifestName          = "manifest.json"
	maxCanonicalJSONDepth = 64
)

var (
	ErrMalformedPacket = errors.New("malformed review packet")
	safeMemberName     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)
	validObjectID      = regexp.MustCompile(`^[0-9a-f]{40,64}$`)
)

type Identity struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
}

type SourceClaim struct {
	Commit       string   `json:"commit"`
	DiffSHA256   string   `json:"diff_sha256"`
	ChangedFiles []string `json:"changed_files"`
}

type Member struct {
	Path   string `json:"path"`
	Kind   string `json:"kind"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

type Manifest struct {
	PacketVersion      int         `json:"packet_version"`
	ReviewID           string      `json:"review_id"`
	ProjectID          string      `json:"project_id"`
	IssueID            string      `json:"issue_id"`
	RunID              string      `json:"run_id"`
	CheckpointSequence uint64      `json:"checkpoint_sequence"`
	Source             SourceClaim `json:"source"`
	Members            []Member    `json:"members"`
}

type ReviewReceipt struct {
	ReceiptVersion     int      `json:"receipt_version"`
	ReviewID           string   `json:"review_id"`
	ProjectID          string   `json:"project_id"`
	IssueID            string   `json:"issue_id"`
	RunID              string   `json:"run_id"`
	CheckpointSequence uint64   `json:"checkpoint_sequence"`
	SourceCommit       string   `json:"source_commit"`
	SourceDigest       string   `json:"source_digest"`
	ArtifactPath       string   `json:"artifact_path"`
	ArtifactSHA256     string   `json:"artifact_sha256"`
	Verdict            string   `json:"verdict"`
	Checks             []string `json:"checks"`
	Author             Identity `json:"author"`
	Reviewer           Identity `json:"reviewer"`
}

type ConsumptionReceipt struct {
	ReceiptVersion int    `json:"receipt_version"`
	ReviewID       string `json:"review_id"`
	PacketDigest   string `json:"packet_digest"`
	SourceDigest   string `json:"source_digest"`
	RunID          string `json:"run_id"`
	Fence          uint64 `json:"fence"`
	ConsumedAt     string `json:"consumed_at"`
	Outcome        string `json:"outcome"`
}

type Limits struct {
	MaxMemberBytes int64
	MaxPacketBytes int64
	MaxMembers     int
}

type Expected struct {
	ReviewID           string
	ProjectID          string
	IssueID            string
	RunID              string
	CheckpointSequence uint64
}

type VerifiedPacket struct {
	Path           string
	Digest         string
	SourceDigest   string
	Manifest       Manifest
	Review         ReviewReceipt
	ArtifactMember Member
	Bytes          map[string][]byte
}

func CanonicalManifest(manifest Manifest) ([]byte, error) {
	if err := validateManifest(manifest); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func CanonicalReviewReceipt(receipt ReviewReceipt) ([]byte, error) {
	if err := validateReviewReceipt(receipt); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func PacketDigest(manifestBytes []byte) string { return digest(manifestBytes) }

func SourceDigest(source SourceClaim) (string, error) {
	if err := validateSource(source); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(source)
	if err != nil {
		return "", err
	}
	return digest(append(encoded, '\n')), nil
}

func validateManifest(manifest Manifest) error {
	invalid := func(message string) error { return fmt.Errorf("%w: %s", ErrMalformedPacket, message) }
	if manifest.PacketVersion != PacketVersion {
		return invalid(fmt.Sprintf("packet_version must be %d", PacketVersion))
	}
	for name, value := range map[string]string{"review_id": manifest.ReviewID, "project_id": manifest.ProjectID, "issue_id": manifest.IssueID, "run_id": manifest.RunID} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value || strings.ContainsRune(value, 0) {
			return invalid(name + " is invalid")
		}
	}
	if manifest.CheckpointSequence == 0 {
		return invalid("checkpoint_sequence must be positive")
	}
	if strings.Contains(manifest.RunID, ":") || strings.Contains(manifest.IssueID, ":") || manifest.ReviewID != ReviewID(manifest.RunID, manifest.IssueID, manifest.CheckpointSequence) {
		return invalid("review_id must be canonically derived from run, issue, and checkpoint")
	}
	if err := validateSource(manifest.Source); err != nil {
		return err
	}
	if len(manifest.Members) == 0 {
		return invalid("members must not be empty")
	}
	seen := make(map[string]bool, len(manifest.Members))
	kinds := map[string]int{}
	previous := ""
	for _, member := range manifest.Members {
		if !safeMember(member.Path) || member.Path == ManifestName || seen[member.Path] {
			return invalid("member path is unsafe or duplicated: " + member.Path)
		}
		if previous != "" && member.Path <= previous {
			return invalid("members must be sorted by path")
		}
		previous = member.Path
		seen[member.Path] = true
		switch member.Kind {
		case "artifact", "test_receipt", "review_receipt", "source_diff":
		default:
			return invalid("unknown member kind: " + member.Kind)
		}
		kinds[member.Kind]++
		if !validDigest(member.SHA256) || member.Size < 0 {
			return invalid("member digest or size is invalid: " + member.Path)
		}
	}
	if kinds["artifact"] < 1 || kinds["test_receipt"] < 1 || kinds["review_receipt"] != 1 || kinds["source_diff"] != 1 {
		return invalid("packet requires artifacts, test receipts, exactly one review receipt, and exactly one source diff")
	}
	return nil
}

func ReviewID(runID, issueID string, checkpoint uint64) string {
	return fmt.Sprintf("review:%s:%s:%d", runID, issueID, checkpoint)
}

func validateSource(source SourceClaim) error {
	invalid := func(message string) error { return fmt.Errorf("%w: %s", ErrMalformedPacket, message) }
	if !validObjectID.MatchString(source.Commit) || !validDigest(source.DiffSHA256) {
		return invalid("source commit or diff digest is invalid")
	}
	previous := ""
	seen := map[string]bool{}
	for _, path := range source.ChangedFiles {
		if !safeRelativePath(path) || seen[path] || (previous != "" && path <= previous) {
			return invalid("changed_files must contain unique sorted safe relative paths")
		}
		seen[path] = true
		previous = path
	}
	return nil
}

func validateReviewReceipt(receipt ReviewReceipt) error {
	invalid := func(message string) error { return fmt.Errorf("%w: %s", ErrMalformedPacket, message) }
	if receipt.ReceiptVersion != ReceiptVersion || receipt.Verdict != "approved" {
		return invalid("review receipt version or verdict is invalid")
	}
	for name, value := range map[string]string{
		"review_id": receipt.ReviewID, "project_id": receipt.ProjectID, "issue_id": receipt.IssueID,
		"run_id": receipt.RunID, "source_commit": receipt.SourceCommit, "source_digest": receipt.SourceDigest,
		"artifact_path": receipt.ArtifactPath, "artifact_sha256": receipt.ArtifactSHA256,
		"author.provider": receipt.Author.Provider, "author.model": receipt.Author.Model,
		"reviewer.provider": receipt.Reviewer.Provider, "reviewer.model": receipt.Reviewer.Model,
	} {
		if strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value || strings.ContainsRune(value, 0) {
			return invalid(name + " is invalid")
		}
	}
	if receipt.CheckpointSequence == 0 || !validObjectID.MatchString(receipt.SourceCommit) || !validDigest(receipt.SourceDigest) || !safeMember(receipt.ArtifactPath) || !validDigest(receipt.ArtifactSHA256) {
		return invalid("review receipt binding is invalid")
	}
	if strings.Contains(receipt.RunID, ":") || strings.Contains(receipt.IssueID, ":") || receipt.ReviewID != ReviewID(receipt.RunID, receipt.IssueID, receipt.CheckpointSequence) {
		return invalid("review_id must be canonically derived from run, issue, and checkpoint")
	}
	if strings.EqualFold(receipt.Author.Provider, receipt.Reviewer.Provider) || strings.EqualFold(receipt.Author.Model, receipt.Reviewer.Model) {
		return invalid("reviewer provider and model must both differ from the author")
	}
	if len(receipt.Checks) == 0 {
		return invalid("checks must not be empty")
	}
	seen := map[string]bool{}
	for _, check := range receipt.Checks {
		if strings.TrimSpace(check) == "" || strings.TrimSpace(check) != check || seen[check] {
			return invalid("checks must be nonempty and unique")
		}
		seen[check] = true
	}
	return nil
}

func strictCanonicalJSON(data []byte, target any, canonical func() ([]byte, error)) error {
	if err := rejectDuplicateJSONKeys(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	want, err := canonical()
	if err != nil {
		return err
	}
	if !bytes.Equal(data, want) {
		return errors.New("JSON is not in canonical form")
	}
	return nil
}

func rejectDuplicateJSONKeys(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	var walk func(int) error
	walk = func(depth int) error {
		if depth > maxCanonicalJSONDepth {
			return errors.New("JSON nesting exceeds limit")
		}
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		delim, ok := token.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || seen[key] {
					return errors.New("duplicate or invalid JSON object key")
				}
				seen[key] = true
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		case '[':
			for decoder.More() {
				if err := walk(depth + 1); err != nil {
					return err
				}
			}
			_, err = decoder.Token()
			return err
		default:
			return errors.New("unexpected JSON delimiter")
		}
	}
	if err := walk(1); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("multiple JSON values")
	}
	return nil
}

func safeMember(path string) bool {
	return safeMemberName.MatchString(path) && filepath.Base(path) == path && path != "." && path != ".."
}

func safeRelativePath(path string) bool {
	if path == "" || strings.ContainsRune(path, 0) || strings.Contains(path, `\`) || filepath.IsAbs(path) || filepath.Clean(path) != path || path == "." || path == ".." || strings.HasPrefix(path, "../") {
		return false
	}
	for _, part := range strings.Split(filepath.ToSlash(path), "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

func digest(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func validDigest(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func sortedMemberPaths(members []Member) []string {
	paths := make([]string, 0, len(members))
	for _, member := range members {
		paths = append(paths, member.Path)
	}
	sort.Strings(paths)
	return paths
}
