package domain

import "slices"

// SelectNext deterministically chooses the highest-priority unblocked Ready
// issue. Priority zero is treated as unset and sorted after priorities 1-4.
func SelectNext(issues []Issue) (Issue, bool) {
	return SelectNextExcluding(issues, "")
}

// SelectNextExcluding applies the stable Linear ordering and excludes the current issue by
// either immutable ID or human identifier. Identifier falls back to ID for M0/M1 fixtures.
func SelectNextExcluding(issues []Issue, current string) (Issue, bool) {
	candidates := make([]Issue, 0, len(issues))
	for _, issue := range issues {
		if issue.State == IssueReady && !issue.Blocked && (current == "" || issue.ID != current && issue.Identifier != current) {
			candidates = append(candidates, issue)
		}
	}

	slices.SortFunc(candidates, func(a, b Issue) int {
		if comparison := normalizedPriority(a.Priority) - normalizedPriority(b.Priority); comparison != 0 {
			return comparison
		}
		if comparison := a.CreatedAt.Compare(b.CreatedAt); comparison != 0 {
			return comparison
		}
		aIdentifier, bIdentifier := a.Identifier, b.Identifier
		if aIdentifier == "" {
			aIdentifier = a.ID
		}
		if bIdentifier == "" {
			bIdentifier = b.ID
		}
		if aIdentifier < bIdentifier {
			return -1
		}
		if aIdentifier > bIdentifier {
			return 1
		}
		return 0
	})

	if len(candidates) == 0 {
		return Issue{}, false
	}
	return candidates[0], true
}

func normalizedPriority(priority int) int {
	if priority == 0 {
		return 5
	}
	return priority
}
