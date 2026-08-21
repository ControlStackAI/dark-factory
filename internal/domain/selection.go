package domain

import "slices"

// SelectNext deterministically chooses the highest-priority unblocked Ready
// issue. Priority zero is treated as unset and sorted after priorities 1-4.
func SelectNext(issues []Issue) (Issue, bool) {
	candidates := make([]Issue, 0, len(issues))
	for _, issue := range issues {
		if issue.State == IssueReady && !issue.Blocked {
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
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
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
