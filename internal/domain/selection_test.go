package domain_test

import (
	"testing"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/domain"
)

func TestSelectNextIsDeterministic(t *testing.T) {
	created := time.Date(2026, time.August, 20, 12, 0, 0, 0, time.UTC)
	issues := []domain.Issue{
		{ID: "DF-9", Priority: 1, CreatedAt: created.Add(-time.Hour), State: domain.IssueReady, Blocked: true},
		{ID: "DF-3", Priority: 1, CreatedAt: created, State: domain.IssueReady},
		{ID: "DF-2", Priority: 1, CreatedAt: created, State: domain.IssueReady},
		{ID: "DF-1", Priority: 2, CreatedAt: created.Add(-24 * time.Hour), State: domain.IssueReady},
		{ID: "DF-0", Priority: 0, CreatedAt: created.Add(-48 * time.Hour), State: domain.IssueReady},
	}

	next, ok := domain.SelectNext(issues)
	if !ok {
		t.Fatal("SelectNext returned no candidate")
	}
	if next.ID != "DF-2" {
		t.Fatalf("SelectNext chose %q, want DF-2", next.ID)
	}
}
