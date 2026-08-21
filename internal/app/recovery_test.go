package app

import (
	"context"
	"path/filepath"
	"testing"

	durablesqlite "github.com/ControlStackAI/dark-factory/internal/adapters/sqlite"
	"github.com/ControlStackAI/dark-factory/internal/domain"
)

func TestDurableDryRunReopensWithoutRepeatingWork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "factory.db")
	first, err := DurableDryRun(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != domain.RunComplete || first.Attempts != 1 {
		t.Fatalf("first durable run = %#v", first)
	}
	second, err := DurableDryRun(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if second.Version != first.Version || second.Attempts != first.Attempts || second.Status != domain.RunComplete {
		t.Fatalf("reopen changed completed run: first=%#v second=%#v", first, second)
	}
	store, err := durablesqlite.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	reservations, err := store.AttemptReservationCount(context.Background(), durableRunID)
	if err != nil {
		t.Fatal(err)
	}
	if reservations != 1 {
		t.Fatalf("attempt reservations = %d, want 1", reservations)
	}
	entries, err := store.Journal(context.Background(), durableRunID)
	if err != nil {
		t.Fatal(err)
	}
	remoteCommits := 0
	for _, entry := range entries {
		if entry.Phase == "remote_advance_committed" {
			remoteCommits++
		}
	}
	if remoteCommits != 1 {
		t.Fatalf("remote advancement journal entries = %d, want 1", remoteCommits)
	}
}
