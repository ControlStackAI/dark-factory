package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"

	"github.com/ControlStackAI/dark-factory/internal/domain"
	"github.com/ControlStackAI/dark-factory/internal/ports"
	_ "modernc.org/sqlite"
)

// ReadRun opens SQLite in read-only mode for factoryctl status. It does not acquire the
// factoryd writer lock, create schema, change pragmas, or mutate durable state.
func ReadRun(ctx context.Context, path, runID string) (domain.Run, error) {
	dsn := (&url.URL{Scheme: "file", Path: path, RawQuery: "mode=ro"}).String()
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return domain.Run{}, err
	}
	defer db.Close()
	var appID, version int
	if err := db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&appID); err != nil {
		return domain.Run{}, classify(err)
	}
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return domain.Run{}, classify(err)
	}
	if appID != applicationID || version != schemaVersion {
		return domain.Run{}, fmt.Errorf("%w: unsupported database identity", ErrInvalidRecord)
	}
	var storedVersion uint64
	var payload []byte
	if err := db.QueryRowContext(ctx, `SELECT version, payload FROM runs WHERE id = ?`, runID).Scan(&storedVersion, &payload); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.Run{}, ports.ErrNotFound
		}
		return domain.Run{}, classify(err)
	}
	return decodeRun(runID, storedVersion, payload)
}
