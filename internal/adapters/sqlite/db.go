package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ControlStackAI/dark-factory/internal/ports"
	_ "modernc.org/sqlite"
)

const (
	schemaVersion = 1
	applicationID = 0x44464b31 // "DFK1"
)

type AfterCommitHook func(phase string) error

type Option func(*options)

type options struct {
	beforeCommit BeforeCommitHook
	afterCommit  AfterCommitHook
}

type BeforeCommitHook func(phase string) error

// WithBeforeCommitHook installs a deterministic transaction-rollback hook. It is intended
// for tests proving that no part of a phase is visible when commit does not occur.
func WithBeforeCommitHook(hook BeforeCommitHook) Option {
	return func(options *options) { options.beforeCommit = hook }
}

// WithAfterCommitHook installs a deterministic fault-injection hook. The hook runs only
// after a transaction is durable, so an error models a process losing the acknowledgement
// for a committed phase. Production callers should not install one.
func WithAfterCommitHook(hook AfterCommitHook) Option {
	return func(options *options) { options.afterCommit = hook }
}

type Store struct {
	beforeCommit BeforeCommitHook
	db           *sql.DB
	afterCommit  AfterCommitHook
}

func Open(path string, opts ...Option) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("sqlite path is empty")
	}
	config := options{}
	for _, opt := range opts {
		opt(&config)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, classify(err)
	}
	// A factory controller owns one SQLite writer. Serializing through one connection
	// avoids in-process lock upgrades; busy_timeout handles a second process explicitly.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	store := &Store{db: db, beforeCommit: config.beforeCommit, afterCommit: config.afterCommit}
	if err := store.initialize(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) initialize(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		return fmt.Errorf("open sqlite: %w", classify(err))
	}
	if _, err := s.db.ExecContext(ctx, "PRAGMA busy_timeout = 5000"); err != nil {
		return fmt.Errorf("configure sqlite busy timeout: %w", classify(err))
	}
	if err := integrityCheck(ctx, s.db); err != nil {
		return err
	}
	var version, appID int
	if err := s.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("read sqlite schema version: %w", classify(err))
	}
	if err := s.db.QueryRowContext(ctx, "PRAGMA application_id").Scan(&appID); err != nil {
		return fmt.Errorf("read sqlite application id: %w", classify(err))
	}
	if version > schemaVersion {
		return fmt.Errorf("%w: database=%d supported=%d", ErrUnsupportedSchema, version, schemaVersion)
	}
	if version != 0 && appID != applicationID {
		return fmt.Errorf("%w: application id %d", ErrInvalidRecord, appID)
	}
	if version == 0 {
		if appID != 0 {
			return fmt.Errorf("%w: unversioned database has application id %d", ErrInvalidRecord, appID)
		}
		var objects int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'`).Scan(&objects); err != nil {
			return fmt.Errorf("inspect unversioned sqlite database: %w", classify(err))
		}
		if objects != 0 {
			return fmt.Errorf("%w: refusing to initialize non-empty unversioned database", ErrInvalidRecord)
		}
	}
	for _, statement := range []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA synchronous = FULL",
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("configure sqlite: %w", classify(err))
		}
	}
	var journalMode string
	if err := s.db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&journalMode); err != nil {
		return fmt.Errorf("enable sqlite WAL: %w", classify(err))
	}
	if journalMode != "wal" && journalMode != "memory" {
		return fmt.Errorf("enable sqlite WAL: got journal mode %q", journalMode)
	}
	if version == 0 {
		if err := s.createSchema(ctx); err != nil {
			return fmt.Errorf("create sqlite schema: %w", err)
		}
		version = schemaVersion
		appID = applicationID
	}
	if version != schemaVersion {
		return fmt.Errorf("%w: database=%d supported=%d", ErrUnsupportedSchema, version, schemaVersion)
	}
	if err := s.validateSchema(ctx); err != nil {
		return err
	}
	return nil
}

func (s *Store) createSchema(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return classify(err)
	}
	defer tx.Rollback()
	for _, statement := range schemaStatements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return classify(err)
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA application_id = %d", applicationID)); err != nil {
		return classify(err)
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
		return classify(err)
	}
	return classify(tx.Commit())
}

var schemaStatements = []string{
	`CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
	`INSERT INTO schema_meta(key, value) VALUES ('format', 'dark-factory-recovery-v1')`,
	`CREATE TABLE runs (
		id TEXT PRIMARY KEY CHECK(length(id) > 0),
		version INTEGER NOT NULL CHECK(version > 0),
		payload BLOB NOT NULL CHECK(length(payload) > 0)
	)`,
	`CREATE TABLE journal (
		sequence INTEGER PRIMARY KEY AUTOINCREMENT,
		run_id TEXT NOT NULL REFERENCES runs(id),
		run_version INTEGER NOT NULL CHECK(run_version > 0),
		phase TEXT NOT NULL CHECK(length(phase) > 0),
		payload BLOB NOT NULL CHECK(length(payload) > 0),
		created_at TEXT NOT NULL CHECK(length(created_at) > 0)
	)`,
	`CREATE INDEX journal_run_sequence ON journal(run_id, sequence)`,
	`CREATE TABLE attempt_reservations (
		run_id TEXT NOT NULL REFERENCES runs(id),
		attempt INTEGER NOT NULL CHECK(attempt > 0),
		fence INTEGER NOT NULL CHECK(fence > 0),
		reserved_at TEXT NOT NULL CHECK(length(reserved_at) > 0),
		PRIMARY KEY(run_id, attempt)
	)`,
	`CREATE TABLE reviews (
		id TEXT PRIMARY KEY CHECK(length(id) > 0),
		project_id TEXT NOT NULL CHECK(length(project_id) > 0),
		issue_id TEXT NOT NULL CHECK(length(issue_id) > 0),
		status TEXT NOT NULL CHECK(length(status) > 0),
		immutable INTEGER NOT NULL CHECK(immutable IN (0, 1)),
		artifact_ref TEXT NOT NULL CHECK(length(artifact_ref) > 0),
		artifact_sha256 TEXT NOT NULL CHECK(length(artifact_sha256) = 64),
		consumed_by_run TEXT NOT NULL DEFAULT ''
	)`,
	`CREATE TABLE artifacts (
		ref TEXT PRIMARY KEY CHECK(length(ref) > 0),
		contents BLOB NOT NULL,
		sha256 TEXT NOT NULL CHECK(length(sha256) = 64)
	)`,
	`CREATE TABLE issues (
		id TEXT PRIMARY KEY CHECK(length(id) > 0),
		project_id TEXT NOT NULL CHECK(length(project_id) > 0),
		title TEXT NOT NULL,
		priority INTEGER NOT NULL,
		created_at TEXT NOT NULL CHECK(length(created_at) > 0),
		state TEXT NOT NULL CHECK(state IN ('ready', 'in_progress', 'completed')),
		blocked INTEGER NOT NULL CHECK(blocked IN (0, 1))
	)`,
	`CREATE TABLE advance_receipts (
		idempotency_key TEXT PRIMARY KEY CHECK(length(idempotency_key) > 0),
		request BLOB NOT NULL CHECK(length(request) > 0),
		committed_at TEXT NOT NULL CHECK(length(committed_at) > 0)
	)`,
}

func (s *Store) validateSchema(ctx context.Context) error {
	var format string
	if err := s.db.QueryRowContext(ctx, `SELECT value FROM schema_meta WHERE key = 'format'`).Scan(&format); err != nil {
		return fmt.Errorf("%w: schema metadata: %v", ErrInvalidRecord, classify(err))
	}
	if format != "dark-factory-recovery-v1" {
		return fmt.Errorf("%w: schema format %q", ErrInvalidRecord, format)
	}
	for _, table := range []string{"runs", "journal", "attempt_reservations", "reviews", "artifacts", "issues", "advance_receipts"} {
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, table).Scan(&count); err != nil {
			return fmt.Errorf("validate table %s: %w", table, classify(err))
		}
		if count != 1 {
			return fmt.Errorf("%w: missing table %s", ErrInvalidRecord, table)
		}
	}
	if err := integrityCheck(ctx, s.db); err != nil {
		return err
	}
	return s.validateRecords(ctx)
}

func integrityCheck(ctx context.Context, db *sql.DB) error {
	rows, err := db.QueryContext(ctx, "PRAGMA quick_check")
	if err != nil {
		return fmt.Errorf("%w: %v", ErrCorruptDatabase, classify(err))
	}
	defer rows.Close()
	var results []string
	for rows.Next() {
		var result string
		if err := rows.Scan(&result); err != nil {
			return fmt.Errorf("%w: quick_check: %v", ErrCorruptDatabase, err)
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: quick_check: %v", ErrCorruptDatabase, err)
	}
	if len(results) != 1 || results[0] != "ok" {
		return fmt.Errorf("%w: quick_check: %s", ErrCorruptDatabase, strings.Join(results, "; "))
	}
	return nil
}

func (s *Store) committed(phase string) error {
	if s.afterCommit == nil {
		return nil
	}
	return s.afterCommit(phase)
}

func (s *Store) committing(phase string) error {
	if s.beforeCommit == nil {
		return nil
	}
	return s.beforeCommit(phase)
}

func nowText() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func classify(err error) error {
	if err == nil {
		return nil
	}
	lower := strings.ToLower(err.Error())
	switch {
	case strings.Contains(lower, "database is locked"), strings.Contains(lower, "sqlite_busy"):
		return fmt.Errorf("%w: %v", ports.ErrBusy, err)
	case strings.Contains(lower, "database disk image is malformed"), strings.Contains(lower, "file is not a database"), strings.Contains(lower, "sqlite_corrupt"):
		return fmt.Errorf("%w: %v", ErrCorruptDatabase, err)
	default:
		return err
	}
}
