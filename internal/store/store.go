// Package store persists webhook events, calls, and per-account aggregates.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is one call-completion webhook delivery.
type Event struct {
	EventID      string
	CallID       string
	AccountID    string
	Status       string
	DurationSec  int
	RecordingURL string
	OccurredAt   time.Time
	Payload      []byte
}

// Stats is the durable per-account aggregate.
type Stats struct {
	CallCount        int64
	TotalDurationSec int64
}

// StatsChange is one account-total adjustment made by a delivery.
type StatsChange struct {
	AccountID        string
	CallCountDelta   int64
	DurationSecDelta int64
}

// Store is a Postgres-backed repository.
type Store struct {
	pool *pgxpool.Pool
}

// New opens a connection pool bounded to maxConns.
func New(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = maxConns

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Store{pool: pool}, nil
}

// Pool exposes the underlying pool for tests and ad-hoc queries.
func (s *Store) Pool() *pgxpool.Pool { return s.pool }

// Close releases all pooled connections.
func (s *Store) Close() { s.pool.Close() }

// ProcessDelivery atomically stores a new delivery, updates the current state
// of its call, and applies only the resulting aggregate delta. A duplicate
// event ID is an acknowledged no-op and returns inserted=false.
func (s *Store) ProcessDelivery(ctx context.Context, e Event) (changes []StatsChange, inserted bool, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var storedEventID string
	err = tx.QueryRow(ctx, `INSERT INTO events (event_id, call_id, account_id, payload)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (event_id) WHERE deduplicated_at IS NULL DO NOTHING
		RETURNING event_id`, e.EventID, e.CallID, e.AccountID, e.Payload).Scan(&storedEventID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	// Serialize different event IDs for one call, including the first insert
	// where SELECT ... FOR UPDATE alone has no row to lock yet.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, e.CallID); err != nil {
		return nil, false, err
	}

	var previousAccountID string
	var previousDurationSec int
	err = tx.QueryRow(ctx, `SELECT account_id, duration_sec FROM calls WHERE call_id = $1 FOR UPDATE`, e.CallID).
		Scan(&previousAccountID, &previousDurationSec)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		if _, err = tx.Exec(ctx, `INSERT INTO calls (call_id, account_id, status, duration_sec, recording_url, updated_at)
			VALUES ($1, $2, $3, $4, $5, now())`, e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL); err != nil {
			return nil, false, err
		}
		changes = append(changes, StatsChange{AccountID: e.AccountID, CallCountDelta: 1, DurationSecDelta: int64(e.DurationSec)})
	case err == nil:
		if _, err = tx.Exec(ctx, `UPDATE calls SET account_id = $2, status = $3, duration_sec = $4,
			recording_processed = CASE WHEN recording_url IS DISTINCT FROM $5 THEN FALSE ELSE recording_processed END,
			recording_url = $5, updated_at = now() WHERE call_id = $1`,
			e.CallID, e.AccountID, e.Status, e.DurationSec, e.RecordingURL); err != nil {
			return nil, false, err
		}
		if previousAccountID == e.AccountID {
			changes = append(changes, StatsChange{AccountID: e.AccountID, DurationSecDelta: int64(e.DurationSec - previousDurationSec)})
		} else {
			changes = append(changes,
				StatsChange{AccountID: previousAccountID, CallCountDelta: -1, DurationSecDelta: -int64(previousDurationSec)},
				StatsChange{AccountID: e.AccountID, CallCountDelta: 1, DurationSecDelta: int64(e.DurationSec)},
			)
		}
	default:
		return nil, false, err
	}

	for _, change := range changes {
		if change.CallCountDelta == 0 && change.DurationSecDelta == 0 {
			continue
		}
		if _, err = tx.Exec(ctx, `INSERT INTO account_stats (account_id, call_count, total_duration_sec)
			VALUES ($1, $2, $3)
			ON CONFLICT (account_id) DO UPDATE SET
				call_count = account_stats.call_count + EXCLUDED.call_count,
				total_duration_sec = account_stats.total_duration_sec + EXCLUDED.total_duration_sec`,
			change.AccountID, change.CallCountDelta, change.DurationSecDelta); err != nil {
			return nil, false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	return changes, true, nil
}

// MarkRecordingProcessed flags the call's recording as handled.
func (s *Store) MarkRecordingProcessed(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE calls SET recording_processed = TRUE, recording_locked_at = NULL, updated_at = now()
		 WHERE call_id = $1`, callID)
	return err
}

// ClaimPendingRecordingCallIDs assigns a short durable lease to unfinished
// recordings. SKIP LOCKED lets many workers and replicas claim distinct work.
func (s *Store) ClaimPendingRecordingCallIDs(ctx context.Context, limit int) ([]string, error) {
	rows, err := s.pool.Query(ctx, `WITH candidates AS (
		SELECT call_id
		FROM calls
		WHERE recording_processed = FALSE
		  AND recording_url IS NOT NULL
		  AND recording_url <> ''
		  AND (recording_locked_at IS NULL OR recording_locked_at < now() - interval '5 minutes')
		ORDER BY updated_at
		FOR UPDATE SKIP LOCKED
		LIMIT $1
	)
	UPDATE calls
	SET recording_locked_at = now()
	FROM candidates
	WHERE calls.call_id = candidates.call_id
	RETURNING calls.call_id`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var callIDs []string
	for rows.Next() {
		var callID string
		if err := rows.Scan(&callID); err != nil {
			return nil, err
		}
		callIDs = append(callIDs, callID)
	}
	return callIDs, rows.Err()
}

// ReleaseRecordingLease makes a failed or cancelled recording eligible for a
// later retry without waiting for its lease to expire.
func (s *Store) ReleaseRecordingLease(ctx context.Context, callID string) error {
	_, err := s.pool.Exec(ctx, `UPDATE calls
		SET recording_locked_at = NULL
		WHERE call_id = $1 AND recording_processed = FALSE`, callID)
	return err
}

// AccountStats reads the durable aggregate. A missing account reads as zero.
func (s *Store) AccountStats(ctx context.Context, accountID string) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx,
		`SELECT call_count, total_duration_sec FROM account_stats WHERE account_id = $1`,
		accountID).Scan(&st.CallCount, &st.TotalDurationSec)
	if errors.Is(err, pgx.ErrNoRows) {
		return Stats{}, nil
	}
	if err != nil {
		return Stats{}, err
	}
	return st, nil
}

// AllAccountStats returns every durable account total for cache hydration.
func (s *Store) AllAccountStats(ctx context.Context) (map[string]Stats, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT account_id, call_count, total_duration_sec FROM account_stats`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	totals := make(map[string]Stats)
	for rows.Next() {
		var accountID string
		var total Stats
		if err := rows.Scan(&accountID, &total.CallCount, &total.TotalDurationSec); err != nil {
			return nil, err
		}
		totals[accountID] = total
	}
	return totals, rows.Err()
}
