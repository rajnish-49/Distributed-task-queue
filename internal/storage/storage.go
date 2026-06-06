package storage

import (
	"context"
	"distributed-task-queue/internal/job"
	"encoding/json"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
	"github.com/jackc/pgx/v5"

)

type Store struct {
	db *pgxpool.Pool
}

func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}

	return pool, nil
}

func NewStore(db *pgxpool.Pool) *Store {
	return &Store{
		db: db, // store.db = value that was passed into NewStore
	}
}

type CreateJobParams struct {
	Type        string
	Payload     json.RawMessage
	MaxAttempts int
	ScheduledAt time.Time
}

func (s *Store) CreateJob(ctx context.Context, params CreateJobParams) (*job.Job, error) {
	if params.MaxAttempts == 0 {
		params.MaxAttempts = 3
	}

	if params.ScheduledAt.IsZero() {
		params.ScheduledAt = time.Now().UTC()
	}

	const query = `
	INSERT INTO jobs (job_type, payload, status, max_attempts, scheduled_at)
	VALUES ($1, $2::jsonb, $3, $4, $5)
	RETURNING
		id,
		job_type,
		payload,
		status,
		attempts,
		max_attempts,
		COALESCE(error_msg, ''),
		scheduled_at,
		created_at,
		updated_at
`

	var created job.Job

	err := s.db.QueryRow(
		ctx,
		query,
		params.Type,
		string(params.Payload),
		job.StatusPending,
		params.MaxAttempts,
		params.ScheduledAt,
	).Scan(
		&created.ID,
		&created.Type,
		&created.Payload,
		&created.Status,
		&created.Attempts,
		&created.MaxAttempts,
		&created.ErrorMsg,
		&created.ScheduledAt,
		&created.CreatedAt,
		&created.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	return &created, nil
}

func (s *Store) ClaimJob(ctx context.Context) (*job.Job, error) {
    tx, err := s.db.Begin(ctx)
    if err != nil {
        return nil, err
    }
    defer tx.Rollback(ctx)

    const selectQuery = `
        SELECT id FROM jobs
        WHERE status = 'pending'
        AND scheduled_at <= NOW()
        ORDER BY scheduled_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    `

    var id int64
    err = tx.QueryRow(ctx, selectQuery).Scan(&id)
    if err == pgx.ErrNoRows {
        return nil, nil
    }
    if err != nil {
        return nil, err
    }

    const updateQuery = `
        UPDATE jobs
        SET status = 'processing',
            updated_at = NOW()
        WHERE id = $1
        RETURNING
            id,
            job_type,
            payload,
            status,
            attempts,
            max_attempts,
            COALESCE(error_msg, ''),
            scheduled_at,
            created_at,
            updated_at
    `

    var claimed job.Job
    err = tx.QueryRow(ctx, updateQuery, id).Scan(
        &claimed.ID,
        &claimed.Type,
        &claimed.Payload,
        &claimed.Status,
        &claimed.Attempts,
        &claimed.MaxAttempts,
        &claimed.ErrorMsg,
        &claimed.ScheduledAt,
        &claimed.CreatedAt,
        &claimed.UpdatedAt,
    )
    if err != nil {
        return nil, err
    }

    if err := tx.Commit(ctx); err != nil {
        return nil, err
    }

    return &claimed, nil
}


