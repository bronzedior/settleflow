package queue

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) (interface{}, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Store struct {
	execer Execer
}

func NewStore(execer Execer) *Store {
	return &Store{execer: execer}
}

func (s *Store) Enqueue(ctx context.Context, queueName, jobType string, payloadVersion int, payload interface{}) (JobID, error) {
	id := NewJobID()

	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return id, fmt.Errorf("marshal payload: %w", err)
	}

	_, err = s.execer.Exec(ctx,
		`INSERT INTO jobs (id, queue, job_type, payload_version, payload, state, attempt, max_attempts)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		id, queueName, jobType, payloadVersion, payloadJSON, string(StatePending), 0, 20)

	if err != nil {
		return id, fmt.Errorf("enqueue: %w", err)
	}

	return id, nil
}

func (s *Store) ClaimJobs(ctx context.Context, workerID string, queues []string, k int) ([]Job, error) {
	sql := `
		UPDATE jobs j
		SET state = $1, claimed_by = $2, claimed_at = now(),
			heartbeat_at = now(), attempt = j.attempt + 1, updated_at = now()
		FROM (
			SELECT id FROM jobs
			WHERE state = $3 AND queue = ANY($4::text[]) AND run_at <= now()
			ORDER BY run_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $5
		) s
		WHERE j.id = s.id
		RETURNING j.id, j.queue, j.job_type, j.payload_version, j.payload,
			j.state, j.attempt, j.max_attempts, j.run_at, j.claimed_at,
			j.claimed_by, j.heartbeat_at, j.last_error, j.last_error_class,
			j.checkpoint, j.created_at, j.updated_at
	`

	rows, err := s.execer.Query(ctx, sql,
		string(StateClaimed), workerID, string(StatePending), queues, k)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	defer rows.Close()

	jobs := make([]Job, 0, k)
	for rows.Next() {
		var j Job
		err := rows.Scan(
			&j.ID, &j.Queue, &j.JobType, &j.PayloadVersion, &j.Payload,
			&j.State, &j.Attempt, &j.MaxAttempts, &j.RunAt, &j.ClaimedAt,
			&j.ClaimedBy, &j.HeartbeatAt, &j.LastError, &j.LastErrorClass,
			&j.Checkpoint, &j.CreatedAt, &j.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		jobs = append(jobs, j)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return jobs, nil
}

func (s *Store) CompleteJob(ctx context.Context, jobID JobID, workerID string) error {
	sql := `
		WITH done AS (
			DELETE FROM jobs WHERE id = $1 AND claimed_by = $2 RETURNING *
		)
		INSERT INTO jobs_archive SELECT *, now() FROM done
	`

	_, err := s.execer.Exec(ctx, sql, jobID, workerID)
	if err != nil {
		return fmt.Errorf("complete job: %w", err)
	}

	return nil
}

func (s *Store) FailJob(ctx context.Context, jobID JobID, workerID string, errorMsg string, errorClass ErrorClass) error {
	sql := `
		UPDATE jobs
		SET state = $1, last_error = $2, last_error_class = $3, updated_at = now()
		WHERE id = $4 AND claimed_by = $5
	`

	_, err := s.execer.Exec(ctx, sql,
		string(StateDead), errorMsg, string(errorClass), jobID, workerID)
	if err != nil {
		return fmt.Errorf("fail job: %w", err)
	}

	return nil
}

func (s *Store) RetryJob(ctx context.Context, jobID JobID, workerID string, runAt interface{}, errorMsg string, errorClass ErrorClass) error {
	sql := `
		UPDATE jobs
		SET state = $1, claimed_by = NULL, claimed_at = NULL, heartbeat_at = NULL,
			run_at = $2, last_error = $3, last_error_class = $4, updated_at = now()
		WHERE id = $5 AND claimed_by = $6
	`

	_, err := s.execer.Exec(ctx, sql,
		string(StatePending), runAt, errorMsg, string(errorClass), jobID, workerID)
	if err != nil {
		return fmt.Errorf("retry job: %w", err)
	}

	return nil
}

func (s *Store) GetJob(ctx context.Context, jobID JobID) (*Job, error) {
	sql := `SELECT id, queue, job_type, payload_version, payload, state, attempt,
			max_attempts, run_at, claimed_at, claimed_by, heartbeat_at, last_error,
			last_error_class, checkpoint, created_at, updated_at
		FROM jobs WHERE id = $1`

	var j Job
	err := s.execer.QueryRow(ctx, sql, jobID).Scan(
		&j.ID, &j.Queue, &j.JobType, &j.PayloadVersion, &j.Payload,
		&j.State, &j.Attempt, &j.MaxAttempts, &j.RunAt, &j.ClaimedAt,
		&j.ClaimedBy, &j.HeartbeatAt, &j.LastError, &j.LastErrorClass,
		&j.Checkpoint, &j.CreatedAt, &j.UpdatedAt,
	)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get job: %w", err)
	}

	return &j, nil
}
