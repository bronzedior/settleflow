package client

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type JobID = uuid.UUID

type JobArgs interface {
	Kind() string
}

type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) error
}

type Client struct {
	execer Execer
}

type JobOption func(*jobOpts)

type jobOpts struct {
	queue       string
	runAt       time.Time
	maxAttempts int
}

func WithQueue(q string) JobOption {
	return func(o *jobOpts) {
		o.queue = q
	}
}

func WithMaxAttempts(n int) JobOption {
	return func(o *jobOpts) {
		o.maxAttempts = n
	}
}

func WithRunAt(t time.Time) JobOption {
	return func(o *jobOpts) {
		o.runAt = t
	}
}

func New(execer Execer) *Client {
	return &Client{execer: execer}
}

func Enqueue[T JobArgs](ctx context.Context, c *Client, args T, opts ...JobOption) (JobID, error) {
	return EnqueueTx(ctx, c, c.execer, args, opts...)
}

func EnqueueTx[T JobArgs](ctx context.Context, c *Client, tx Execer, args T, opts ...JobOption) (JobID, error) {
	jobOpts := &jobOpts{
		queue:       "default",
		maxAttempts: 20,
		runAt:       time.Now(),
	}

	for _, opt := range opts {
		opt(jobOpts)
	}

	id := JobID(uuid.New())

	payload, err := json.Marshal(args)
	if err != nil {
		return id, fmt.Errorf("marshal payload: %w", err)
	}

	const sql = `INSERT INTO jobs (id, queue, job_type, payload_version, payload, state, attempt, max_attempts, run_at)
                      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`

	err = tx.Exec(ctx, sql,
		id, jobOpts.queue, args.Kind(), 1, payload, "pending", 0, jobOpts.maxAttempts, jobOpts.runAt)
	if err != nil {
		return id, fmt.Errorf("enqueue: %w", err)
	}

	return id, nil
}
