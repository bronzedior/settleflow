package client

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

type JobID = uuid.UUID

type Execer interface {
	Exec(ctx context.Context, sql string, args ...any) error
}

type Client struct {
	execer Execer
}

type JobOption func(*jobOpts)

type jobOpts struct {
	queue        string
	runAt        interface{}
	maxAttempts  int
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

func New(execer Execer) *Client {
	return &Client{execer: execer}
}

func Enqueue[T any](ctx context.Context, c *Client, args T, opts ...JobOption) (JobID, error) {
	return EnqueueTx(ctx, c, c.execer, args, opts...)
}

func EnqueueTx[T any](ctx context.Context, c *Client, tx Execer, args T, opts ...JobOption) (JobID, error) {
	jobOpts := &jobOpts{
		queue:       "default",
		maxAttempts: 20,
		runAt:       "now()",
	}

	for _, opt := range opts {
		opt(jobOpts)
	}

	id := JobID(uuid.New())

	payload, err := json.Marshal(args)
	if err != nil {
		return id, fmt.Errorf("marshal payload: %w", err)
	}

	sql := `INSERT INTO jobs (id, queue, job_type, payload_version, payload, state, attempt, max_attempts, run_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, ` + formatRunAt(jobOpts.runAt) + `)`

	args_list := []any{
		id, jobOpts.queue, "", 1, payload, "pending", 0, jobOpts.maxAttempts,
	}

	err = tx.Exec(ctx, sql, args_list...)
	if err != nil {
		return id, fmt.Errorf("enqueue: %w", err)
	}

	return id, nil
}

func formatRunAt(runAt interface{}) string {
	if s, ok := runAt.(string); ok && s == "now()" {
		return "now()"
	}
	return "$9"
}
