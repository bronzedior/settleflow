package queue

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type JobID = uuid.UUID

func NewJobID() JobID {
	return JobID(uuid.New())
}

type Job struct {
	ID              JobID           `db:"id"`
	Queue           string          `db:"queue"`
	JobType         string          `db:"job_type"`
	PayloadVersion  int             `db:"payload_version"`
	Payload         json.RawMessage `db:"payload"`
	State           string          `db:"state"`
	Attempt         int             `db:"attempt"`
	MaxAttempts     int             `db:"max_attempts"`
	RunAt           time.Time       `db:"run_at"`
	ClaimedAt       *time.Time      `db:"claimed_at"`
	ClaimedBy       *string         `db:"claimed_by"`
	HeartbeatAt     *time.Time      `db:"heartbeat_at"`
	LastError       *string         `db:"last_error"`
	LastErrorClass  *string         `db:"last_error_class"`
	Checkpoint      json.RawMessage `db:"checkpoint"`
	CreatedAt       time.Time       `db:"created_at"`
	UpdatedAt       time.Time       `db:"updated_at"`
}

type JobState string

const (
	StatePending JobState = "pending"
	StateClaimed JobState = "claimed"
	StateDead    JobState = "dead"
)

type ErrorClass string

const (
	ErrorClassRetryable  ErrorClass = "retryable"
	ErrorClassPermanent  ErrorClass = "permanent"
	ErrorClassTransport  ErrorClass = "transport"
	ErrorClassResumable  ErrorClass = "resumable"
	ErrorClassPanic      ErrorClass = "panic"
)
