package queue

import (
	"context"
	"log/slog"
	"time"
)

type ExecutorConfig struct {
	RetryBase time.Duration
	RetryCap  time.Duration
	Logger    *slog.Logger
	Store     *Store
	Registry  *Registry
}

type Executor struct {
	config *ExecutorConfig
}

func NewExecutor(config *ExecutorConfig) *Executor {
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Executor{config: config}
}

type ExecutionResult struct {
	State         JobState
	ErrorMsg      string
	ErrorClass    ErrorClass
	Checkpoint    []byte
	RunAt         time.Time
	RefundAttempt bool
}

func (e *Executor) Execute(ctx context.Context, job Job) ExecutionResult {
	handler, maxAttempts, err := e.config.Registry.Handler(job.JobType, job.PayloadVersion)
	if err != nil {
		e.config.Logger.Error("Handler not found", "jobType", job.JobType, "version", job.PayloadVersion)
		return ExecutionResult{
			State:      StateDead,
			ErrorMsg:   err.Error(),
			ErrorClass: ErrorClassPermanent,
		}
	}

	meta := JobMeta{
		ID:          job.ID,
		Attempt:     job.Attempt,
		MaxAttempts: maxAttempts,
		Checkpoint:  job.Checkpoint,
		EnqueuedAt:  job.CreatedAt,
		Payload:     job.Payload,
	}

	var handlerErr error
	func() {
		defer func() {
			if r := recover(); r != nil {
				handlerErr = Panic(r)
				e.config.Logger.Error("Handler panic", "jobID", job.ID, "panic", r)
			}
		}()
		handlerErr = handler(ctx, meta)
	}()

	return e.classifyResult(job, handlerErr, maxAttempts)
}

func (e *Executor) classifyResult(job Job, err error, maxAttempts int) ExecutionResult {
	if err == nil {
		return ExecutionResult{
			State: StateDead, // Special marker for success (will be archived)
		}
	}

	errClass := ClassifyError(err)
	errMsg := err.Error()

	if job.Attempt >= maxAttempts {
		return ExecutionResult{
			State:         StateDead,
			ErrorMsg:      errMsg,
			ErrorClass:    errClass,
			RefundAttempt: false,
		}
	}

	switch errClass {
	case ErrorClassResumable:
		checkpoint := e.extractCheckpoint(err)
		return ExecutionResult{
			State:         StatePending,
			ErrorMsg:      errMsg,
			ErrorClass:    errClass,
			Checkpoint:    checkpoint,
			RunAt:         time.Now(),
			RefundAttempt: true,
		}

	case ErrorClassTransport:
		backoff := CalculateBackoff(job.Attempt, e.config.RetryBase, e.config.RetryCap)
		return ExecutionResult{
			State:         StatePending,
			ErrorMsg:      errMsg,
			ErrorClass:    errClass,
			RunAt:         time.Now().Add(backoff),
			RefundAttempt: true,
		}

	case ErrorClassRetryable, ErrorClassPanic:
		backoff := CalculateBackoff(job.Attempt, e.config.RetryBase, e.config.RetryCap)
		return ExecutionResult{
			State:         StatePending,
			ErrorMsg:      errMsg,
			ErrorClass:    errClass,
			RunAt:         time.Now().Add(backoff),
			RefundAttempt: false,
		}

	case ErrorClassPermanent:
		return ExecutionResult{
			State:         StateDead,
			ErrorMsg:      errMsg,
			ErrorClass:    errClass,
			RefundAttempt: false,
		}

	default:
		return ExecutionResult{
			State:         StateDead,
			ErrorMsg:      errMsg,
			ErrorClass:    errClass,
			RefundAttempt: false,
		}
	}
}

func (e *Executor) extractCheckpoint(err error) []byte {
	if checkpointErr, ok := err.(CheckpointError); ok {
		return checkpointErr.Checkpoint()
	}
	return nil
}
