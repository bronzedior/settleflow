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
	State      JobState
	ErrorMsg   string
	ErrorClass ErrorClass
	RunAt      time.Time
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
			State: StateDead,
		}
	}

	errClass := ClassifyError(err)
	errMsg := err.Error()

	if job.Attempt >= maxAttempts {
		return ExecutionResult{
			State:      StateDead,
			ErrorMsg:   errMsg,
			ErrorClass: errClass,
		}
	}

	switch errClass {
	case ErrorClassTransport, ErrorClassRetryable, ErrorClassPanic:
		backoff := CalculateBackoff(job.Attempt, e.config.RetryBase, e.config.RetryCap)
		return ExecutionResult{
			State:      StatePending,
			ErrorMsg:   errMsg,
			ErrorClass: errClass,
			RunAt:      time.Now().Add(backoff),
		}

	case ErrorClassPermanent:
		return ExecutionResult{
			State:      StateDead,
			ErrorMsg:   errMsg,
			ErrorClass: errClass,
		}

	default:
		return ExecutionResult{
			State:      StateDead,
			ErrorMsg:   errMsg,
			ErrorClass: errClass,
		}
	}
}
