package queue

import (
	"fmt"
	"time"
)

type RetryableError interface {
	error
	Retryable() bool
}

type PermanentError interface {
	error
	Permanent() bool
}

type CheckpointError interface {
	error
	Checkpoint() []byte
	Resumable() bool
}

type PanicError interface {
	error
	IsPanic() bool
}

type TransportError interface {
	error
	IsTransport() bool
}

type retryableErr struct {
	err error
}

func (e *retryableErr) Error() string {
	return fmt.Sprintf("retryable: %v", e.err)
}

func (e *retryableErr) Retryable() bool {
	return true
}

func Retryable(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(RetryableError); ok {
		return err
	}
	return &retryableErr{err: err}
}

type permanentErr struct {
	err error
}

func (e *permanentErr) Error() string {
	return fmt.Sprintf("permanent: %v", e.err)
}

func (e *permanentErr) Permanent() bool {
	return true
}

func Permanent(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(PermanentError); ok {
		return err
	}
	return &permanentErr{err: err}
}

type checkpointErr struct {
	err        error
	checkpoint []byte
}

func (e *checkpointErr) Error() string {
	return fmt.Sprintf("resumable: %v", e.err)
}

func (e *checkpointErr) Checkpoint() []byte {
	return e.checkpoint
}

func (e *checkpointErr) Resumable() bool {
	return true
}

func Resume(checkpoint []byte) error {
	return &checkpointErr{
		err:        fmt.Errorf("resumable"),
		checkpoint: checkpoint,
	}
}

type transportErr struct {
	err error
}

func (e *transportErr) Error() string {
	return fmt.Sprintf("transport: %v", e.err)
}

func (e *transportErr) IsTransport() bool {
	return true
}

func Transport(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(TransportError); ok {
		return err
	}
	return &transportErr{err: err}
}

type panicErr struct {
	value interface{}
}

func (e *panicErr) Error() string {
	return fmt.Sprintf("panic: %v", e.value)
}

func (e *panicErr) IsPanic() bool {
	return true
}

func Panic(v interface{}) error {
	return &panicErr{value: v}
}

func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ""
	}

	if _, ok := err.(PanicError); ok {
		return ErrorClassPanic
	}
	if _, ok := err.(CheckpointError); ok {
		return ErrorClassResumable
	}
	if _, ok := err.(TransportError); ok {
		return ErrorClassTransport
	}
	if _, ok := err.(PermanentError); ok {
		return ErrorClassPermanent
	}
	if _, ok := err.(RetryableError); ok {
		return ErrorClassRetryable
	}

	return ErrorClassRetryable
}

func ShouldRefundAttempt(errClass ErrorClass) bool {
	return errClass == ErrorClassTransport || errClass == ErrorClassResumable
}

func CalculateBackoff(attempt int, retryBase time.Duration, retryCap time.Duration) time.Duration {
	if attempt <= 0 {
		return retryBase
	}

	exponential := retryBase
	for i := 1; i < attempt; i++ {
		exponential *= 2
		if exponential > retryCap {
			exponential = retryCap
			break
		}
	}

	return exponential
}
