package graph

import (
	"fmt"
	"time"
)

type APIError struct {
	Status     int
	Code       string
	RetryAfter time.Duration
	err        error
}

func (e *APIError) Error() string {
	return fmt.Sprintf("graph: status %d code %q: %v", e.Status, e.Code, e.err)
}

func (e *APIError) Unwrap() error { return e.err }

func (e *APIError) Retryable() bool {
	switch {
	case e.Status == 429, e.Status >= 500:
		return true
	case e.Status == 403 && e.Code == "Authorization_RequestDenied":
		return false
	case e.Status == 404:
		return false
	default:
		return e.Status < 400 || e.Status >= 500
	}
}

func (e *APIError) NotFound() bool { return e.Status == 404 }
