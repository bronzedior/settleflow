package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
)

type HandlerFunc func(ctx context.Context, meta JobMeta) error

type handlerEntry struct {
	jobType       string
	version       int
	unmarshalFunc func([]byte) (interface{}, error)
	handlerFunc   HandlerFunc
	maxAttempts   int
}

type Registry struct {
	mu       sync.RWMutex
	handlers map[string]map[int]*handlerEntry
	logger   *slog.Logger
}

func NewRegistry(logger *slog.Logger) *Registry {
	if logger == nil {
		logger = slog.Default()
	}
	return &Registry{
		handlers: make(map[string]map[int]*handlerEntry),
		logger:   logger,
	}
}

func Register[T any](
	r *Registry,
	jobType string,
	version int,
	maxAttempts int,
	fn func(ctx context.Context, args T) error,
) error {
	if r == nil {
		return fmt.Errorf("registry is nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[jobType]; !exists {
		r.handlers[jobType] = make(map[int]*handlerEntry)
	}

	if _, exists := r.handlers[jobType][version]; exists {
		return fmt.Errorf("handler already registered for %s version %d", jobType, version)
	}

	unmarshalFunc := func(data []byte) (interface{}, error) {
		var args T
		if err := json.Unmarshal(data, &args); err != nil {
			return nil, err
		}
		return args, nil
	}

	handlerFunc := func(ctx context.Context, meta JobMeta) error {
		args, err := unmarshalFunc(meta.Payload)
		if err != nil {
			return Permanent(fmt.Errorf("unmarshal payload: %w", err))
		}
		return fn(ctx, args.(T))
	}

	r.handlers[jobType][version] = &handlerEntry{
		jobType:       jobType,
		version:       version,
		unmarshalFunc: unmarshalFunc,
		handlerFunc:   handlerFunc,
		maxAttempts:   maxAttempts,
	}

	r.logger.Debug("Handler registered", "jobType", jobType, "version", version)
	return nil
}

func (r *Registry) Handler(jobType string, version int) (HandlerFunc, int, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, exists := r.handlers[jobType]
	if !exists {
		return nil, 0, fmt.Errorf("no handlers for job type: %s", jobType)
	}

	entry, exists := versions[version]
	if !exists {
		return nil, 0, fmt.Errorf("no handler for %s version %d", jobType, version)
	}

	return entry.handlerFunc, entry.maxAttempts, nil
}

func (r *Registry) Handlers() map[string]map[int]*handlerEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.handlers
}
