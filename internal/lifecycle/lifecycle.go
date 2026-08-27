package lifecycle

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type Component interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Name() string
}

type Supervisor struct {
	mu         sync.Mutex
	components []Component
	logger     *slog.Logger
}

func NewSupervisor(logger *slog.Logger) *Supervisor {
	return &Supervisor{
		components: make([]Component, 0),
		logger:     logger,
	}
}

func (s *Supervisor) Register(c Component) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.components = append(s.components, c)
}

func (s *Supervisor) StartAll(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i, c := range s.components {
		s.logger.Info("Starting component", "index", i, "name", c.Name())
		if err := c.Start(ctx); err != nil {
			return fmt.Errorf("start %s: %w", c.Name(), err)
		}
	}
	return nil
}

func (s *Supervisor) StopAll(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var errs []error
	for i := len(s.components) - 1; i >= 0; i-- {
		c := s.components[i]
		s.logger.Info("Stopping component", "index", i, "name", c.Name())
		if err := c.Stop(ctx); err != nil {
			s.logger.Error("Error stopping component", "name", c.Name(), "err", err)
			errs = append(errs, fmt.Errorf("stop %s: %w", c.Name(), err))
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("stop errors: %v", errs)
	}
	return nil
}

type ComponentBase struct {
	name string
}

func NewComponentBase(name string) ComponentBase {
	return ComponentBase{name: name}
}

func (c ComponentBase) Name() string {
	return c.name
}
