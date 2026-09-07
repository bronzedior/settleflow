package lifecycle

import (
	"context"
)

type Component interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Name() string
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
