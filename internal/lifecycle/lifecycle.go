package lifecycle

import (
	"context"
	"errors"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// Manager defines the lifecycle management interface
type Manager interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// ValidatedLifecycle provides lifecycle validation for implementations.
// Embed this struct in implementations that need Start/Stop validation
// but delegate actual lifecycle work to wrapped components.
type ValidatedLifecycle struct {
	validator *StateValidator
	tracer    trace.Tracer
}

func NewValidatedLifecycle(name string) (*ValidatedLifecycle, error) {
	validator, err := New(name)
	if err != nil {
		return nil, err
	}
	return &ValidatedLifecycle{
		validator: validator,
		tracer:    otel.Tracer("internal/lifecycle"),
	}, nil
}

func (vl *ValidatedLifecycle) Start(ctx context.Context) error {
	return vl.validator.Start()
}

func (vl *ValidatedLifecycle) Stop(ctx context.Context) error {
	return vl.validator.Stop()
}

type StateValidator struct {
	isStarted atomic.Bool
	isStopped atomic.Bool
	name      string
}

func New(name string) (*StateValidator, error) {
	return &StateValidator{
		isStarted: atomic.Bool{},
		isStopped: atomic.Bool{},
		name:      name,
	}, nil
}

func (validator *StateValidator) Start() error {
	if !validator.isStarted.CompareAndSwap(false, true) {
		return errors.New(validator.name + " already started")
	}
	return nil
}

func (validator *StateValidator) Stop() error {
	if !validator.isStarted.Load() {
		return errors.New(validator.name + " not started")
	}
	if !validator.isStopped.CompareAndSwap(false, true) {
		return errors.New(validator.name + " already stopped")
	}
	return nil
}
