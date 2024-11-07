package startstopvalidator

import (
	"errors"
	"sync/atomic"
)

type StartStopValidator struct {
	isStarted atomic.Bool
	isStopped atomic.Bool
	name      string
}

func New(name string) (*StartStopValidator, error) {
	return &StartStopValidator{
		isStarted: atomic.Bool{},
		isStopped: atomic.Bool{},
		name:      name,
	}, nil
}

func (validator *StartStopValidator) Start() error {
	if !validator.isStarted.CompareAndSwap(false, true) {
		return errors.New(validator.name + " already started")
	}
	return nil
}

func (validator *StartStopValidator) Stop() error {
	if !validator.isStarted.Load() {
		return errors.New(validator.name + " not started")
	}
	if !validator.isStopped.CompareAndSwap(false, true) {
		return errors.New(validator.name + " already stopped")
	}
	return nil
}
