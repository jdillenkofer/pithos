package startstopvalidator

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStartStopValidatorDoubleStartReturnsErr(t *testing.T) {
	startStopValidator, err := New("DoubleStart")
	assert.Nil(t, err)

	err = startStopValidator.Start()
	assert.Nil(t, err)

	err = startStopValidator.Start()
	assert.NotNil(t, err)
}

func TestStartStopValidatorStopBeforeStartReturnsErr(t *testing.T) {
	startStopValidator, err := New("StopBeforeStart")
	assert.Nil(t, err)

	err = startStopValidator.Stop()
	assert.NotNil(t, err)
}

func TestStartStopValidatorDoubleStopReturnsErr(t *testing.T) {
	startStopValidator, err := New("DoubleStop")
	assert.Nil(t, err)

	err = startStopValidator.Start()
	assert.Nil(t, err)

	err = startStopValidator.Stop()
	assert.Nil(t, err)

	err = startStopValidator.Stop()
	assert.NotNil(t, err)
}

func TestStartStopValidatorCorrectUsageReturnsNoErr(t *testing.T) {
	startStopValidator, err := New("CorrectUsage")
	assert.Nil(t, err)

	err = startStopValidator.Start()
	assert.Nil(t, err)

	err = startStopValidator.Stop()
	assert.Nil(t, err)
}
