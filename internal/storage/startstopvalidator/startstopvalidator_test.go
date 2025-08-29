package startstopvalidator

import (
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestStartStopValidatorDoubleStartReturnsErr(t *testing.T) {
	testutils.SkipIfIntegration(t)
	startStopValidator, err := New("DoubleStart")
	assert.Nil(t, err)

	err = startStopValidator.Start()
	assert.Nil(t, err)

	err = startStopValidator.Start()
	assert.NotNil(t, err)
}

func TestStartStopValidatorStopBeforeStartReturnsErr(t *testing.T) {
	testutils.SkipIfIntegration(t)
	startStopValidator, err := New("StopBeforeStart")
	assert.Nil(t, err)

	err = startStopValidator.Stop()
	assert.NotNil(t, err)
}

func TestStartStopValidatorDoubleStopReturnsErr(t *testing.T) {
	testutils.SkipIfIntegration(t)
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
	testutils.SkipIfIntegration(t)
	startStopValidator, err := New("CorrectUsage")
	assert.Nil(t, err)

	err = startStopValidator.Start()
	assert.Nil(t, err)

	err = startStopValidator.Stop()
	assert.Nil(t, err)
}
