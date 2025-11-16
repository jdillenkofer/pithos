package lifecycle

import (
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestStateValidatorDoubleStartReturnsErr(t *testing.T) {
	testutils.SkipIfIntegration(t)
	validator, err := New("DoubleStart")
	assert.Nil(t, err)

	err = validator.Start()
	assert.Nil(t, err)

	err = validator.Start()
	assert.NotNil(t, err)
}

func TestStateValidatorStopBeforeStartReturnsErr(t *testing.T) {
	testutils.SkipIfIntegration(t)
	validator, err := New("StopBeforeStart")
	assert.Nil(t, err)

	err = validator.Stop()
	assert.NotNil(t, err)
}

func TestStateValidatorDoubleStopReturnsErr(t *testing.T) {
	testutils.SkipIfIntegration(t)
	validator, err := New("DoubleStop")
	assert.Nil(t, err)

	err = validator.Start()
	assert.Nil(t, err)

	err = validator.Stop()
	assert.Nil(t, err)

	err = validator.Stop()
	assert.NotNil(t, err)
}

func TestStateValidatorCorrectUsageReturnsNoErr(t *testing.T) {
	testutils.SkipIfIntegration(t)
	validator, err := New("CorrectUsage")
	assert.Nil(t, err)

	err = validator.Start()
	assert.Nil(t, err)

	err = validator.Stop()
	assert.Nil(t, err)
}
