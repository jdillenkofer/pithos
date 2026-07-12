package config

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

func TestCanCreateSimulatorTapeDeviceOpenerFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tapePath := filepath.Join(t.TempDir(), "tape.sim")
	jsonData := fmt.Sprintf(`{
			 "type": "SimulatorTapeDevice",
			 "path": %s,
			 "capacityBytes": 4096,
			 "latency": "none"
		 }`, strconv.Quote(tapePath))

	instantiator, err := CreateTapeDeviceInstantiatorFromJson([]byte(jsonData))
	require.NoError(t, err)
	opener, err := instantiator.Instantiate(nil)
	require.NoError(t, err)

	device, err := opener(context.Background())
	require.NoError(t, err)
	require.NoError(t, device.Close())
}

func TestSimulatorTapeDeviceRejectsUnknownLatencyProfile(t *testing.T) {
	testutils.SkipIfIntegration(t)

	jsonData := `{
			 "type": "SimulatorTapeDevice",
			 "path": "./tape.sim",
			 "latency": "warpspeed"
		 }`

	instantiator, err := CreateTapeDeviceInstantiatorFromJson([]byte(jsonData))
	require.NoError(t, err)
	_, err = instantiator.Instantiate(nil)
	assert.ErrorContains(t, err, "unknown tape latency profile")
}

func TestCanCreateStTapeDeviceOpenerFromJson(t *testing.T) {
	testutils.SkipIfIntegration(t)

	jsonData := `{
			 "type": "StTapeDevice",
			 "path": "/dev/nst0"
		 }`

	instantiator, err := CreateTapeDeviceInstantiatorFromJson([]byte(jsonData))
	require.NoError(t, err)
	opener, err := instantiator.Instantiate(nil)
	require.NoError(t, err)
	assert.NotNil(t, opener)
}

func TestUnknownTapeDeviceTypeIsRejected(t *testing.T) {
	testutils.SkipIfIntegration(t)

	_, err := CreateTapeDeviceInstantiatorFromJson([]byte(`{"type": "FloppyTapeDevice"}`))
	assert.ErrorContains(t, err, "unknown tapeDevice type")
}
