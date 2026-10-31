package testing

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func TestRunContainerRetriesReaperRace(t *testing.T) {
	calls := 0
	_, err := RunContainer(func() (*testcontainers.DockerContainer, error) {
		calls++
		if calls < 3 {
			return nil, errors.New(`create container: reaper: from container "bf0c926a": wait for reaper bf0c926a: unexpected container status "created"`)
		}
		return nil, nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, calls)
}

func TestRunContainerDoesNotRetryOtherErrors(t *testing.T) {
	calls := 0
	_, err := RunContainer(func() (*testcontainers.DockerContainer, error) {
		calls++
		return nil, errors.New("pull image: not found")
	})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestRunContainerDoesNotRetryPermanentReaperErrors(t *testing.T) {
	calls := 0
	_, err := RunContainer(func() (*testcontainers.DockerContainer, error) {
		calls++
		return nil, errors.New(`create container: reaper: from container "bf0c926a": wait for reaper bf0c926a: container exited`)
	})
	require.Error(t, err)
	require.Equal(t, 1, calls)
}

func TestRunContainerGivesUpAfterLastAttempt(t *testing.T) {
	calls := 0
	_, err := RunContainer(func() (*testcontainers.DockerContainer, error) {
		calls++
		return nil, errors.New(`create container: reaper: from container "bf0c926a": wait for reaper bf0c926a: unexpected container status "created"`)
	})
	require.Error(t, err)
	require.Equal(t, reaperRetryAttempts, calls)
}
