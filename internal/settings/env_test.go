package settings

import (
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestBooleanEnvironmentParsing(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, value := range []string{"true", "TRUE", "True", "tRuE", "t", "T", "1", "false", "FALSE", "False", "f", "F", "0"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv(authenticationEnabledEnvKey, value)
			got, err := getBoolFromEnv(authenticationEnabledEnvKey)
			require.NoError(t, err)
			require.NotNil(t, got)
			want := value == "true" || value == "TRUE" || value == "True" || value == "tRuE" || value == "t" || value == "T" || value == "1"
			require.Equal(t, want, *got)
		})
	}
	for _, value := range []string{"tru", "yes", "no", "2", " true", "false ", " "} {
		t.Run("invalid_"+value, func(t *testing.T) {
			t.Setenv(authenticationEnabledEnvKey, value)
			got, err := getBoolFromEnv(authenticationEnabledEnvKey)
			require.ErrorContains(t, err, authenticationEnabledEnvKey)
			require.Nil(t, got)
		})
	}
}

func TestIntegerEnvironmentParsing(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		value string
		want  int
	}{
		{"0", 0}, {"9000", 9000}, {"-1", -1}, {"2147483647", 2147483647}, {"-2147483648", -2147483648},
	} {
		t.Run(tc.value, func(t *testing.T) {
			t.Setenv(portEnvKey, tc.value)
			got, err := getIntFromEnv(portEnvKey)
			require.NoError(t, err)
			require.NotNil(t, got)
			require.Equal(t, tc.want, *got)
		})
	}
	for _, value := range []string{"abc", "1.5", "2147483648", "-2147483649", " 9000", "9000 ", " "} {
		t.Run("invalid_"+value, func(t *testing.T) {
			t.Setenv(portEnvKey, value)
			got, err := getIntFromEnv(portEnvKey)
			require.ErrorContains(t, err, portEnvKey)
			require.Nil(t, got)
		})
	}
}

func TestEmptyEnvironmentValuesPreserveDefaults(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv(authenticationEnabledEnvKey, "")
	t.Setenv(portEnvKey, "")
	boolean, err := getBoolFromEnv(authenticationEnabledEnvKey)
	require.NoError(t, err)
	require.Nil(t, boolean)
	integer, err := getIntFromEnv(portEnvKey)
	require.NoError(t, err)
	require.Nil(t, integer)
}

func TestLoadSettingsRejectsInvalidEnvironment(t *testing.T) {
	testutils.SkipIfIntegration(t)
	keys := []string{
		authenticationEnabledEnvKey, monitoringPortEnabledEnvKey, trustForwardedHeadersEnvKey, otelEnabledEnvKey,
		credentialsReloadIntervalSecondsEnvKey, credentialsDatabaseIndexEnvKey, portEnvKey, monitoringPortEnvKey,
		authorizerTimeoutMillisecondsEnvKey, policyReloadIntervalSecondsEnvKey, metricsGaugesIntervalSecondsEnvKey,
	}
	for _, key := range keys {
		t.Setenv(key, "")
	}
	for _, key := range keys {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "invalid")
			settings, err := LoadSettings(nil)
			require.ErrorContains(t, err, key)
			require.Nil(t, settings)
		})
	}
}
