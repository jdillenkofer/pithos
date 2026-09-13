package settings

import (
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func addrOf[T any](t T) *T { return &t }

func TestMergeSettingsTwoNils(t *testing.T) {
	testutils.SkipIfIntegration(t)

	a := Settings{
		domain: nil,
	}
	b := Settings{
		domain: nil,
	}
	mergedSettings := mergeSettings(&a, &b)
	assert.NotNil(t, mergedSettings)
	assert.Nil(t, a.domain)
	assert.Nil(t, b.domain)
	assert.Nil(t, mergedSettings.domain)
}

func TestMergeSettingsNilAndValue(t *testing.T) {
	testutils.SkipIfIntegration(t)

	a := Settings{
		domain: nil,
	}
	b := Settings{
		domain: addrOf("test"),
	}
	mergedSettings := mergeSettings(&a, &b)
	assert.NotNil(t, mergedSettings)
	assert.Nil(t, a.domain)
	assert.Equal(t, "test", *b.domain)
	assert.Equal(t, b.domain, mergedSettings.domain)
}

func TestMergeSettingsTwoValues(t *testing.T) {
	testutils.SkipIfIntegration(t)

	a := Settings{
		domain: addrOf("test"),
	}
	b := Settings{
		domain: addrOf("test2"),
	}
	mergedSettings := mergeSettings(&a, &b)
	assert.NotNil(t, mergedSettings)
	assert.Equal(t, "test", *a.domain)
	assert.Equal(t, "test2", *b.domain)
	assert.Equal(t, b.domain, mergedSettings.domain)
}

func TestSpoolDirDefaultsToEmptyOverride(t *testing.T) {
	testutils.SkipIfIntegration(t)

	settings := &Settings{}

	assert.Empty(t, settings.SpoolDir())
}

func TestLoadSpoolDirFromCmdArgs(t *testing.T) {
	testutils.SkipIfIntegration(t)

	settings, err := loadSettingsFromCmdArgs([]string{"-spoolDir", "/var/tmp/pithos"})

	assert.NoError(t, err)
	assert.Equal(t, "/var/tmp/pithos", settings.SpoolDir())
}

func TestLoadSpoolDirFromEnv(t *testing.T) {
	testutils.SkipIfIntegration(t)
	t.Setenv(spoolDirEnvKey, "/var/tmp/pithos")

	settings, err := loadSettingsFromEnv()

	assert.NoError(t, err)
	assert.Equal(t, "/var/tmp/pithos", settings.SpoolDir())
}

func TestCredentialFileSettings(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		settings := &Settings{}
		assert.Empty(t, settings.CredentialsPath())
		assert.Equal(t, 5, settings.CredentialsReloadIntervalSeconds())
	})

	t.Run("environment", func(t *testing.T) {
		t.Setenv(credentialsPathEnvKey, "/run/secrets/pithos-credentials.json")
		t.Setenv(credentialsReloadIntervalSecondsEnvKey, "12")
		settings, err := loadSettingsFromEnv()
		assert.NoError(t, err)
		assert.Equal(t, "/run/secrets/pithos-credentials.json", settings.CredentialsPath())
		assert.Equal(t, 12, settings.CredentialsReloadIntervalSeconds())
	})

	t.Run("arguments", func(t *testing.T) {
		settings, err := loadSettingsFromCmdArgs([]string{"-credentialsPath", "/run/credentials.json", "-credentialsReloadIntervalSeconds", "3"})
		assert.NoError(t, err)
		assert.Equal(t, "/run/credentials.json", settings.CredentialsPath())
		assert.Equal(t, 3, settings.CredentialsReloadIntervalSeconds())
	})
}

func TestSQLCredentialSettings(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		settings := &Settings{}
		assert.Equal(t, "auto", settings.CredentialsProvider())
		assert.Zero(t, settings.CredentialsDatabaseIndex())
	})

	t.Run("environment", func(t *testing.T) {
		t.Setenv(credentialsProviderEnvKey, "sql")
		t.Setenv(credentialsDatabaseIndexEnvKey, "2")
		settings, err := loadSettingsFromEnv()
		assert.NoError(t, err)
		assert.Equal(t, "sql", settings.CredentialsProvider())
		assert.Equal(t, 2, settings.CredentialsDatabaseIndex())
	})

	t.Run("arguments", func(t *testing.T) {
		settings, err := loadSettingsFromCmdArgs([]string{"-credentialsProvider", "sql", "-credentialsDatabaseIndex", "1"})
		assert.NoError(t, err)
		assert.Equal(t, "sql", settings.CredentialsProvider())
		assert.Equal(t, 1, settings.CredentialsDatabaseIndex())
	})
}
