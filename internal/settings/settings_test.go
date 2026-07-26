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
