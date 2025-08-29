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
