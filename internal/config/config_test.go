package config

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCanCreateStringProviderFromRawStringJson(t *testing.T) {
	jsonData := `"String"`
	stringProvider := StringProvider{}
	err := json.Unmarshal([]byte(jsonData), &stringProvider)
	assert.Nil(t, err)
	assert.Equal(t, "String", stringProvider.Value())
}

func TestCanCreateStringProviderFromEnvKeyStringJson(t *testing.T) {
	jsonData := `{
	  "type": "EnvKey",
	  "envKey": "PITHOS_ENV_KEY_STRING_TEST"
	}`
	err := os.Setenv("PITHOS_ENV_KEY_STRING_TEST", "EnvString")
	assert.Nil(t, err)
	stringProvider := StringProvider{}
	err = json.Unmarshal([]byte(jsonData), &stringProvider)
	assert.Nil(t, err)
	assert.Equal(t, "EnvString", stringProvider.Value())
}

func TestCanCreateInt64ProviderFromRawInt64Json(t *testing.T) {
	jsonData := `1`
	int64Provider := Int64Provider{}
	err := json.Unmarshal([]byte(jsonData), &int64Provider)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), int64Provider.Value())
}

func TestCanCreateInt64ProviderFromEnvKeyStringJson(t *testing.T) {
	jsonData := `{
	  "type": "EnvKey",
	  "envKey": "PITHOS_ENV_KEY_INT64_TEST"
	}`
	err := os.Setenv("PITHOS_ENV_KEY_INT64_TEST", "2")
	assert.Nil(t, err)
	int64Provider := Int64Provider{}
	err = json.Unmarshal([]byte(jsonData), &int64Provider)
	assert.Nil(t, err)
	assert.Equal(t, int64(2), int64Provider.Value())
}
