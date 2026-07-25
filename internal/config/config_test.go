package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestCanCreateStringProviderFromRawStringJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	jsonData := `"String"`
	stringProvider := StringProvider{}
	err := json.Unmarshal([]byte(jsonData), &stringProvider)
	assert.Nil(t, err)
	assert.Equal(t, "String", stringProvider.Value())
	assert.False(t, stringProvider.CanPersistValue())
}

func TestCanCreateStringProviderFromEnvKeyStringJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
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
	assert.False(t, stringProvider.CanPersistValue())
	assert.Error(t, stringProvider.WriteValue("updated"))
	assert.Equal(t, "EnvString", os.Getenv("PITHOS_ENV_KEY_STRING_TEST"))
}

func TestCanCreateStringProviderFromFileJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir := t.TempDir()
	tempFile := tempDir + string(os.PathSeparator) + "token.json"
	err := os.WriteFile(tempFile, []byte("FileString"), 0o600)
	assert.Nil(t, err)
	jsonData, err := json.Marshal(map[string]string{
		"type": "File",
		"path": tempFile,
	})
	assert.Nil(t, err)
	stringProvider := StringProvider{}
	err = json.Unmarshal(jsonData, &stringProvider)
	assert.Nil(t, err)
	assert.Equal(t, "FileString", stringProvider.Value())
	assert.True(t, stringProvider.CanPersistValue())
}

func TestFileStringProviderPersistsValueAtomicallyWithRestrictedPermissions(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "token.json")
	err := os.WriteFile(tempFile, []byte("old-token"), 0o644)
	assert.NoError(t, err)
	jsonData, err := json.Marshal(map[string]string{
		"type": "File",
		"path": tempFile,
	})
	assert.NoError(t, err)
	stringProvider := StringProvider{}
	err = json.Unmarshal(jsonData, &stringProvider)
	assert.NoError(t, err)

	err = stringProvider.WriteValue("new-token")

	assert.NoError(t, err)
	assert.Equal(t, "new-token", stringProvider.Value())
	persisted, err := os.ReadFile(tempFile)
	assert.NoError(t, err)
	assert.Equal(t, "new-token", string(persisted))
	if runtime.GOOS != "windows" {
		info, statErr := os.Stat(tempFile)
		assert.NoError(t, statErr)
		assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
	}
	temporaryFiles, err := filepath.Glob(filepath.Join(tempDir, ".token.json.tmp-*"))
	assert.NoError(t, err)
	assert.Empty(t, temporaryFiles)
}

func TestCanCreateInt64ProviderFromRawInt64Json(t *testing.T) {
	testutils.SkipIfIntegration(t)
	jsonData := `1`
	int64Provider := Int64Provider{}
	err := json.Unmarshal([]byte(jsonData), &int64Provider)
	assert.Nil(t, err)
	assert.Equal(t, int64(1), int64Provider.Value())
}

func TestCanCreateInt64ProviderFromEnvKeyStringJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
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

func TestCanCreateFloat64ProviderFromRawFloat64Json(t *testing.T) {
	testutils.SkipIfIntegration(t)
	jsonData := `1.5`
	float64Provider := Float64Provider{}
	err := json.Unmarshal([]byte(jsonData), &float64Provider)
	assert.Nil(t, err)
	assert.Equal(t, 1.5, float64Provider.Value())
}

func TestCannotCreateFloat64ProviderFromRawStringJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	jsonData := `"2.75"`
	float64Provider := Float64Provider{}
	err := json.Unmarshal([]byte(jsonData), &float64Provider)
	assert.NotNil(t, err)
}

func TestCanCreateFloat64ProviderFromEnvKeyJson(t *testing.T) {
	testutils.SkipIfIntegration(t)
	jsonData := `{
	  "type": "EnvKey",
	  "envKey": "PITHOS_ENV_KEY_FLOAT64_TEST"
	}`
	err := os.Setenv("PITHOS_ENV_KEY_FLOAT64_TEST", "3.25")
	assert.Nil(t, err)
	float64Provider := Float64Provider{}
	err = json.Unmarshal([]byte(jsonData), &float64Provider)
	assert.Nil(t, err)
	assert.Equal(t, 3.25, float64Provider.Value())
}
