package onedrive

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPermissionModeScope(t *testing.T) {
	tests := []struct {
		name  string
		value string
		mode  PermissionMode
		scope string
	}{
		{
			name:  "full drive",
			value: "fullDrive",
			mode:  PermissionModeFullDrive,
			scope: ScopeFilesReadWrite,
		},
		{
			name:  "app folder preview",
			value: "appFolderPreview",
			mode:  PermissionModeAppFolderPreview,
			scope: ScopeFilesReadWriteAppFolder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mode, err := ParsePermissionMode(tt.value)
			require.NoError(t, err)
			assert.Equal(t, tt.mode, mode)

			scope, err := mode.Scope()
			require.NoError(t, err)
			assert.Equal(t, tt.scope, scope)
		})
	}
}

func TestParsePermissionModeRejectsUnknownValue(t *testing.T) {
	mode, err := ParsePermissionMode("unknown")

	assert.Empty(t, mode)
	assert.EqualError(t, err, `invalid OneDrive permission mode "unknown": must be "fullDrive" or "appFolderPreview"`)
}
