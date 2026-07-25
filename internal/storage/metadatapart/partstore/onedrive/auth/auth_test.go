package auth

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/onedrive"
	_ "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOAuthConfigUsesPermissionModeScope(t *testing.T) {
	tests := []struct {
		name  string
		mode  onedrive.PermissionMode
		scope string
	}{
		{
			name:  "full drive",
			mode:  onedrive.PermissionModeFullDrive,
			scope: onedrive.ScopeFilesReadWrite,
		},
		{
			name:  "app folder preview",
			mode:  onedrive.PermissionModeAppFolderPreview,
			scope: onedrive.ScopeFilesReadWriteAppFolder,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := OAuthConfig("", "client-id", tt.mode)

			require.NoError(t, err)
			assert.Equal(t, []string{tt.scope, "offline_access"}, cfg.Scopes)
			assert.Equal(t, "https://login.microsoftonline.com/consumers/oauth2/v2.0/devicecode", cfg.Endpoint.DeviceAuthURL)
		})
	}
}

func TestOAuthConfigRejectsInvalidPermissionMode(t *testing.T) {
	cfg, err := OAuthConfig("", "client-id", onedrive.PermissionMode("unknown"))

	assert.Nil(t, cfg)
	assert.EqualError(t, err, `invalid OneDrive permission mode "unknown": must be "fullDrive" or "appFolderPreview"`)
}
