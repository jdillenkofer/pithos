package onedrive

import "fmt"

// PermissionMode controls the delegated Microsoft Graph permission requested
// by pithos while leaving the storage location inside the application's folder.
type PermissionMode string

const (
	// PermissionModeFullDrive uses the stable Files.ReadWrite permission.
	PermissionModeFullDrive PermissionMode = "fullDrive"
	// PermissionModeAppFolderPreview uses the least-privilege preview permission.
	PermissionModeAppFolderPreview PermissionMode = "appFolderPreview"
)

const (
	// ScopeFilesReadWrite grants access to the signed-in user's OneDrive files.
	ScopeFilesReadWrite = "Files.ReadWrite"
	// ScopeFilesReadWriteAppFolder confines access to the application's folder.
	ScopeFilesReadWriteAppFolder = "Files.ReadWrite.AppFolder"
)

// ParsePermissionMode validates and returns a configured permission mode.
func ParsePermissionMode(value string) (PermissionMode, error) {
	mode := PermissionMode(value)
	if _, err := mode.Scope(); err != nil {
		return "", err
	}
	return mode, nil
}

// Scope returns the delegated Microsoft Graph scope for the permission mode.
func (m PermissionMode) Scope() (string, error) {
	switch m {
	case PermissionModeFullDrive:
		return ScopeFilesReadWrite, nil
	case PermissionModeAppFolderPreview:
		return ScopeFilesReadWriteAppFolder, nil
	default:
		return "", fmt.Errorf(
			"invalid OneDrive permission mode %q: must be %q or %q",
			m,
			PermissionModeFullDrive,
			PermissionModeAppFolderPreview,
		)
	}
}
