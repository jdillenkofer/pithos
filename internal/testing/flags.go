package testing

import (
	"flag"
	"os"
	"runtime"
	"testing"
)

var (
	Integration         = flag.Bool("integration", false, "run integration tests")
	DBType              = flag.String("db", "sqlite", "database type to use (sqlite or postgres)")
	PathStyle           = flag.String("path-style", "host", "addressing style to use (host or path)")
	ReplMode            = flag.String("repl-mode", "none", "replication mode to use (none or replicated)")
	BlobStore           = flag.String("blob-store", "sql", "blob store to use (sql or filesystem)")
	BlobStoreEncryption = flag.String("blob-store-encryption", "none", "blob store encryption to use (none or tink)")
)

// SkipIfIntegration skips the test if -integration flag is set (for unit tests)
func SkipIfIntegration(t *testing.T) {
	if *Integration {
		t.Skip("Skipping unit test when running integration tests")
	}
}

// SkipIfNotIntegration skips the test if -integration flag is not set (for integration tests)
func SkipIfNotIntegration(t *testing.T) {
	if !*Integration {
		t.Skip("Skipping integration test")
	}
}

// SkipOnWindowsInGitHubActions skips the test if it is running on Windows in GitHub Actions
func SkipOnWindowsInGitHubActions(t *testing.T) {
	if runtime.GOOS == "windows" && os.Getenv("GITHUB_ACTIONS") == "true" {
		t.Skip("Skipping test on Windows in GitHub Actions")
	}
}

// SkipOnMacOSInGitHubActions skips the test if it is running on macOS in GitHub Actions
func SkipOnMacOSInGitHubActions(t *testing.T) {
	if runtime.GOOS == "darwin" && os.Getenv("GITHUB_ACTIONS") == "true" {
		t.Skip("Skipping test on macOS in GitHub Actions")
	}
}
