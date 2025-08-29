package testing

import (
	"flag"
	"testing"
)

var (
	Integration = flag.Bool("integration", false, "run integration tests")
	DBType      = flag.String("db", "sqlite", "database type to use (sqlite or postgres)")
	PathStyle   = flag.String("path-style", "host", "addressing style to use (host or path)")
	ReplMode    = flag.String("repl-mode", "none", "replication mode to use (none or replicated)")
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
