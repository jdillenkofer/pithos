package buildinfo

import (
	"runtime"
	"runtime/debug"
	"testing"

	_ "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestResolvePrefersExplicitBuildMetadata(t *testing.T) {
	goBuildInfo := &debug.BuildInfo{
		Main:      debug.Module{Version: "v1.0.0"},
		GoVersion: "go1.99.0",
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "toolchain-commit"},
			{Key: "vcs.modified", Value: "true"},
			{Key: "vcs.time", Value: "2020-01-01T00:00:00Z"},
		},
	}

	info := resolve(goBuildInfo, "v2.0.0", "release-commit", "false", "2024-05-01T12:00:00Z")

	assert.Equal(t, Info{
		Version:    "v2.0.0",
		Commit:     "release-commit",
		Dirty:      false,
		DirtyKnown: true,
		Date:       "2024-05-01T12:00:00Z",
		GoVersion:  "go1.99.0",
	}, info)
}

func TestResolveFallsBackToGoVCMetadata(t *testing.T) {
	goBuildInfo := &debug.BuildInfo{
		Main:      debug.Module{Version: "(devel)"},
		GoVersion: "go1.22.1",
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "local-commit"},
			{Key: "vcs.modified", Value: "true"},
			{Key: "vcs.time", Value: "2023-06-15T08:30:00Z"},
		},
	}

	info := resolve(goBuildInfo, "", "", "", "")

	assert.Equal(t, Info{
		Version:    "devel",
		Commit:     "local-commit",
		Dirty:      true,
		DirtyKnown: true,
		Date:       "2023-06-15T08:30:00Z",
		GoVersion:  "go1.22.1",
	}, info)
}

func TestResolveReportsUnknownMetadata(t *testing.T) {
	info := resolve(nil, "", "", "", "")

	assert.Equal(t, Info{
		Version:   "devel",
		Commit:    "unknown",
		Date:      "unknown",
		GoVersion: runtime.Version(),
	}, info)
}
