package buildinfo

import (
	"runtime/debug"
	"testing"

	_ "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestResolvePrefersExplicitBuildMetadata(t *testing.T) {
	goBuildInfo := &debug.BuildInfo{
		Main: debug.Module{Version: "v1.0.0"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "toolchain-commit"},
			{Key: "vcs.modified", Value: "true"},
		},
	}

	info := resolve(goBuildInfo, "v2.0.0", "release-commit", "false")

	assert.Equal(t, Info{
		Version:    "v2.0.0",
		Commit:     "release-commit",
		Dirty:      false,
		DirtyKnown: true,
	}, info)
}

func TestResolveFallsBackToGoVCMetadata(t *testing.T) {
	goBuildInfo := &debug.BuildInfo{
		Main: debug.Module{Version: "(devel)"},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: "local-commit"},
			{Key: "vcs.modified", Value: "true"},
		},
	}

	info := resolve(goBuildInfo, "", "", "")

	assert.Equal(t, Info{
		Version:    "devel",
		Commit:     "local-commit",
		Dirty:      true,
		DirtyKnown: true,
	}, info)
}

func TestResolveReportsUnknownMetadata(t *testing.T) {
	info := resolve(nil, "", "", "")

	assert.Equal(t, Info{
		Version: "devel",
		Commit:  "unknown",
	}, info)
}
