package main

import (
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/buildinfo"
	"github.com/stretchr/testify/assert"
)

func TestPrintVersion(t *testing.T) {
	var output strings.Builder

	printVersion(&output, buildinfo.Info{
		Version:    "v1.2.3",
		Commit:     "0123456789abcdef",
		Dirty:      true,
		DirtyKnown: true,
		Date:       "2024-05-01T12:00:00Z",
		GoVersion:  "go1.22.1",
	})

	assert.Equal(t, "pithos v1.2.3+dirty\ncommit: 0123456789abcdef\nbuild date: 2024-05-01T12:00:00Z\ngo1.22.1\n", output.String())
}

func TestPrintVersionDoesNotDuplicateDirtySuffix(t *testing.T) {
	var output strings.Builder

	printVersion(&output, buildinfo.Info{
		Version:    "v1.2.3+dirty",
		Commit:     "0123456789abcdef",
		Dirty:      true,
		DirtyKnown: true,
		Date:       "2024-05-01T12:00:00Z",
		GoVersion:  "go1.22.1",
	})

	assert.Equal(t, "pithos v1.2.3+dirty\ncommit: 0123456789abcdef\nbuild date: 2024-05-01T12:00:00Z\ngo1.22.1\n", output.String())
}

func TestVersionWithDirtySuffix(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		expected string
	}{
		{name: "release", version: "v1.2.3", expected: "v1.2.3+dirty"},
		{name: "prerelease", version: "v1.2.3-rc.1", expected: "v1.2.3-rc.1+dirty"},
		{name: "build metadata", version: "v1.2.3+musl", expected: "v1.2.3+musl.dirty"},
		{name: "prerelease and build metadata", version: "v1.2.3-rc.1+musl", expected: "v1.2.3-rc.1+musl.dirty"},
		{name: "dirty build metadata", version: "v1.2.3+musl.dirty", expected: "v1.2.3+musl.dirty"},
		{name: "legacy dirty suffix", version: "v1.2.3-dirty", expected: "v1.2.3-dirty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, versionWithDirtySuffix(tt.version))
		})
	}
}
