package buildinfo

import (
	"runtime/debug"
	"strconv"
)

// These values are populated with -ldflags=-X for release builds. When they
// are not set, Current falls back to the VCS metadata embedded by the Go
// toolchain.
var (
	version string
	commit  string
	dirty   string
)

// Info describes the source used to build the running binary.
type Info struct {
	Version    string
	Commit     string
	Dirty      bool
	DirtyKnown bool
}

// Current returns the build metadata embedded in the running binary.
func Current() Info {
	goBuildInfo, _ := debug.ReadBuildInfo()
	return resolve(goBuildInfo, version, commit, dirty)
}

func resolve(goBuildInfo *debug.BuildInfo, embeddedVersion, embeddedCommit, embeddedDirty string) Info {
	info := Info{
		Version: embeddedVersion,
		Commit:  embeddedCommit,
	}

	var vcsModified string
	if goBuildInfo != nil {
		if info.Version == "" && goBuildInfo.Main.Version != "" && goBuildInfo.Main.Version != "(devel)" {
			info.Version = goBuildInfo.Main.Version
		}

		for _, setting := range goBuildInfo.Settings {
			switch setting.Key {
			case "vcs.revision":
				if info.Commit == "" {
					info.Commit = setting.Value
				}
			case "vcs.modified":
				vcsModified = setting.Value
			}
		}
	}

	if info.Version == "" {
		info.Version = "devel"
	}
	if info.Commit == "" {
		info.Commit = "unknown"
	}

	dirtyValue := embeddedDirty
	if dirtyValue == "" {
		dirtyValue = vcsModified
	}
	if parsedDirty, err := strconv.ParseBool(dirtyValue); err == nil {
		info.Dirty = parsedDirty
		info.DirtyKnown = true
	}

	return info
}
