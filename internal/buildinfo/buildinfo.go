package buildinfo

import (
	"runtime"
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
	date    string
)

// Info describes the source used to build the running binary.
type Info struct {
	Version    string
	Commit     string
	Dirty      bool
	DirtyKnown bool
	Date       string
	GoVersion  string
}

// Current returns the build metadata embedded in the running binary.
func Current() Info {
	goBuildInfo, _ := debug.ReadBuildInfo()
	return resolve(goBuildInfo, version, commit, dirty, date)
}

func resolve(goBuildInfo *debug.BuildInfo, embeddedVersion, embeddedCommit, embeddedDirty, embeddedDate string) Info {
	info := Info{
		Version: embeddedVersion,
		Commit:  embeddedCommit,
		Date:    embeddedDate,
	}

	var vcsModified string
	var vcsTime string
	if goBuildInfo != nil {
		if info.Version == "" && goBuildInfo.Main.Version != "" && goBuildInfo.Main.Version != "(devel)" {
			info.Version = goBuildInfo.Main.Version
		}

		if goBuildInfo.GoVersion != "" {
			info.GoVersion = goBuildInfo.GoVersion
		}

		for _, setting := range goBuildInfo.Settings {
			switch setting.Key {
			case "vcs.revision":
				if info.Commit == "" {
					info.Commit = setting.Value
				}
			case "vcs.modified":
				vcsModified = setting.Value
			case "vcs.time":
				vcsTime = setting.Value
			}
		}
	}

	if info.Version == "" {
		info.Version = "devel"
	}
	if info.Commit == "" {
		info.Commit = "unknown"
	}
	if info.Date == "" {
		info.Date = vcsTime
	}
	if info.Date == "" {
		info.Date = "unknown"
	}
	if info.GoVersion == "" {
		info.GoVersion = runtime.Version()
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
