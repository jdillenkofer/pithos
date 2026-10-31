package testing

import (
	"errors"
	"strings"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

const (
	reaperRetryAttempts = 5
	reaperRetryDelay    = 500 * time.Millisecond
)

// RunContainer starts a container with run and retries when the start fails
// while attaching to the Ryuk reaper.
//
// All test binaries of one `go test` invocation share a reaper, because
// testcontainers-go derives the session ID from the parent process. When one
// package creates the reaper, a package starting concurrently can find it while
// it is still in the "created" state. testcontainers-go v0.44.0 then fails
// immediately with `wait for reaper ...: unexpected container status "created"`
// instead of waiting, and treats that error as permanent. A short retry finds
// the reaper running.
//
// The reaper is attached after the requested container was created, so every
// failed attempt returns a created container. It is terminated before the next
// attempt and after the last one, because callers only register cleanup for a
// successful start.
//
// Remove this once testcontainers-go waits for a created reaper to start.
func RunContainer[C testcontainers.Container](run func() (C, error)) (C, error) {
	var zero C
	var err error
	for attempt := range reaperRetryAttempts {
		if attempt > 0 {
			time.Sleep(reaperRetryDelay)
		}
		var ctr C
		ctr, err = run()
		if err == nil || !isReaperRace(err) {
			return ctr, err
		}
		if terminateErr := testcontainers.TerminateContainer(ctr); terminateErr != nil {
			return zero, errors.Join(err, terminateErr)
		}
	}
	return zero, err
}

// isReaperRace reports whether err is the transient failure of finding the
// reaper still in the "created" state. Other reaper failures are permanent.
// The library wraps the error without a sentinel, so match its text.
func isReaperRace(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "wait for reaper") && strings.Contains(msg, `unexpected container status "created"`)
}
