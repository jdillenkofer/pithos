package server

import (
	"io"
	"net/http"
	"time"
)

// A blocked response write must not extend an external reader's lifetime.
// Inspect deadlines before tracing wrappers obscure the reader capability.
func boundObjectResponse(w http.ResponseWriter, readers []io.ReadCloser) (func(), error) {
	var deadline time.Time
	for _, reader := range readers {
		if bounded, ok := reader.(interface{ Deadline() time.Time }); ok {
			d := bounded.Deadline()
			if !d.IsZero() && (deadline.IsZero() || d.Before(deadline)) {
				deadline = d
			}
		}
	}
	if deadline.IsZero() {
		return func() {}, nil
	}
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(deadline); err != nil {
		return func() {}, err
	}
	return func() { _ = controller.SetWriteDeadline(time.Time{}) }, nil
}
