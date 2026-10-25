package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestRequestContextAllowsHardResponseWriteDeadline(t *testing.T) {
	testutils.SkipIfIntegration(t)
	result := make(chan error, 1)
	server := httptest.NewServer(MakeRequestContextMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		controller := http.NewResponseController(w)
		if err := controller.SetWriteDeadline(time.Now().Add(-time.Second)); err != nil {
			result <- err
			return
		}
		// Exceed net/http's buffer so the write must reach the expired connection.
		_, err := w.Write(make([]byte, 1<<20))
		result <- err
	})))
	defer server.Close()
	response, err := http.Get(server.URL)
	if err == nil {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}
	require.ErrorIs(t, <-result, os.ErrDeadlineExceeded)
}
