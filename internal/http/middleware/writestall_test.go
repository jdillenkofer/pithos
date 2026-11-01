package middleware

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

const testWriteStallTimeout = 200 * time.Millisecond

func TestWriteStallTimeoutKeepsSlowClientsThatMakeProgress(t *testing.T) {
	testutils.SkipIfIntegration(t)
	chunk := bytes.Repeat([]byte("x"), 1024)
	server := httptest.NewServer(MakeWriteStallTimeoutMiddleware(testWriteStallTimeout, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The response takes far longer than the stall timeout overall.
		for range 6 {
			if _, err := w.Write(chunk); err != nil {
				return
			}
			http.NewResponseController(w).Flush()
			time.Sleep(testWriteStallTimeout / 2)
		}
	})))
	defer server.Close()

	response, err := http.Get(server.URL)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Len(t, body, 6*len(chunk))
}

func TestWriteStallTimeoutKeepsSlowClientsOfLargeWrites(t *testing.T) {
	testutils.SkipIfIntegration(t)
	body := bytes.Repeat([]byte("x"), 16<<20)
	server := httptest.NewServer(MakeWriteStallTimeoutMiddleware(testWriteStallTimeout, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// One write that takes the client longer than the stall timeout.
		_, _ = w.Write(body)
	})))
	defer server.Close()

	response, err := http.Get(server.URL)
	require.NoError(t, err)
	defer response.Body.Close()
	received := 0
	buf := make([]byte, 128<<10)
	for {
		n, err := io.ReadFull(response.Body, buf)
		received += n
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		require.NoError(t, err)
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, len(body), received)
}

func TestWriteStallTimeoutDropsClientsThatStopReading(t *testing.T) {
	testutils.SkipIfIntegration(t)
	writeErr := make(chan error, 1)
	server := httptest.NewServer(MakeWriteStallTimeoutMiddleware(testWriteStallTimeout, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chunk := bytes.Repeat([]byte("x"), 1<<20)
		for {
			if _, err := w.Write(chunk); err != nil {
				writeErr <- err
				return
			}
		}
	})))
	defer server.Close()

	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n"))
	require.NoError(t, err)

	// The client never reads, so socket buffers fill and writes stall.
	select {
	case err := <-writeErr:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("stalled response was not aborted")
	}
}

func TestWriteStallTimeoutIgnoresSlowSources(t *testing.T) {
	testutils.SkipIfIntegration(t)
	body := bytes.Repeat([]byte("x"), 3*writeStallSliceSize)
	server := httptest.NewServer(MakeWriteStallTimeoutMiddleware(testWriteStallTimeout, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The client reads at once; only the source stalls past the timeout.
		_, _ = io.Copy(w, &slowReader{inner: bytes.NewReader(body), delay: 2 * testWriteStallTimeout})
	})))
	defer server.Close()

	response, err := http.Get(server.URL)
	require.NoError(t, err)
	defer response.Body.Close()
	received, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, body, received)
}

func TestWriteStallTimeoutIgnoresSlowSourcesBeforeFlush(t *testing.T) {
	testutils.SkipIfIntegration(t)
	chunk := bytes.Repeat([]byte("x"), 1024)
	server := httptest.NewServer(MakeWriteStallTimeoutMiddleware(testWriteStallTimeout, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write(chunk); err != nil {
			return
		}
		// The source stalls past the timeout before the handler flushes.
		time.Sleep(2 * testWriteStallTimeout)
		if err := http.NewResponseController(w).Flush(); err != nil {
			return
		}
		_, _ = w.Write(chunk)
	})))
	defer server.Close()

	response, err := http.Get(server.URL)
	require.NoError(t, err)
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Len(t, body, 2*len(chunk))
}

func TestWriteStallTimeoutDoesNotOutliveItsResponse(t *testing.T) {
	testutils.SkipIfIntegration(t)
	server := httptest.NewUnstartedServer(MakeWriteStallTimeoutMiddleware(testWriteStallTimeout, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	})))
	server.Config.MaxHeaderBytes = 1024
	server.Start()
	defer server.Close()

	conn, err := net.Dial("tcp", server.Listener.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	reader := bufio.NewReader(conn)
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\n\r\n"))
	require.NoError(t, err)
	response, err := http.ReadResponse(reader, nil)
	require.NoError(t, err)
	_, err = io.Copy(io.Discard, response.Body)
	require.NoError(t, err)
	require.NoError(t, response.Body.Close())

	// The next request on the connection arrives after the deadline the
	// first response armed, and the server answers it without a handler.
	time.Sleep(2 * testWriteStallTimeout)
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: test\r\nX-Large: " + strings.Repeat("x", 64<<10) + "\r\n\r\n"))
	require.NoError(t, err)
	response, err = http.ReadResponse(reader, nil)
	require.NoError(t, err)
	defer response.Body.Close()
	require.Equal(t, http.StatusRequestHeaderFieldsTooLarge, response.StatusCode)
}

type slowReader struct {
	inner io.Reader
	delay time.Duration
	read  int
}

func (r *slowReader) Read(p []byte) (int, error) {
	if r.read >= writeStallSliceSize {
		time.Sleep(r.delay)
		r.read = 0
	}
	n, err := r.inner.Read(p[:min(len(p), writeStallSliceSize-r.read)])
	r.read += n
	return n, err
}
