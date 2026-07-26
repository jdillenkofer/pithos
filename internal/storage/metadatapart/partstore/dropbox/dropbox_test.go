package dropbox

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

type fakeDropbox struct {
	mu              sync.Mutex
	files           map[string][]byte
	sessions        map[string][]byte
	nextSessionID   int
	sessionRequests int
}

func (f *fakeDropbox) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/2/files/create_folder_v2":
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{}`)
	case "/2/files/upload":
		var arg struct {
			Path string `json:"path"`
		}
		_ = json.Unmarshal([]byte(r.Header.Get("Dropbox-API-Arg")), &arg)
		f.files[arg.Path], _ = io.ReadAll(r.Body)
		io.WriteString(w, `{}`)
	case "/2/files/upload_session/start":
		f.nextSessionID++
		sessionID := strconv.Itoa(f.nextSessionID)
		f.sessions[sessionID], _ = io.ReadAll(r.Body)
		f.sessionRequests++
		json.NewEncoder(w).Encode(map[string]string{"session_id": sessionID})
	case "/2/files/upload_session/append_v2":
		var arg struct {
			Cursor struct {
				SessionID string `json:"session_id"`
				Offset    int64  `json:"offset"`
			} `json:"cursor"`
		}
		_ = json.Unmarshal([]byte(r.Header.Get("Dropbox-API-Arg")), &arg)
		if int64(len(f.sessions[arg.Cursor.SessionID])) != arg.Cursor.Offset {
			http.Error(w, "incorrect_offset", http.StatusConflict)
			return
		}
		chunk, _ := io.ReadAll(r.Body)
		f.sessions[arg.Cursor.SessionID] = append(f.sessions[arg.Cursor.SessionID], chunk...)
		f.sessionRequests++
		io.WriteString(w, `{}`)
	case "/2/files/upload_session/finish":
		var arg struct {
			Cursor struct {
				SessionID string `json:"session_id"`
				Offset    int64  `json:"offset"`
			} `json:"cursor"`
			Commit struct {
				Path string `json:"path"`
			} `json:"commit"`
		}
		_ = json.Unmarshal([]byte(r.Header.Get("Dropbox-API-Arg")), &arg)
		if int64(len(f.sessions[arg.Cursor.SessionID])) != arg.Cursor.Offset {
			http.Error(w, "incorrect_offset", http.StatusConflict)
			return
		}
		chunk, _ := io.ReadAll(r.Body)
		f.sessions[arg.Cursor.SessionID] = append(f.sessions[arg.Cursor.SessionID], chunk...)
		f.files[arg.Commit.Path] = f.sessions[arg.Cursor.SessionID]
		delete(f.sessions, arg.Cursor.SessionID)
		f.sessionRequests++
		io.WriteString(w, `{}`)
	case "/2/files/download":
		var arg struct {
			Path string `json:"path"`
		}
		_ = json.Unmarshal([]byte(r.Header.Get("Dropbox-API-Arg")), &arg)
		data, ok := f.files[arg.Path]
		if !ok {
			w.WriteHeader(409)
			io.WriteString(w, `{"error_summary":"path/not_found/"}`)
			return
		}
		start := 0
		partial := false
		if value := strings.TrimSuffix(strings.TrimPrefix(r.Header.Get("Range"), "bytes="), "-"); value != "" {
			start, _ = strconv.Atoi(value)
			if start >= len(data) {
				w.WriteHeader(416)
				return
			}
			partial = true
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)-start))
		if partial {
			w.WriteHeader(http.StatusPartialContent)
		}
		w.Write(data[start:])
	case "/2/files/list_folder":
		entries := make([]map[string]string, 0, len(f.files)+1)
		for name := range f.files {
			entries = append(entries, map[string]string{"name": name[strings.LastIndex(name, "/")+1:]})
		}
		entries = append(entries, map[string]string{"name": "not-a-part"})
		json.NewEncoder(w).Encode(map[string]any{"entries": entries, "cursor": "", "has_more": false})
	case "/2/files/delete_v2":
		var arg struct {
			Path string `json:"path"`
		}
		_ = json.NewDecoder(r.Body).Decode(&arg)
		if _, ok := f.files[arg.Path]; !ok {
			w.WriteHeader(409)
			io.WriteString(w, `{"error_summary":"path_lookup/not_found/"}`)
			return
		}
		delete(f.files, arg.Path)
		io.WriteString(w, `{}`)
	default:
		w.WriteHeader(404)
	}
}

func TestDropboxPartStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	fake := &fakeDropbox{files: map[string][]byte{}, sessions: map[string][]byte{}}
	server := httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	defer server.Close()
	store, err := New("/parts", Options{HTTPClient: server.Client(), APIEndpoint: server.URL + "/2", ContentEndpoint: server.URL + "/2"})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Stop(context.Background())) })

	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, store.PutPart(context.Background(), nil, *id, partstore.PutPartOptions{}, strings.NewReader("hello world")))

	reader, err := store.GetPart(context.Background(), nil, *id)
	require.NoError(t, err)
	seeker := reader.(io.ReadSeekCloser)
	_, err = seeker.Seek(6, io.SeekStart)
	require.NoError(t, err)
	data, err := io.ReadAll(seeker)
	require.NoError(t, err)
	require.Equal(t, "world", string(data))
	require.NoError(t, seeker.Close())

	ids, err := store.GetPartIds(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, []partstore.PartId{*id}, ids)
	require.NoError(t, store.DeletePart(context.Background(), nil, *id))
	require.NoError(t, store.DeletePart(context.Background(), nil, *id))
	_, err = store.GetPart(context.Background(), nil, *id)
	require.ErrorIs(t, err, partstore.ErrPartNotFound)
}

func TestDropboxPartStoreUsesUploadSessionForLargeParts(t *testing.T) {
	testutils.SkipIfIntegration(t)
	fake := &fakeDropbox{files: map[string][]byte{}, sessions: map[string][]byte{}}
	server := httptest.NewServer(http.HandlerFunc(fake.serveHTTP))
	defer server.Close()
	store, err := New("/parts", Options{HTTPClient: server.Client(), APIEndpoint: server.URL + "/2", ContentEndpoint: server.URL + "/2"})
	require.NoError(t, err)
	require.NoError(t, store.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, store.Stop(context.Background())) })
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	payload := strings.Repeat("x", uploadSessionChunkSize) + "end"

	require.NoError(t, store.PutPart(context.Background(), nil, *id, partstore.PutPartOptions{}, strings.NewReader(payload)))

	reader, err := store.GetPart(context.Background(), nil, *id)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, payload, string(data))
	require.Equal(t, 2, fake.sessionRequests)
	require.Empty(t, fake.sessions)
}

func TestDropboxPartStoreSupportsTxFreeOperations(t *testing.T) {
	testutils.SkipIfIntegration(t)
	store, err := New("parts", Options{})
	require.NoError(t, err)
	require.True(t, partstore.SupportsTxFreeGetPart(store))
	require.True(t, partstore.SupportsTxFreePutPart(store))
	require.True(t, partstore.SupportsTxFreeDeletePart(store))
}

func TestDropboxPartStoreDoesNotRetryDataUpload(t *testing.T) {
	testutils.SkipIfIntegration(t)
	uploadRequests := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		uploadRequests++
		recorder := httptest.NewRecorder()
		http.Error(recorder, "temporary failure", http.StatusServiceUnavailable)
		return recorder.Result(), nil
	})}
	store, err := New("/parts", Options{HTTPClient: client})
	require.NoError(t, err)
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)

	err = store.PutPart(context.Background(), nil, *id, partstore.PutPartOptions{}, strings.NewReader("content"))

	require.Error(t, err)
	require.Equal(t, 1, uploadRequests)
}

func TestDropboxPartStoreCanRetryStartAfterFailure(t *testing.T) {
	testutils.SkipIfIntegration(t)
	requests := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		requests++
		recorder := httptest.NewRecorder()
		if requests <= maxRetries {
			http.Error(recorder, "temporary failure", http.StatusServiceUnavailable)
		} else {
			io.WriteString(recorder, `{}`)
		}
		return recorder.Result(), nil
	})}
	store, err := New("/parts", Options{HTTPClient: client})
	require.NoError(t, err)

	require.Error(t, store.Start(context.Background()))
	require.NoError(t, store.Start(context.Background()))
	require.NoError(t, store.Stop(context.Background()))
}

func TestDropboxPartStoreRejectsFileAtRootPath(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		recorder.WriteHeader(http.StatusConflict)
		io.WriteString(recorder, `{"error_summary":"path/conflict/file/"}`)
		return recorder.Result(), nil
	})}
	store, err := New("/parts", Options{HTTPClient: client})
	require.NoError(t, err)

	require.Error(t, store.Start(context.Background()))
}

func TestDropboxPartStoreDoesNotTreatEveryConflictAsMissing(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		recorder.WriteHeader(http.StatusConflict)
		io.WriteString(recorder, `{"error_summary":"path/restricted_content/"}`)
		return recorder.Result(), nil
	})}
	store, err := New("/parts", Options{HTTPClient: client})
	require.NoError(t, err)
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)

	_, err = store.GetPart(context.Background(), nil, *id)
	require.Error(t, err)
	require.NotErrorIs(t, err, partstore.ErrPartNotFound)
}

func TestDropboxPartStoreDrainsSuccessfulDeleteResponse(t *testing.T) {
	testutils.SkipIfIntegration(t)
	responseBody := &trackingReadCloser{reader: strings.NewReader(`{"metadata":{"name":"part"}}`)}
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       responseBody,
			Request:    r,
		}, nil
	})}
	store, err := New("/parts", Options{HTTPClient: client})
	require.NoError(t, err)
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)

	err = store.DeletePart(context.Background(), nil, *id)

	require.NoError(t, err)
	require.Zero(t, responseBody.reader.Len())
	require.True(t, responseBody.closed)
}

type trackingReadCloser struct {
	reader *strings.Reader
	closed bool
}

func (r *trackingReadCloser) Read(p []byte) (int, error) { return r.reader.Read(p) }
func (r *trackingReadCloser) Close() error {
	r.closed = true
	return nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
