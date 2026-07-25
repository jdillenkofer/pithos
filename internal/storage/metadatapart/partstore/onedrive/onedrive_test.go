package onedrive

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestOneDrivePartStoreRoundTripAndSeek(t *testing.T) {
	testutils.SkipIfIntegration(t)
	files := map[string][]byte{}
	uploadRequests := 0
	ignoreNextRange := false
	mux := http.NewServeMux()
	mux.HandleFunc("/me/drive/special/approot", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]any{"id": "root"}) })
	mux.HandleFunc("/me/drive/items/root:/pithos-parts", func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	mux.HandleFunc("/me/drive/items/root/children", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 201, map[string]any{"id": "folder"}) })
	mux.HandleFunc("/me/drive/items/folder/children", func(w http.ResponseWriter, r *http.Request) {
		items := []map[string]any{}
		for name := range files {
			items = append(items, map[string]any{"id": name, "name": name})
		}
		writeJSON(w, 200, map[string]any{"value": items})
	})
	mux.HandleFunc("/me/drive/items/folder:/", func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/me/drive/items/folder:/")
		if strings.HasSuffix(path, ":/createUploadSession") {
			name := strings.TrimSuffix(path, ":/createUploadSession")
			writeJSON(w, http.StatusOK, map[string]any{"uploadUrl": "http://graph.test/upload/" + name})
			return
		}
		contentRequest := strings.HasSuffix(path, ":/content")
		name := strings.TrimSuffix(path, ":/content")
		switch r.Method {
		case http.MethodPut:
			data, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			files[name] = data
			writeJSON(w, 201, map[string]any{"id": name})
		case http.MethodGet:
			data, ok := files[name]
			if !ok {
				http.NotFound(w, r)
				return
			}
			if !contentRequest {
				writeJSON(w, 200, map[string]any{"id": name, "size": len(data)})
				return
			}
			if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
				var offset int
				_, err := fmt.Sscanf(rangeHeader, "bytes=%d-", &offset)
				require.NoError(t, err)
				if offset >= len(data) {
					w.WriteHeader(416)
					return
				}
				if ignoreNextRange {
					ignoreNextRange = false
				} else {
					total := len(data)
					data = data[offset:]
					w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, total-1, total))
					w.WriteHeader(http.StatusPartialContent)
				}
			}
			_, _ = w.Write(data)
		case http.MethodDelete:
			delete(files, name)
			w.WriteHeader(http.StatusNoContent)
		}
	})
	mux.HandleFunc("/upload/", func(w http.ResponseWriter, r *http.Request) {
		uploadRequests++
		name := strings.TrimPrefix(r.URL.Path, "/upload/")
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		var start, end, total int64
		_, err = fmt.Sscanf(r.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total)
		require.NoError(t, err)
		require.Equal(t, int64(len(files[name])), start)
		files[name] = append(files[name], data...)
		if int64(len(files[name])) == total {
			writeJSON(w, http.StatusCreated, map[string]any{"id": name})
		} else {
			writeJSON(w, http.StatusAccepted, map[string]any{"nextExpectedRanges": []string{fmt.Sprintf("%d-", end+1)}})
		}
	})
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, r)
		return recorder.Result(), nil
	})}
	ps, err := New("pithos-parts", "http://graph.test", client)
	require.NoError(t, err)
	require.NoError(t, ps.Start(context.Background()))
	defer ps.Stop(context.Background())
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	payload := append(bytes.Repeat([]byte{'x'}, uploadChunkSize+1), []byte("onedrive")...)
	require.NoError(t, ps.PutPart(context.Background(), nil, *id, bytes.NewReader(payload)))
	assert.Equal(t, 2, uploadRequests)
	reader, err := ps.GetPart(context.Background(), nil, *id)
	require.NoError(t, err)
	seeker, ok := reader.(io.Seeker)
	require.True(t, ok)
	_, err = seeker.Seek(int64(len(payload)-len("onedrive")), io.SeekStart)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "onedrive", string(data))

	// OneDrive's download CDN may ignore Range and return 200 with the full
	// object. The reader must still expose bytes from the requested offset.
	ignoreNextRange = true
	_, err = seeker.Seek(1, io.SeekStart)
	require.NoError(t, err)
	data, err = io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, payload[1:], data)

	require.NoError(t, reader.Close())
	ids, err := ps.GetPartIds(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, []partstore.PartId{*id}, ids)
	require.NoError(t, ps.DeletePart(context.Background(), nil, *id))
	_, err = ps.GetPart(context.Background(), nil, *id)
	assert.ErrorIs(t, err, partstore.ErrPartNotFound)
}

func TestOneDrivePartStoreCanRetryStartAfterFailure(t *testing.T) {
	testutils.SkipIfIntegration(t)
	rootRequests := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/me/drive/special/approot", func(w http.ResponseWriter, r *http.Request) {
		rootRequests++
		if rootRequests <= 5 {
			http.Error(w, "temporary failure", http.StatusServiceUnavailable)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"id": "root"})
	})
	mux.HandleFunc("/me/drive/items/root:/pithos-parts", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"id": "folder"})
	})
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, r)
		return recorder.Result(), nil
	})}
	ps, err := New("pithos-parts", "http://graph.test", client)
	require.NoError(t, err)

	require.Error(t, ps.Start(context.Background()))
	require.NoError(t, ps.Start(context.Background()))
	require.NoError(t, ps.Stop(context.Background()))
}

func TestOneDrivePartStoreDoesNotRetryDataUpload(t *testing.T) {
	testutils.SkipIfIntegration(t)
	uploadRequests := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		if r.Method == http.MethodPost {
			writeJSON(recorder, http.StatusOK, map[string]any{"uploadUrl": "http://graph.test/upload"})
		} else {
			uploadRequests++
			http.Error(recorder, "temporary failure", http.StatusServiceUnavailable)
		}
		return recorder.Result(), nil
	})}
	ps, err := New("pithos-parts", "http://graph.test", client)
	require.NoError(t, err)
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)

	err = ps.PutPart(context.Background(), nil, *id, strings.NewReader("content"))

	require.Error(t, err)
	assert.Equal(t, 1, uploadRequests)
}

func TestOneDrivePartStoreRejectsAcceptedFinalChunk(t *testing.T) {
	testutils.SkipIfIntegration(t)
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		if r.Method == http.MethodPost {
			writeJSON(recorder, http.StatusOK, map[string]any{"uploadUrl": "http://graph.test/upload"})
		} else {
			writeJSON(recorder, http.StatusAccepted, map[string]any{"nextExpectedRanges": []string{"7-"}})
		}
		return recorder.Result(), nil
	})}
	ps, err := New("pithos-parts", "http://graph.test", client)
	require.NoError(t, err)
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)

	err = ps.PutPart(context.Background(), nil, *id, strings.NewReader("content"))

	require.ErrorContains(t, err, "final upload chunk")
}

func TestOneDrivePartStoreValidatesNextExpectedRange(t *testing.T) {
	testutils.SkipIfIntegration(t)
	uploadRequests := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		recorder := httptest.NewRecorder()
		if r.Method == http.MethodPost {
			writeJSON(recorder, http.StatusOK, map[string]any{"uploadUrl": "http://graph.test/upload"})
		} else {
			uploadRequests++
			writeJSON(recorder, http.StatusAccepted, map[string]any{"nextExpectedRanges": []string{"0-"}})
		}
		return recorder.Result(), nil
	})}
	ps, err := New("pithos-parts", "http://graph.test", client)
	require.NoError(t, err)
	id, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	content := io.LimitReader(
		strings.NewReader(strings.Repeat("x", uploadChunkSize)+"."),
		uploadChunkSize+1,
	)

	err = ps.PutPart(context.Background(), nil, *id, content)

	require.ErrorContains(t, err, "expects upload offset 0")
	assert.Equal(t, 1, uploadRequests)
}

func TestOneDriveTokenPersistenceFailureDoesNotFailRefresh(t *testing.T) {
	testutils.SkipIfIntegration(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"access_token":  "fresh-access",
			"refresh_token": "refresh-token",
			"token_type":    "Bearer",
			"expires_in":    3600,
		})
	}))
	defer server.Close()
	cfg := &oauth2.Config{
		ClientID: "client-id",
		Endpoint: oauth2.Endpoint{TokenURL: server.URL},
	}
	source := NewProactiveTokenSource(cfg, &oauth2.Token{
		AccessToken:  "expired-access",
		RefreshToken: "refresh-token",
		Expiry:       time.Now().Add(-time.Minute),
	}, time.Minute, func(*oauth2.Token) error {
		return errors.New("read-only token provider")
	})

	token, err := source.Token()
	require.NoError(t, err)
	assert.Equal(t, "fresh-access", token.AccessToken)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
