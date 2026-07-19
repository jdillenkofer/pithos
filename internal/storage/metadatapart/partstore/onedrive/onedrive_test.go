package onedrive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOneDrivePartStoreRoundTripAndSeek(t *testing.T) {
	testutils.SkipIfIntegration(t)
	files := map[string][]byte{}
	uploadRequests := 0
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
				data = data[offset:]
				w.WriteHeader(http.StatusPartialContent)
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
	require.NoError(t, reader.Close())
	ids, err := ps.GetPartIds(context.Background(), nil)
	require.NoError(t, err)
	assert.Equal(t, []partstore.PartId{*id}, ids)
	require.NoError(t, ps.DeletePart(context.Background(), nil, *id))
	_, err = ps.GetPart(context.Background(), nil, *id)
	assert.ErrorIs(t, err, partstore.ErrPartNotFound)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
