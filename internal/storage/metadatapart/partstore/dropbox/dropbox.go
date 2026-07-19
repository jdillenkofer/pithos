package dropbox

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

const (
	defaultAPIEndpoint     = "https://api.dropboxapi.com/2"
	defaultContentEndpoint = "https://content.dropboxapi.com/2"
	maxRetries             = 5
)

type Options struct {
	HTTPClient      *http.Client
	APIEndpoint     string
	ContentEndpoint string
}

type dropboxPartStore struct {
	*lifecycle.ValidatedLifecycle
	root            string
	httpClient      *http.Client
	apiEndpoint     string
	contentEndpoint string
	tracer          trace.Tracer
}

var _ partstore.PartStore = (*dropboxPartStore)(nil)

func New(root string, options Options) (partstore.PartStore, error) {
	if strings.TrimSpace(root) == "" {
		return nil, errors.New("root must not be empty")
	}
	l, err := lifecycle.NewValidatedLifecycle("dropboxPartStore")
	if err != nil {
		return nil, err
	}
	client := options.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	apiEndpoint := strings.TrimRight(options.APIEndpoint, "/")
	if apiEndpoint == "" {
		apiEndpoint = defaultAPIEndpoint
	}
	contentEndpoint := strings.TrimRight(options.ContentEndpoint, "/")
	if contentEndpoint == "" {
		contentEndpoint = defaultContentEndpoint
	}
	return &dropboxPartStore{
		ValidatedLifecycle: l,
		root:               normalizeRoot(root),
		httpClient:         client,
		apiEndpoint:        apiEndpoint,
		contentEndpoint:    contentEndpoint,
		tracer:             otel.Tracer("internal/storage/metadatapart/partstore/dropbox"),
	}, nil
}

func normalizeRoot(root string) string {
	return "/" + strings.Trim(path.Clean("/"+root), "/")
}

func (s *dropboxPartStore) Start(ctx context.Context) error {
	if err := s.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	body, _ := json.Marshal(map[string]any{"path": s.root, "autorename": false})
	resp, err := s.do(ctx, s.apiEndpoint+"/files/create_folder_v2", body, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	data, _ := io.ReadAll(resp.Body)
	// Dropbox returns 409 when the folder already exists.
	if resp.StatusCode == http.StatusConflict && bytes.Contains(data, []byte("conflict")) {
		return nil
	}
	return apiError(resp.StatusCode, data)
}

func (s *dropboxPartStore) Stop(ctx context.Context) error { return s.ValidatedLifecycle.Stop(ctx) }

func (s *dropboxPartStore) partPath(id partstore.PartId) string {
	return s.root + "/" + hex.EncodeToString(id.Bytes())
}

func parsePartName(name string) (*partstore.PartId, bool) {
	if len(name) != 32 {
		return nil, false
	}
	b, err := hex.DecodeString(name)
	if err != nil {
		return nil, false
	}
	id, err := partstore.NewPartIdFromBytes(b)
	return id, err == nil
}

func (s *dropboxPartStore) PutPart(ctx context.Context, tx database.Tx, id partstore.PartId, reader io.Reader) error {
	_, span := s.tracer.Start(ctx, "dropboxPartStore.PutPart")
	defer span.End()
	arg, _ := json.Marshal(map[string]any{"path": s.partPath(id), "mode": "overwrite", "autorename": false, "mute": true})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.contentEndpoint+"/files/upload", reader)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Dropbox-API-Arg", string(arg))
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	data, _ := io.ReadAll(resp.Body)
	return apiError(resp.StatusCode, data)
}

func (s *dropboxPartStore) SupportsTxFreePutPart() bool    { return true }
func (s *dropboxPartStore) SupportsTxFreeGetPart() bool    { return true }
func (s *dropboxPartStore) SupportsTxFreeDeletePart() bool { return true }

func (s *dropboxPartStore) GetPart(ctx context.Context, tx database.Tx, id partstore.PartId) (io.ReadCloser, error) {
	_, span := s.tracer.Start(ctx, "dropboxPartStore.GetPart")
	defer span.End()
	body, size, err := s.download(ctx, s.partPath(id), 0)
	if err != nil {
		return nil, err
	}
	return &dropboxReader{store: s, ctx: ctx, path: s.partPath(id), body: body, size: size}, nil
}

func (s *dropboxPartStore) download(ctx context.Context, filePath string, offset int64) (io.ReadCloser, int64, error) {
	arg, _ := json.Marshal(map[string]string{"path": filePath})
	headers := http.Header{"Dropbox-API-Arg": []string{string(arg)}}
	if offset > 0 {
		headers.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := s.do(ctx, s.contentEndpoint+"/files/download", nil, headers, func(code int) bool { return code == 409 || code == 416 })
	if err != nil {
		return nil, -1, err
	}
	if resp.StatusCode == 409 {
		resp.Body.Close()
		return nil, -1, partstore.ErrPartNotFound
	}
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		resp.Body.Close()
		return nil, -1, io.EOF
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, -1, apiError(resp.StatusCode, data)
	}
	size := resp.ContentLength
	if offset > 0 && size >= 0 {
		size += offset
	}
	return resp.Body, size, nil
}

type dropboxReader struct {
	store  *dropboxPartStore
	ctx    context.Context
	path   string
	body   io.ReadCloser
	offset int64
	size   int64
	closed bool
}

var _ io.ReadSeekCloser = (*dropboxReader)(nil)

func (r *dropboxReader) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.EOF
	}
	if r.body == nil {
		body, size, err := r.store.download(r.ctx, r.path, r.offset)
		if errors.Is(err, io.EOF) {
			return 0, io.EOF
		}
		if err != nil {
			return 0, err
		}
		r.body, r.size = body, size
	}
	n, err := r.body.Read(p)
	r.offset += int64(n)
	return n, err
}

func (r *dropboxReader) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = r.offset + offset
	case io.SeekEnd:
		if r.size < 0 {
			return r.offset, errors.New("part size is unavailable")
		}
		next = r.size + offset
	default:
		return r.offset, fmt.Errorf("invalid seek whence: %d", whence)
	}
	if next < 0 {
		return r.offset, errors.New("negative seek offset")
	}
	if next != r.offset && r.body != nil {
		_ = r.body.Close()
		r.body = nil
	}
	r.offset = next
	return next, nil
}

func (r *dropboxReader) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}

func (s *dropboxPartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	_, span := s.tracer.Start(ctx, "dropboxPartStore.GetPartIds")
	defer span.End()
	requestPath := s.apiEndpoint + "/files/list_folder"
	payload := map[string]any{"path": s.root, "recursive": false, "limit": 2000}
	var result []partstore.PartId
	for {
		body, _ := json.Marshal(payload)
		resp, err := s.do(ctx, requestPath, body, nil, nil)
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			return nil, readErr
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, apiError(resp.StatusCode, data)
		}
		var page struct {
			Entries []struct {
				Name string `json:"name"`
			} `json:"entries"`
			Cursor  string `json:"cursor"`
			HasMore bool   `json:"has_more"`
		}
		if err := json.Unmarshal(data, &page); err != nil {
			return nil, err
		}
		for _, entry := range page.Entries {
			if id, ok := parsePartName(entry.Name); ok {
				result = append(result, *id)
			}
		}
		if !page.HasMore {
			return result, nil
		}
		requestPath = s.apiEndpoint + "/files/list_folder/continue"
		payload = map[string]any{"cursor": page.Cursor}
	}
}

func (s *dropboxPartStore) DeletePart(ctx context.Context, tx database.Tx, id partstore.PartId) error {
	_, span := s.tracer.Start(ctx, "dropboxPartStore.DeletePart")
	defer span.End()
	body, _ := json.Marshal(map[string]string{"path": s.partPath(id)})
	resp, err := s.do(ctx, s.apiEndpoint+"/files/delete_v2", body, nil, func(code int) bool { return code == 409 })
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	data, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == 409 && bytes.Contains(data, []byte("not_found")) {
		return nil
	}
	return apiError(resp.StatusCode, data)
}

func (s *dropboxPartStore) do(ctx context.Context, endpoint string, body []byte, headers http.Header, terminal func(int) bool) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		for key, values := range headers {
			for _, value := range values {
				req.Header.Add(key, value)
			}
		}
		resp, err := s.httpClient.Do(req)
		if err != nil {
			return nil, err
		}
		if terminal != nil && terminal(resp.StatusCode) {
			return resp, nil
		}
		if (resp.StatusCode != 429 && resp.StatusCode < 500) || attempt >= maxRetries-1 {
			return resp, nil
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		delay := time.Duration(1<<attempt) * 100 * time.Millisecond
		if retryAfter := resp.Header.Get("Retry-After"); retryAfter != "" {
			if seconds, err := time.ParseDuration(retryAfter + "s"); err == nil {
				delay = seconds
			}
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func apiError(status int, body []byte) error {
	return fmt.Errorf("Dropbox API returned HTTP %d: %s", status, strings.TrimSpace(string(body)))
}

// OAuthEndpoint is Dropbox's OAuth 2 endpoint for exchanging refresh tokens.
var OAuthEndpoint = struct{ AuthURL, TokenURL string }{
	AuthURL:  "https://www.dropbox.com/oauth2/authorize",
	TokenURL: "https://api.dropboxapi.com/oauth2/token",
}

// ValidateEndpoint is used only to make malformed endpoint overrides fail early.
func ValidateEndpoint(endpoint string) error {
	if endpoint == "" {
		return nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return errors.New("endpoint must be an absolute URL")
	}
	return nil
}
