package onedrive

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// Scope confines pithos to its application folder in the user's OneDrive.
const Scope = "Files.ReadWrite.AppFolder"
const defaultEndpoint = "https://graph.microsoft.com/v1.0"
const uploadChunkSize = 10 * 1024 * 1024 // 32 * 320 KiB, as required by Graph.

type store struct {
	*lifecycle.ValidatedLifecycle
	folderName string
	endpoint   string
	client     *http.Client
	folderID   string
	tracer     trace.Tracer
}

var _ partstore.PartStore = (*store)(nil)

func New(folderName, endpoint string, client *http.Client) (partstore.PartStore, error) {
	if folderName == "" {
		return nil, errors.New("folderName must not be empty")
	}
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	if client == nil {
		client = http.DefaultClient
	}
	l, err := lifecycle.NewValidatedLifecycle("oneDrivePartStore")
	if err != nil {
		return nil, err
	}
	return &store{ValidatedLifecycle: l, folderName: folderName, endpoint: strings.TrimRight(endpoint, "/"), client: client, tracer: otel.Tracer("internal/storage/metadatapart/partstore/onedrive")}, nil
}

type driveItem struct {
	ID     string          `json:"id"`
	Name   string          `json:"name"`
	Size   int64           `json:"size"`
	Folder json.RawMessage `json:"folder"`
}
type itemList struct {
	Value []driveItem `json:"value"`
	Next  string      `json:"@odata.nextLink"`
}

func (s *store) Start(ctx context.Context) error {
	if err := s.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	// The app root is created by Graph on first access.
	var root driveItem
	if err := s.doJSON(ctx, http.MethodGet, s.endpoint+"/me/drive/special/approot", nil, &root); err != nil {
		return err
	}
	var folder driveItem
	err := s.doJSON(ctx, http.MethodGet, s.endpoint+"/me/drive/items/"+url.PathEscape(root.ID)+":/"+url.PathEscape(s.folderName), nil, &folder)
	if errors.Is(err, partstore.ErrPartNotFound) {
		body := map[string]any{"name": s.folderName, "folder": map[string]any{}, "@microsoft.graph.conflictBehavior": "fail"}
		err = s.doJSON(ctx, http.MethodPost, s.endpoint+"/me/drive/items/"+url.PathEscape(root.ID)+"/children", body, &folder)
		var apiErr *graphAPIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusConflict {
			// Another pithos instance created the folder after our initial lookup.
			err = s.doJSON(ctx, http.MethodGet, s.endpoint+"/me/drive/items/"+url.PathEscape(root.ID)+":/"+url.PathEscape(s.folderName), nil, &folder)
		}
	}
	if err != nil {
		return err
	}
	s.folderID = folder.ID
	return nil
}

func (s *store) Stop(ctx context.Context) error  { return s.ValidatedLifecycle.Stop(ctx) }
func (s *store) name(id partstore.PartId) string { return hex.EncodeToString(id.Bytes()) }
func parseName(name string) (*partstore.PartId, bool) {
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
func (s *store) itemURL(name string) string {
	return s.endpoint + "/me/drive/items/" + url.PathEscape(s.folderID) + ":/" + url.PathEscape(name)
}

func (s *store) PutPart(ctx context.Context, tx database.Tx, id partstore.PartId, r io.Reader) error {
	_, span := s.tracer.Start(ctx, "oneDrivePartStore.PutPart")
	defer span.End()

	file, err := os.CreateTemp("", "pithos-onedrive-part-*")
	if err != nil {
		return err
	}
	path := file.Name()
	defer os.Remove(path)
	size, err := io.Copy(file, r)
	if err != nil {
		file.Close()
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		file.Close()
		return err
	}
	defer file.Close()

	// Graph upload sessions require a positive total size. Empty files use the
	// simple endpoint, which has no practical size concern for this case.
	if size == 0 {
		return s.putEmptyPart(ctx, id)
	}
	var session struct {
		UploadURL string `json:"uploadUrl"`
	}
	payload := map[string]any{"item": map[string]any{"@microsoft.graph.conflictBehavior": "replace", "name": s.name(id)}}
	if err := s.doJSON(ctx, http.MethodPost, s.itemURL(s.name(id))+":/createUploadSession", payload, &session); err != nil {
		return err
	}
	if session.UploadURL == "" {
		return errors.New("Microsoft Graph returned an empty upload URL")
	}

	uploadClient := unauthenticatedClient(s.client)
	buffer := make([]byte, uploadChunkSize)
	for offset := int64(0); offset < size; {
		chunkSize := int64(len(buffer))
		if remaining := size - offset; remaining < chunkSize {
			chunkSize = remaining
		}
		if _, err := io.ReadFull(file, buffer[:chunkSize]); err != nil {
			return err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPut, session.UploadURL, bytes.NewReader(buffer[:chunkSize]))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/octet-stream")
		req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", offset, offset+chunkSize-1, size))
		resp, err := doWithClient(uploadClient, req)
		if err != nil {
			return err
		}
		if resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
			err = graphError(resp)
			resp.Body.Close()
			return err
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		offset += chunkSize
	}
	return nil
}

func (s *store) putEmptyPart(ctx context.Context, id partstore.PartId) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.itemURL(s.name(id))+":/content", http.NoBody)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := s.do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return graphError(resp)
	}
	return nil
}

func unauthenticatedClient(client *http.Client) *http.Client {
	copy := *client
	if transport, ok := client.Transport.(*oauth2.Transport); ok {
		copy.Transport = transport.Base
	}
	return &copy
}

func doWithClient(client *http.Client, req *http.Request) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if (resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode < 500) || attempt == 4 {
			return resp, nil
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		req.Body, err = req.GetBody()
		if err != nil {
			return nil, err
		}
		delay := time.Duration(1<<attempt) * 100 * time.Millisecond
		if value := resp.Header.Get("Retry-After"); value != "" {
			if seconds, err := strconv.Atoi(value); err == nil {
				delay = time.Duration(seconds) * time.Second
			}
		}
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(delay):
		}
	}
}
func (s *store) SupportsTxFreePutPart() bool { return true }

func (s *store) GetPart(ctx context.Context, tx database.Tx, id partstore.PartId) (io.ReadCloser, error) {
	_, span := s.tracer.Start(ctx, "oneDrivePartStore.GetPart")
	defer span.End()
	return s.openAt(ctx, s.name(id), 0)
}
func (s *store) SupportsTxFreeGetPart() bool { return true }

func (s *store) openAt(ctx context.Context, name string, offset int64) (io.ReadCloser, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.itemURL(name)+":/content", nil)
	if err != nil {
		return nil, err
	}
	if offset > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	resp, err := s.do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, partstore.ErrPartNotFound
	}
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		resp.Body.Close()
		return nil, io.EOF
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, graphError(resp)
	}
	return &readSeekCloser{s: s, ctx: ctx, name: name, body: resp.Body, offset: offset, size: -1}, nil
}

type readSeekCloser struct {
	s            *store
	ctx          context.Context
	name         string
	body         io.ReadCloser
	offset, size int64
	closed       bool
}

func (r *readSeekCloser) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.EOF
	}
	if r.body == nil {
		b, e := r.s.openBodyAt(r.ctx, r.name, r.offset)
		if e != nil {
			return 0, e
		}
		r.body = b
	}
	n, e := r.body.Read(p)
	r.offset += int64(n)
	return n, e
}
func (s *store) openBodyAt(ctx context.Context, name string, off int64) (io.ReadCloser, error) {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, s.itemURL(name)+":/content", nil)
	if e != nil {
		return nil, e
	}
	if off > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", off))
	}
	resp, e := s.do(req)
	if e != nil {
		return nil, e
	}
	if resp.StatusCode == 416 {
		resp.Body.Close()
		return nil, io.EOF
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		return nil, graphError(resp)
	}
	return resp.Body, nil
}
func (r *readSeekCloser) Seek(off int64, w int) (int64, error) {
	var n int64
	switch w {
	case io.SeekStart:
		n = off
	case io.SeekCurrent:
		n = r.offset + off
	case io.SeekEnd:
		if r.size < 0 {
			var item driveItem
			if e := r.s.doJSON(r.ctx, http.MethodGet, r.s.itemURL(r.name), nil, &item); e != nil {
				return r.offset, e
			}
			r.size = item.Size
		}
		n = r.size + off
	default:
		return r.offset, errors.New("invalid seek whence")
	}
	if n < 0 {
		return r.offset, errors.New("negative seek offset")
	}
	if n != r.offset && r.body != nil {
		_ = r.body.Close()
		r.body = nil
	}
	r.offset = n
	return n, nil
}
func (r *readSeekCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}

func (s *store) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	_, span := s.tracer.Start(ctx, "oneDrivePartStore.GetPartIds")
	defer span.End()
	next := s.endpoint + "/me/drive/items/" + url.PathEscape(s.folderID) + "/children?$select=id,name&$top=1000"
	ids := []partstore.PartId{}
	for next != "" {
		var page itemList
		if err := s.doJSON(ctx, http.MethodGet, next, nil, &page); err != nil {
			return nil, err
		}
		for _, x := range page.Value {
			if id, ok := parseName(x.Name); ok {
				ids = append(ids, *id)
			}
		}
		next = page.Next
	}
	return ids, nil
}
func (s *store) DeletePart(ctx context.Context, tx database.Tx, id partstore.PartId) error {
	_, span := s.tracer.Start(ctx, "oneDrivePartStore.DeletePart")
	defer span.End()
	req, e := http.NewRequestWithContext(ctx, http.MethodDelete, s.itemURL(s.name(id)), nil)
	if e != nil {
		return e
	}
	resp, e := s.do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return graphError(resp)
	}
	return nil
}
func (s *store) SupportsTxFreeDeletePart() bool { return true }

func (s *store) doJSON(ctx context.Context, method, target string, input, output any) error {
	var data []byte
	var e error
	if input != nil {
		data, e = json.Marshal(input)
		if e != nil {
			return e
		}
	}
	req, e := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(data))
	if e != nil {
		return e
	}
	if input != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, e := s.do(req)
	if e != nil {
		return e
	}
	defer resp.Body.Close()
	if resp.StatusCode == 404 {
		return partstore.ErrPartNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return graphError(resp)
	}
	if output != nil {
		return json.NewDecoder(resp.Body).Decode(output)
	}
	return nil
}
func (s *store) do(req *http.Request) (*http.Response, error) {
	for attempt := 0; ; attempt++ {
		resp, e := s.client.Do(req)
		if e != nil {
			return nil, e
		}
		if resp.StatusCode != 429 && resp.StatusCode < 500 {
			return resp, nil
		}
		if attempt == 4 || req.Body != nil && req.GetBody == nil {
			return resp, nil
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if req.Body != nil {
			req.Body, e = req.GetBody()
			if e != nil {
				return nil, e
			}
		}
		delay := time.Duration(1<<attempt) * 100 * time.Millisecond
		if v := resp.Header.Get("Retry-After"); v != "" {
			if sec, e := strconv.Atoi(v); e == nil {
				delay = time.Duration(sec) * time.Second
			}
		}
		slog.Warn("OneDrive operation failed; retrying", "status", resp.StatusCode, "delay", delay)
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-time.After(delay):
		}
	}
}

type graphAPIError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e *graphAPIError) Error() string {
	return fmt.Sprintf("Microsoft Graph returned %s: %s", e.Status, e.Body)
}

func graphError(resp *http.Response) error {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	return &graphAPIError{StatusCode: resp.StatusCode, Status: resp.Status, Body: strings.TrimSpace(string(b))}
}

// NewProactiveTokenSource refreshes Microsoft OAuth tokens before expiry.
func NewProactiveTokenSource(cfg *oauth2.Config, token *oauth2.Token, window time.Duration, persist func(*oauth2.Token) error) oauth2.TokenSource {
	return &tokenSource{cfg: cfg, token: token, window: window, persist: persist}
}

type tokenSource struct {
	cfg     *oauth2.Config
	token   *oauth2.Token
	window  time.Duration
	persist func(*oauth2.Token) error
	mu      sync.Mutex
}

func (s *tokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.token == nil {
		return nil, errors.New("token is nil")
	}
	if s.token.AccessToken != "" && !s.token.Expiry.IsZero() && time.Until(s.token.Expiry) > s.window {
		return s.token, nil
	}
	t := *s.token
	t.AccessToken = ""
	t.Expiry = time.Now().Add(-time.Minute)
	fresh, e := s.cfg.TokenSource(context.Background(), &t).Token()
	if e != nil {
		return nil, e
	}
	s.token = fresh
	if s.persist != nil {
		e = s.persist(fresh)
	}
	return fresh, e
}
