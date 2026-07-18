package gdrive

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/oauth2"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

const maxDriveRetries = 5

const folderMimeType = "application/vnd.google-apps.folder"

// Scope is the OAuth scope the part store needs. drive.file only grants
// access to files created by this application, so the part folder must be
// created by pithos itself (see ensureFolder).
//
// The store is intended for single-instance use: multiple pithos instances
// writing to the same Drive part folder can race on part uploads because the
// part name is used as the Drive file identity.
const Scope = drive.DriveFileScope

type gdrivePartStore struct {
	*lifecycle.ValidatedLifecycle
	folderName    string
	clientOptions []option.ClientOption
	svc           *drive.Service
	folderId      string
	tracer        trace.Tracer
	// fileIdCache maps part name → Drive file id so reads and deletes can skip
	// the files.list lookup. Best-effort: a stale entry falls back to a lookup.
	fileIdCache sync.Map
}

// Compile-time check to ensure gdrivePartStore implements partstore.PartStore
var _ partstore.PartStore = (*gdrivePartStore)(nil)

func New(folderName string, clientOptions ...option.ClientOption) (partstore.PartStore, error) {
	if folderName == "" {
		return nil, errors.New("folderName must not be empty")
	}
	validatedLifecycle, err := lifecycle.NewValidatedLifecycle("gdrivePartStore")
	if err != nil {
		return nil, err
	}
	bs := &gdrivePartStore{
		ValidatedLifecycle: validatedLifecycle,
		folderName:         folderName,
		clientOptions:      clientOptions,
		tracer:             otel.Tracer("internal/storage/metadatapart/partstore/gdrive"),
	}
	return bs, nil
}

func (s *gdrivePartStore) Start(ctx context.Context) error {
	if err := s.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	svc, err := drive.NewService(ctx, s.clientOptions...)
	if err != nil {
		return err
	}
	s.svc = svc
	if err := s.ensureFolder(ctx); err != nil {
		return err
	}
	return nil
}

// NewProactiveTokenSource wraps an OAuth token with background refresh so the
// Google Drive client keeps a fresh access token while the process is running.
func NewProactiveTokenSource(cfg *oauth2.Config, token *oauth2.Token, refreshWindow time.Duration, persist func(*oauth2.Token) error) oauth2.TokenSource {
	if cfg == nil || token == nil || token.RefreshToken == "" {
		return oauth2.StaticTokenSource(token)
	}
	source := &proactiveTokenSource{cfg: cfg, token: token, refreshWindow: refreshWindow, persist: persist}
	if refreshWindow > 0 {
		go source.run()
	}
	return source
}

type proactiveTokenSource struct {
	cfg           *oauth2.Config
	token         *oauth2.Token
	refreshWindow time.Duration
	persist       func(*oauth2.Token) error
	mu            sync.Mutex
}

func (s *proactiveTokenSource) Token() (*oauth2.Token, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.token == nil {
		return nil, errors.New("token is nil")
	}
	if s.token.RefreshToken == "" {
		return s.token, nil
	}
	if s.token.Expiry.IsZero() || time.Until(s.token.Expiry) > s.refreshWindow {
		return s.token, nil
	}
	refreshCandidate := *s.token
	refreshCandidate.AccessToken = ""
	refreshCandidate.Expiry = time.Now().Add(-time.Minute)
	refreshed, err := s.cfg.TokenSource(context.Background(), &refreshCandidate).Token()
	if err != nil {
		return nil, err
	}
	s.token = refreshed
	if s.persist != nil {
		if err := s.persist(s.token); err != nil {
			return nil, err
		}
	}
	return s.token, nil
}

func (s *proactiveTokenSource) run() {
	if s.refreshWindow <= 0 {
		return
	}
	ticker := time.NewTicker(s.refreshWindow / 2)
	defer ticker.Stop()
	for range ticker.C {
		if _, err := s.Token(); err != nil {
			slog.Warn("Google Drive token refresh failed", "err", err)
		}
	}
}

// ensureFolder finds or creates the part folder. Under the drive.file scope
// only files created by this OAuth client are visible, so a folder created
// manually in the Drive UI cannot be used.
func (s *gdrivePartStore) ensureFolder(ctx context.Context) error {
	query := fmt.Sprintf("name = '%s' and mimeType = '%s' and trashed = false", escapeQueryValue(s.folderName), folderMimeType)
	fileList, err := doRetriableOperation(ctx, func() (*drive.FileList, error) {
		return s.svc.Files.List().Q(query).Fields("files(id)").PageSize(1).Context(ctx).Do()
	}, nil)
	if err != nil {
		return err
	}
	if len(fileList.Files) > 0 {
		s.folderId = fileList.Files[0].Id
		return nil
	}
	folder, err := doRetriableOperation(ctx, func() (*drive.File, error) {
		return s.svc.Files.Create(&drive.File{Name: s.folderName, MimeType: folderMimeType}).Fields("id").Context(ctx).Do()
	}, nil)
	if err != nil {
		return err
	}
	s.folderId = folder.Id
	return nil
}

func (s *gdrivePartStore) getPartName(partId partstore.PartId) string {
	return hex.EncodeToString(partId.Bytes())
}

func (s *gdrivePartStore) tryGetPartIdFromName(name string) (partId *partstore.PartId, ok bool) {
	if len(name) != 32 {
		return nil, false
	}
	partIdBytes, err := hex.DecodeString(name)
	if err != nil {
		return nil, false
	}
	partId, err = partstore.NewPartIdFromBytes(partIdBytes)
	if err != nil {
		return nil, false
	}
	return partId, true
}

// escapeQueryValue escapes a string for use inside single quotes in a Drive
// query expression.
func escapeQueryValue(value string) string {
	var escaped strings.Builder
	for _, r := range value {
		if r == '\'' || r == '\\' {
			escaped.WriteByte('\\')
		}
		escaped.WriteRune(r)
	}
	return escaped.String()
}

// findFileIdsByName returns the ids of all non-trashed files with the given
// name inside the part folder. The store updates an existing part file in
// place, so reads only need any matching file id.
func (s *gdrivePartStore) findFileIdsByName(ctx context.Context, name string) ([]string, error) {
	query := fmt.Sprintf("name = '%s' and '%s' in parents and trashed = false", escapeQueryValue(name), s.folderId)
	fileList, err := doRetriableOperation(ctx, func() (*drive.FileList, error) {
		return s.svc.Files.List().Q(query).Fields("files(id)").Context(ctx).Do()
	}, nil)
	if err != nil {
		return nil, err
	}
	fileIds := make([]string, 0, len(fileList.Files))
	for _, file := range fileList.Files {
		fileIds = append(fileIds, file.Id)
	}
	return fileIds, nil
}

// findFileIdByName returns the id of any non-trashed file with the given
// name, or "" if none exists.
func (s *gdrivePartStore) findFileIdByName(ctx context.Context, name string) (string, error) {
	fileIds, err := s.findFileIdsByName(ctx, name)
	if err != nil {
		return "", err
	}
	if len(fileIds) == 0 {
		return "", nil
	}
	return fileIds[0], nil
}

// deleteAllFilesByName deletes every file with the given name and drops the
// cache entry.
func (s *gdrivePartStore) deleteAllFilesByName(ctx context.Context, name string) error {
	s.fileIdCache.Delete(name)
	fileIds, err := s.findFileIdsByName(ctx, name)
	if err != nil {
		return err
	}
	var errs []error
	for _, fileId := range fileIds {
		if err := s.deleteFile(ctx, fileId); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func (s *gdrivePartStore) uploadFile(ctx context.Context, name string, reader io.Reader) (fileId string, err error) {
	fileId, err = s.findFileIdByName(ctx, name)
	if err != nil {
		return "", err
	}
	if fileId != "" {
		_, err = doRetriableOperation(ctx, func() (*drive.File, error) {
			return s.svc.Files.Update(fileId, &drive.File{}).Media(reader).Fields("id").Context(ctx).Do()
		}, nil)
		if err != nil {
			return "", err
		}
		return fileId, nil
	}

	file, err := doRetriableOperation(ctx, func() (*drive.File, error) {
		return s.svc.Files.Create(&drive.File{Name: name, Parents: []string{s.folderId}}).Media(reader).Fields("id").Context(ctx).Do()
	}, nil)
	if err != nil {
		return "", err
	}
	return file.Id, nil
}

// deleteFile permanently deletes a file (bypassing the trash). Deleting an
// already deleted file is treated as success.
func (s *gdrivePartStore) deleteFile(ctx context.Context, fileId string) error {
	_, err := doRetriableOperation(ctx, func() (*struct{}, error) {
		return nil, s.svc.Files.Delete(fileId).Context(ctx).Do()
	}, isNotFoundError)
	if err != nil && !isNotFoundError(err) {
		return err
	}
	return nil
}

func (s *gdrivePartStore) Stop(ctx context.Context) error {
	return s.ValidatedLifecycle.Stop(ctx)
}

func (s *gdrivePartStore) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	_, span := s.tracer.Start(ctx, "gdrivePartStore.PutPart")
	defer span.End()

	partName := s.getPartName(partId)
	fileId, err := s.uploadFile(ctx, partName, reader)
	if err != nil {
		return err
	}
	s.fileIdCache.Store(partName, fileId)
	return nil
}

// SupportsTxFreeGetPart reports that GetPart never uses the transaction.
func (s *gdrivePartStore) SupportsTxFreeGetPart() bool {
	return true
}

func (s *gdrivePartStore) SupportsTxFreePutPart() bool { return true }

func (s *gdrivePartStore) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	_, span := s.tracer.Start(ctx, "gdrivePartStore.GetPart")
	defer span.End()

	partName := s.getPartName(partId)

	// Try the cached file id first; on a stale entry fall back to a lookup.
	if cached, ok := s.fileIdCache.Load(partName); ok {
		body, err := s.downloadFile(ctx, cached.(string))
		if err == nil {
			return body, nil
		}
		if !errors.Is(err, partstore.ErrPartNotFound) {
			return nil, err
		}
		s.fileIdCache.CompareAndDelete(partName, cached)
	}

	fileId, err := s.findFileIdByName(ctx, partName)
	if err != nil {
		return nil, err
	}
	if fileId == "" {
		return nil, partstore.ErrPartNotFound
	}
	s.fileIdCache.Store(partName, fileId)
	return s.downloadFile(ctx, fileId)
}

// downloadFile opens the file eagerly (so a missing part surfaces as
// ErrPartNotFound here, not on the first Read) and returns a seekable reader:
// seeking closes the stream and the next Read reopens it with an HTTP Range
// header, so ranged object reads that start in the middle of a part do not
// download and discard the part's head.
func (s *gdrivePartStore) downloadFile(ctx context.Context, fileId string) (io.ReadCloser, error) {
	body, err := s.downloadFileAt(ctx, fileId, 0)
	if err != nil {
		return nil, err
	}
	return &driveFileReadSeekCloser{store: s, ctx: ctx, fileId: fileId, body: body, size: -1}, nil
}

// errRangeNotSatisfiable marks a download whose offset is at or past EOF.
var errRangeNotSatisfiable = errors.New("requested range not satisfiable")

func (s *gdrivePartStore) downloadFileAt(ctx context.Context, fileId string, offset int64) (io.ReadCloser, error) {
	return doRetriableOperation(ctx, func() (io.ReadCloser, error) {
		call := s.svc.Files.Get(fileId).Context(ctx)
		if offset > 0 {
			call.Header().Set("Range", fmt.Sprintf("bytes=%d-", offset))
		}
		resp, err := call.Download()
		if err != nil {
			if isNotFoundError(err) {
				return nil, partstore.ErrPartNotFound
			}
			var apiErr *googleapi.Error
			if errors.As(err, &apiErr) && apiErr.Code == http.StatusRequestedRangeNotSatisfiable {
				return nil, errRangeNotSatisfiable
			}
			return nil, err
		}
		return resp.Body, nil
	}, func(err error) bool {
		return errors.Is(err, partstore.ErrPartNotFound) || errors.Is(err, errRangeNotSatisfiable)
	})
}

type driveFileReadSeekCloser struct {
	store  *gdrivePartStore
	ctx    context.Context
	fileId string
	offset int64
	size   int64 // -1 until fetched (only needed for io.SeekEnd)
	body   io.ReadCloser
	closed bool
}

var _ io.ReadSeekCloser = (*driveFileReadSeekCloser)(nil)

func (r *driveFileReadSeekCloser) Read(p []byte) (int, error) {
	if r.closed {
		return 0, io.EOF
	}
	if r.body == nil {
		body, err := r.store.downloadFileAt(r.ctx, r.fileId, r.offset)
		if err != nil {
			// Seeking beyond EOF is allowed; reads there report EOF.
			if errors.Is(err, errRangeNotSatisfiable) {
				return 0, io.EOF
			}
			return 0, err
		}
		r.body = body
	}
	n, err := r.body.Read(p)
	r.offset += int64(n)
	return n, err
}

func (r *driveFileReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		if r.size < 0 {
			file, err := doRetriableOperation(r.ctx, func() (*drive.File, error) {
				return r.store.svc.Files.Get(r.fileId).Fields("size").Context(r.ctx).Do()
			}, nil)
			if err != nil {
				return r.offset, err
			}
			r.size = file.Size
		}
		newOffset = r.size + offset
	default:
		return r.offset, fmt.Errorf("invalid seek whence: %d", whence)
	}
	if newOffset < 0 {
		return r.offset, errors.New("negative seek offset")
	}
	if newOffset != r.offset && r.body != nil {
		_ = r.body.Close()
		r.body = nil
	}
	r.offset = newOffset
	return r.offset, nil
}

func (r *driveFileReadSeekCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.body != nil {
		return r.body.Close()
	}
	return nil
}

func (s *gdrivePartStore) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	_, span := s.tracer.Start(ctx, "gdrivePartStore.GetPartIds")
	defer span.End()

	partIds := []partstore.PartId{}
	seen := map[partstore.PartId]struct{}{}
	query := fmt.Sprintf("'%s' in parents and trashed = false", s.folderId)
	pageToken := ""
	for {
		fileList, err := doRetriableOperation(ctx, func() (*drive.FileList, error) {
			return s.svc.Files.List().Q(query).Fields("nextPageToken, files(name)").PageSize(1000).PageToken(pageToken).Context(ctx).Do()
		}, nil)
		if err != nil {
			return nil, err
		}
		for _, file := range fileList.Files {
			if partId, ok := s.tryGetPartIdFromName(file.Name); ok {
				if _, alreadySeen := seen[*partId]; !alreadySeen {
					seen[*partId] = struct{}{}
					partIds = append(partIds, *partId)
				}
			}
		}
		pageToken = fileList.NextPageToken
		if pageToken == "" {
			break
		}
	}
	return partIds, nil
}

func (s *gdrivePartStore) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	_, span := s.tracer.Start(ctx, "gdrivePartStore.DeletePart")
	defer span.End()

	partName := s.getPartName(partId)
	return s.deleteAllFilesByName(ctx, partName)
}

func (s *gdrivePartStore) SupportsTxFreeDeletePart() bool { return true }

func isNotFoundError(err error) bool {
	var apiErr *googleapi.Error
	return errors.As(err, &apiErr) && apiErr.Code == 404
}

// isRetriableError reports whether the Drive API error is transient
// (rate limiting or server errors). The Drive SDK does not retry on its own.
func isRetriableError(err error) bool {
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.Code == 429 || apiErr.Code >= 500 {
		return true
	}
	if apiErr.Code == 403 {
		for _, e := range apiErr.Errors {
			if e.Reason == "userRateLimitExceeded" || e.Reason == "rateLimitExceeded" {
				return true
			}
		}
	}
	return false
}

// getBackoffDuration returns the backoff duration for a given retry attempt.
// First retry is immediate, subsequent retries use exponential backoff:
// attempt 1: 0ms, attempt 2: 100ms, attempt 3: 1s, attempt 4: 5s, attempt 5: 10s
func getBackoffDuration(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 0
	case 2:
		return 100 * time.Millisecond
	case 3:
		return 1 * time.Second
	case 4:
		return 5 * time.Second
	default:
		return 10 * time.Second
	}
}

func doRetriableOperation[T any](ctx context.Context, op func() (T, error), shouldIgnoreError func(error) bool) (T, error) {
	retries := 0
	var empty T
	for {
		if err := ctx.Err(); err != nil {
			return empty, err
		}

		t, err := op()
		if err != nil {
			if shouldIgnoreError != nil && shouldIgnoreError(err) {
				return empty, err
			}
			if !isRetriableError(err) {
				return empty, err
			}

			retries += 1
			if retries < maxDriveRetries {
				backoff := getBackoffDuration(retries)
				if backoff > 0 {
					slog.Warn(fmt.Sprintf("Drive operation failed (attempt %d/%d): %v, retrying in %v", retries, maxDriveRetries, err, backoff))
					select {
					case <-ctx.Done():
						return empty, ctx.Err()
					case <-time.After(backoff):
					}
				} else {
					slog.Warn(fmt.Sprintf("Drive operation failed (attempt %d/%d): %v, retrying immediately", retries, maxDriveRetries, err))
				}
				continue
			}
			slog.Error(fmt.Sprintf("Drive operation failed after %d retries: %v", retries, err))
			return empty, err
		}
		return t, nil
	}
}
