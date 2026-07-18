package gdrive

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

const maxDriveRetries = 5

const folderMimeType = "application/vnd.google-apps.folder"

// maxConcurrentDriveOps bounds the number of parallel Drive API calls when a
// transaction touches many parts (e.g. deleting a large multi-part object).
// Sequential execution would put the whole per-part latency on the commit
// path and blow past client timeouts.
const maxConcurrentDriveOps = 8

// staleTransientFileMaxAge is the age after which leftover temp/backup files
// (from crashed or interrupted transactions) are deleted on Start. Files
// belonging to in-flight transactions are far younger than this.
const staleTransientFileMaxAge = 24 * time.Hour

// Scope is the OAuth scope the part store needs. drive.file only grants
// access to files created by this application, so the part folder must be
// created by pithos itself (see ensureFolder).
const Scope = drive.DriveFileScope

type gdrivePartStore struct {
	*lifecycle.ValidatedLifecycle
	folderName    string
	clientOptions []option.ClientOption
	svc           *drive.Service
	folderId      string
	tracer        trace.Tracer
	// txBatchesMu guards txBatches; a single tx can be shared across fan-out
	// goroutines (see database.TxController).
	txBatchesMu sync.Mutex
	// txBatches collects all part operations of one transaction so the commit
	// hooks can execute them with bounded parallelism. Keyed by the underlying
	// *sql.Tx because child tx controllers are distinct values sharing it.
	txBatches map[*sql.Tx]*txBatch
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
		txBatches:          map[*sql.Tx]*txBatch{},
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
	s.sweepStaleTransientFiles(ctx)
	return nil
}

// tryGetTransientFileTime extracts the creation time from a temp
// (".<part>.tmp.<ulid>") or backup ("<part>.txbackup.<ulid>") file name.
func tryGetTransientFileTime(name string) (time.Time, bool) {
	trimmed := strings.TrimPrefix(name, ".")
	var ulidStr string
	if i := strings.Index(trimmed, ".tmp."); i == 32 && strings.HasPrefix(name, ".") {
		ulidStr = trimmed[i+len(".tmp."):]
	} else if i := strings.Index(trimmed, ".txbackup."); i == 32 && !strings.HasPrefix(name, ".") {
		ulidStr = trimmed[i+len(".txbackup."):]
	} else {
		return time.Time{}, false
	}
	if _, err := hex.DecodeString(trimmed[:32]); err != nil {
		return time.Time{}, false
	}
	id, err := ulid.ParseStrict(ulidStr)
	if err != nil {
		return time.Time{}, false
	}
	return ulid.Time(id.Time()), true
}

// sweepStaleTransientFiles deletes temp/backup files left behind by crashed or
// interrupted transactions. Best-effort: failures are logged, never fatal, so
// a sweep problem cannot prevent the store from starting.
func (s *gdrivePartStore) sweepStaleTransientFiles(ctx context.Context) {
	cutoff := time.Now().Add(-staleTransientFileMaxAge)
	query := fmt.Sprintf("'%s' in parents and trashed = false", s.folderId)
	pageToken := ""
	swept := 0
	for {
		fileList, err := doRetriableOperation(ctx, func() (*drive.FileList, error) {
			return s.svc.Files.List().Q(query).Fields("nextPageToken, files(id, name)").PageSize(1000).PageToken(pageToken).Context(ctx).Do()
		}, nil)
		if err != nil {
			slog.Warn(fmt.Sprintf("Drive stale file sweep aborted: %v", err))
			return
		}
		for _, file := range fileList.Files {
			createdAt, ok := tryGetTransientFileTime(file.Name)
			if !ok || !createdAt.Before(cutoff) {
				continue
			}
			if err := s.deleteFile(ctx, file.Id); err != nil {
				slog.Warn(fmt.Sprintf("Drive stale file sweep could not delete %s: %v", file.Name, err))
				continue
			}
			swept++
		}
		pageToken = fileList.NextPageToken
		if pageToken == "" {
			break
		}
	}
	if swept > 0 {
		slog.Info(fmt.Sprintf("Drive stale file sweep deleted %d leftover temp/backup files", swept))
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
// name inside the part folder, newest first. Drive allows duplicate names
// (e.g. after an overwrite of the same part id); the newest file wins for
// reads, deletes remove every duplicate.
func (s *gdrivePartStore) findFileIdsByName(ctx context.Context, name string) ([]string, error) {
	query := fmt.Sprintf("name = '%s' and '%s' in parents and trashed = false", escapeQueryValue(name), s.folderId)
	fileList, err := doRetriableOperation(ctx, func() (*drive.FileList, error) {
		return s.svc.Files.List().Q(query).Fields("files(id)").OrderBy("createdTime desc").Context(ctx).Do()
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

// findFileIdByName returns the id of the newest non-trashed file with the
// given name, or "" if none exists.
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

// deleteAllFilesByName deletes every file with the given name (newest-first
// duplicates included) and drops the cache entry.
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
	file, err := s.svc.Files.Create(&drive.File{Name: name, Parents: []string{s.folderId}}).Media(reader).Fields("id").Context(ctx).Do()
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

type txOpKind int

const (
	txOpPut txOpKind = iota
	txOpDelete
)

// driveTxOp is one staged part operation inside a transaction.
//
// Puts upload the file under its final name at call time (one API call) and
// simply delete it again on rollback. A file that is visible in Drive before
// its transaction commits is harmless: readers only ask for parts referenced
// by committed metadata, and the garbage collector's grace window (keyed on
// the part id's ULID timestamp) protects in-flight parts. Crash leftovers are
// unreferenced parts the garbage collector removes.
//
// Deletes defer all Drive work to after the commit; a failed post-commit
// delete leaves an unreferenced part for the garbage collector.
type driveTxOp struct {
	kind     txOpKind
	partName string
	fileId   string // put only: the uploaded file
}

type txBatch struct {
	ops []*driveTxOp
}

// addTxOp stages op for tx, registering the commit hooks once per transaction.
func (s *gdrivePartStore) addTxOp(tx database.Tx, op *driveTxOp) {
	s.txBatchesMu.Lock()
	defer s.txBatchesMu.Unlock()
	sqlTx := tx.SqlTx()
	batch, ok := s.txBatches[sqlTx]
	if !ok {
		batch = &txBatch{}
		s.txBatches[sqlTx] = batch
		tx.OnAfterCommit(func(hookCtx context.Context) error {
			s.removeTxBatch(sqlTx)
			return s.runBatch(hookCtx, batch, s.opAfterCommit)
		})
		tx.OnRollback(func(hookCtx context.Context) error {
			s.removeTxBatch(sqlTx)
			return s.runBatch(hookCtx, batch, s.opRollback)
		})
	}
	batch.ops = append(batch.ops, op)
}

func (s *gdrivePartStore) removeTxBatch(sqlTx *sql.Tx) {
	s.txBatchesMu.Lock()
	defer s.txBatchesMu.Unlock()
	delete(s.txBatches, sqlTx)
}

// runBatch applies fn to every operation of the batch with bounded
// parallelism. Operations on the same part (e.g. the dedup path puts and
// deletes a part in one transaction) keep their registration order; distinct
// parts run concurrently.
func (s *gdrivePartStore) runBatch(ctx context.Context, batch *txBatch, fn func(context.Context, *driveTxOp) error) error {
	opsByPart := map[string][]*driveTxOp{}
	partOrder := []string{}
	for _, op := range batch.ops {
		if _, ok := opsByPart[op.partName]; !ok {
			partOrder = append(partOrder, op.partName)
		}
		opsByPart[op.partName] = append(opsByPart[op.partName], op)
	}

	sem := make(chan struct{}, maxConcurrentDriveOps)
	var wg sync.WaitGroup
	var errsMu sync.Mutex
	var errs []error
	for _, partName := range partOrder {
		ops := opsByPart[partName]
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			for _, op := range ops {
				if err := fn(ctx, op); err != nil {
					errsMu.Lock()
					errs = append(errs, err)
					errsMu.Unlock()
					return
				}
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

func (s *gdrivePartStore) opAfterCommit(ctx context.Context, op *driveTxOp) error {
	if op.kind == txOpDelete {
		return s.deleteAllFilesByName(ctx, op.partName)
	}
	return nil
}

func (s *gdrivePartStore) opRollback(ctx context.Context, op *driveTxOp) error {
	if op.kind == txOpPut {
		s.fileIdCache.CompareAndDelete(op.partName, op.fileId)
		return s.deleteFile(ctx, op.fileId)
	}
	return nil
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

	if tx != nil {
		s.addTxOp(tx, &driveTxOp{
			kind:     txOpPut,
			partName: partName,
			fileId:   fileId,
		})
		return nil
	}

	// Without a transaction (e.g. the outbox worker replaying an upload, which
	// may retry after a failure) remove any older duplicate. Best-effort: the
	// upload itself succeeded and reads prefer the newest file either way.
	fileIds, err := s.findFileIdsByName(ctx, partName)
	if err != nil {
		return nil
	}
	for _, existingFileId := range fileIds {
		if existingFileId != fileId {
			_ = s.deleteFile(ctx, existingFileId)
		}
	}
	return nil
}

// SupportsTxFreeGetPart reports that GetPart never uses the transaction.
func (s *gdrivePartStore) SupportsTxFreeGetPart() bool {
	return true
}

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

func (s *gdrivePartStore) downloadFile(ctx context.Context, fileId string) (io.ReadCloser, error) {
	return doRetriableOperation(ctx, func() (io.ReadCloser, error) {
		resp, err := s.svc.Files.Get(fileId).Context(ctx).Download()
		if err != nil {
			if isNotFoundError(err) {
				return nil, partstore.ErrPartNotFound
			}
			return nil, err
		}
		return resp.Body, nil
	}, func(err error) bool {
		return errors.Is(err, partstore.ErrPartNotFound)
	})
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
				// Duplicate names can briefly exist during an overwrite; report each part once.
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
	if tx != nil {
		s.addTxOp(tx, &driveTxOp{
			kind:     txOpDelete,
			partName: partName,
		})
		return nil
	}

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
