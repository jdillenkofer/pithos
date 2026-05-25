package cache

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	cachepkg "github.com/jdillenkofer/pithos/internal/cache"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// Production default: cache parts up to 64 MiB.
// This balances hit rate and memory pressure for common multipart workloads.
const defaultMaxPartSizeBytes int64 = 64 * 1024 * 1024

type Options struct {
	MaxPartSizeBytes      int64
	CacheReadErrorsAsMiss bool
}

type cachePartStore struct {
	innerPartStore        partstore.PartStore
	cache                 cachepkg.Cache
	maxPartSizeBytes      int64
	cacheReadErrorsAsMiss bool
	tracer                trace.Tracer
	oversizedHints        sync.Map
}

var _ partstore.PartStore = (*cachePartStore)(nil)

func New(cache cachepkg.Cache, innerPartStore partstore.PartStore, opts Options) (partstore.PartStore, error) {
	maxPartSizeBytes := opts.MaxPartSizeBytes
	if maxPartSizeBytes <= 0 {
		maxPartSizeBytes = defaultMaxPartSizeBytes
	}

	return &cachePartStore{
		innerPartStore:        innerPartStore,
		cache:                 cache,
		maxPartSizeBytes:      maxPartSizeBytes,
		cacheReadErrorsAsMiss: opts.CacheReadErrorsAsMiss,
		tracer:                otel.Tracer("internal/storage/metadatapart/partstore/cache"),
	}, nil
}

func getPartCacheKey(partId partstore.PartId) string {
	return "PART_" + hex.EncodeToString(partId.Bytes())
}

func (ps *cachePartStore) Start(ctx context.Context) error {
	return ps.innerPartStore.Start(ctx)
}

func (ps *cachePartStore) Stop(ctx context.Context) error {
	return ps.innerPartStore.Stop(ctx)
}

func (ps *cachePartStore) PutPart(ctx context.Context, tx *database.TxContext, partId partstore.PartId, reader io.Reader) error {
	ctx, span := ps.tracer.Start(ctx, "cachePartStore.PutPart")
	defer span.End()

	cacheKey := getPartCacheKey(partId)
	buf := make([]byte, 0)
	teed := &cacheBufferingReader{Reader: reader, maxPartSizeBytes: ps.maxPartSizeBytes, data: &buf}
	teed.cacheEligible = true

	err := ps.innerPartStore.PutPart(ctx, tx, partId, teed)
	if err != nil {
		return err
	}
	if tx != nil {
		tx.RegisterOnCommit(func(opCtx context.Context) error {
			if teed.cacheEligible {
				ps.clearOversizedHint(cacheKey)
				if setErr := ps.cache.Set(cacheKey, bytes.NewReader(buf), int64(len(buf))); setErr != nil {
					slog.WarnContext(opCtx, "Failed to write part to cache", "cacheKey", cacheKey, "error", setErr)
					_ = ps.cache.Remove(cacheKey)
				}
				return nil
			}
			ps.markOversizedHint(cacheKey)
			if removeErr := ps.cache.Remove(cacheKey); removeErr != nil {
				slog.DebugContext(opCtx, "Failed to remove oversized part from cache", "cacheKey", cacheKey, "error", removeErr)
			}
			return nil
		})
		return nil
	}

	if teed.cacheEligible {
		ps.clearOversizedHint(cacheKey)
		if err := ps.cache.Set(cacheKey, bytes.NewReader(buf), int64(len(buf))); err != nil {
			slog.WarnContext(ctx, "Failed to write part to cache", "cacheKey", cacheKey, "error", err)
			_ = ps.cache.Remove(cacheKey)
		}
	} else {
		ps.markOversizedHint(cacheKey)
		if err := ps.cache.Remove(cacheKey); err != nil {
			slog.DebugContext(ctx, "Failed to remove oversized part from cache", "cacheKey", cacheKey, "error", err)
		}
	}

	return nil
}

func (ps *cachePartStore) GetPart(ctx context.Context, tx *database.TxContext, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := ps.tracer.Start(ctx, "cachePartStore.GetPart")
	defer span.End()

	cacheKey := getPartCacheKey(partId)
	rc, err := ps.cache.Get(cacheKey)
	if err != nil && err != cachepkg.ErrCacheMiss {
		if !ps.cacheReadErrorsAsMiss {
			return nil, err
		}
		slog.WarnContext(ctx, "Treating cache read error as cache miss", "cacheKey", cacheKey, "error", err)
	}
	if err == nil {
		return rc, nil
	}
	if ps.hasOversizedHint(cacheKey) {
		return ps.innerPartStore.GetPart(ctx, tx, partId)
	}

	rc, err = ps.innerPartStore.GetPart(ctx, tx, partId)
	if err != nil {
		return nil, err
	}

	pr, pw := io.Pipe()
	cacheWriteDone := make(chan struct{})
	go func() {
		defer close(cacheWriteDone)
		err := ps.cache.Set(cacheKey, pr, -1)
		if err == nil {
			ps.clearOversizedHint(cacheKey)
			return
		}
		if errors.Is(err, errPartLargerThanCacheThreshold) {
			ps.markOversizedHint(cacheKey)
			_ = ps.cache.Remove(cacheKey)
			return
		}
		slog.WarnContext(ctx, "Failed to stream part into cache", "cacheKey", cacheKey, "error", err)
		_ = ps.cache.Remove(cacheKey)
	}()

	return &streamingCacheOnReadCloser{
		ReadCloser:        rc,
		pipeWriter:        pw,
		cacheWriteDone:    cacheWriteDone,
		maxPartSizeBytes:  ps.maxPartSizeBytes,
		cacheBytesWritten: 0,
		cachePipeActive:   true,
	}, nil
}

func (ps *cachePartStore) GetPartIds(ctx context.Context, tx *database.TxContext) ([]partstore.PartId, error) {
	ctx, span := ps.tracer.Start(ctx, "cachePartStore.GetPartIds")
	defer span.End()

	return ps.innerPartStore.GetPartIds(ctx, tx)
}

func (ps *cachePartStore) DeletePart(ctx context.Context, tx *database.TxContext, partId partstore.PartId) error {
	ctx, span := ps.tracer.Start(ctx, "cachePartStore.DeletePart")
	defer span.End()

	err := ps.innerPartStore.DeletePart(ctx, tx, partId)
	if err != nil {
		return err
	}
	if tx != nil {
		cacheKey := getPartCacheKey(partId)
		tx.RegisterOnCommit(func(opCtx context.Context) error {
			ps.clearOversizedHint(cacheKey)
			if removeErr := ps.cache.Remove(cacheKey); removeErr != nil {
				slog.DebugContext(opCtx, "Failed to remove part from cache on delete", "cacheKey", cacheKey, "error", removeErr)
			}
			return nil
		})
		return nil
	}

	cacheKey := getPartCacheKey(partId)
	ps.clearOversizedHint(cacheKey)
	if err := ps.cache.Remove(cacheKey); err != nil {
		slog.DebugContext(ctx, "Failed to remove part from cache on delete", "cacheKey", cacheKey, "error", err)
	}
	return nil
}

func (ps *cachePartStore) OnTxCommit(ctx context.Context, tx *database.TxContext) error { return nil }
func (ps *cachePartStore) OnTxRollback(ctx context.Context, tx *database.TxContext) error {
	return nil
}

func (ps *cachePartStore) hasOversizedHint(cacheKey string) bool {
	_, ok := ps.oversizedHints.Load(cacheKey)
	return ok
}

func (ps *cachePartStore) markOversizedHint(cacheKey string) {
	ps.oversizedHints.Store(cacheKey, struct{}{})
}

func (ps *cachePartStore) clearOversizedHint(cacheKey string) {
	ps.oversizedHints.Delete(cacheKey)
}

var errPartLargerThanCacheThreshold = errors.New("part larger than cache threshold")

type streamingCacheOnReadCloser struct {
	io.ReadCloser
	pipeWriter        *io.PipeWriter
	cacheWriteDone    chan struct{}
	maxPartSizeBytes  int64
	cacheBytesWritten int64
	cachePipeActive   bool
	closed            bool
}

func (r *streamingCacheOnReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 && r.cachePipeActive {
		r.cacheBytesWritten += int64(n)
		if r.cacheBytesWritten > r.maxPartSizeBytes {
			_ = r.pipeWriter.CloseWithError(errPartLargerThanCacheThreshold)
			r.cachePipeActive = false
			r.pipeWriter = nil
		} else if _, writeErr := r.pipeWriter.Write(p[:n]); writeErr != nil {
			_ = r.pipeWriter.CloseWithError(writeErr)
			r.cachePipeActive = false
			r.pipeWriter = nil
		}
	}

	if err == io.EOF {
		if r.cachePipeActive && r.pipeWriter != nil {
			_ = r.pipeWriter.Close()
			r.cachePipeActive = false
			r.pipeWriter = nil
		}
	} else if err != nil {
		if r.cachePipeActive && r.pipeWriter != nil {
			_ = r.pipeWriter.CloseWithError(err)
			r.cachePipeActive = false
			r.pipeWriter = nil
		}
	}

	return n, err
}

func (r *streamingCacheOnReadCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	if r.cachePipeActive && r.pipeWriter != nil {
		_ = r.pipeWriter.CloseWithError(io.ErrUnexpectedEOF)
		r.cachePipeActive = false
		r.pipeWriter = nil
	}
	if r.cacheWriteDone != nil {
		<-r.cacheWriteDone
		r.cacheWriteDone = nil
	}
	return r.ReadCloser.Close()
}

type cacheBufferingReader struct {
	io.Reader
	maxPartSizeBytes int64
	cacheEligible    bool
	data             *[]byte
}

func (r *cacheBufferingReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		if r.cacheEligible {
			nextLen := int64(len(*r.data) + n)
			if nextLen <= r.maxPartSizeBytes {
				*r.data = append(*r.data, p[:n]...)
			} else {
				r.cacheEligible = false
				*r.data = nil
			}
		}
	}
	return n, err
}
