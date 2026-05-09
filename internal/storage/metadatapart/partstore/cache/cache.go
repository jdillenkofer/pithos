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
	"golang.org/x/sync/singleflight"

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
	readGroup             singleflight.Group
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

	v, err, shared := ps.readGroup.Do(cacheKey, func() (interface{}, error) {
		rc, innerErr := ps.innerPartStore.GetPart(ctx, tx, partId)
		if innerErr != nil {
			return nil, innerErr
		}
		payload, readErr := readUpTo(rc, ps.maxPartSizeBytes+1)
		if readErr != nil {
			_ = rc.Close()
			return nil, readErr
		}

		if int64(len(payload)) > ps.maxPartSizeBytes {
			ps.markOversizedHint(cacheKey)
			return &oversizedMissResult{readCloser: &bytesPrefixReadCloser{prefix: payload, inner: rc}}, nil
		}

		_ = rc.Close()

		if setErr := ps.cache.Set(cacheKey, bytes.NewReader(payload), int64(len(payload))); setErr != nil {
			slog.WarnContext(ctx, "Failed to set part cache entry after backend read", "cacheKey", cacheKey, "error", setErr)
		}

		return payload, nil
	})
	if err == nil {
		payload, ok := v.([]byte)
		if ok {
			return io.NopCloser(bytes.NewReader(payload)), nil
		}
		overSized, ok := v.(*oversizedMissResult)
		if !ok {
			return nil, errors.New("invalid cache read payload type")
		}
		if !shared {
			return overSized.readCloser, nil
		}
		_ = overSized.readCloser.Close()
		return ps.innerPartStore.GetPart(ctx, tx, partId)
	}
	return nil, err
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

type oversizedMissResult struct {
	readCloser io.ReadCloser
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

type bytesPrefixReadCloser struct {
	prefix []byte
	inner  io.ReadCloser
}

func (r *bytesPrefixReadCloser) Read(p []byte) (int, error) {
	if len(r.prefix) > 0 {
		n := copy(p, r.prefix)
		r.prefix = r.prefix[n:]
		return n, nil
	}
	return r.inner.Read(p)
}

func (r *bytesPrefixReadCloser) Close() error {
	return r.inner.Close()
}

func readUpTo(reader io.Reader, limit int64) ([]byte, error) {
	if limit <= 0 {
		return []byte{}, nil
	}
	capHint := int(minInt64(limit, 64*1024))
	buf := make([]byte, 0, capHint)
	tmp := make([]byte, 32*1024)
	for int64(len(buf)) < limit {
		remaining := limit - int64(len(buf))
		chunk := tmp
		if remaining < int64(len(chunk)) {
			chunk = chunk[:remaining]
		}
		n, err := reader.Read(chunk)
		if n > 0 {
			buf = append(buf, chunk[:n]...)
		}
		if err == io.EOF {
			return buf, nil
		}
		if err != nil {
			return nil, err
		}
	}
	return buf, nil
}

func minInt64(a int64, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

type cacheOnReadCloser struct {
	io.ReadCloser
	cache            cachepkg.Cache
	cacheKey         string
	maxPartSizeBytes int64
	cacheEligible    bool
	buf              []byte
	closed           bool
	reachedEOF       bool
}

func (r *cacheOnReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	if n > 0 && r.cacheEligible {
		nextLen := int64(len(r.buf) + n)
		if nextLen <= r.maxPartSizeBytes {
			r.buf = append(r.buf, p[:n]...)
		} else {
			r.cacheEligible = false
			r.buf = nil
		}
	}
	if err == io.EOF {
		r.reachedEOF = true
		r.persistCache()
	}
	return n, err
}

func (r *cacheOnReadCloser) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.persistCache()
	return r.ReadCloser.Close()
}

func (r *cacheOnReadCloser) persistCache() {
	if !r.cacheEligible || !r.reachedEOF {
		return
	}
	_ = r.cache.Set(r.cacheKey, bytes.NewReader(r.buf), int64(len(r.buf)))
	r.cacheEligible = false
	r.buf = nil
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
