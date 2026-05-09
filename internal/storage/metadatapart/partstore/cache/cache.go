package cache

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/singleflight"

	cachepkg "github.com/jdillenkofer/pithos/internal/cache"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

// Production default: cache parts up to 64 MiB.
// This balances hit rate and memory pressure for common multipart workloads.
const defaultMaxPartSizeBytes int64 = 64 * 1024 * 1024

type Options struct {
	MaxPartSizeBytes               int64
	CacheReadErrorsAsMiss          bool
	MutatingOpsAffectCacheWithinTx bool
}

type cachePartStore struct {
	innerPartStore                 partstore.PartStore
	cache                          cachepkg.Cache
	maxPartSizeBytes               int64
	cacheReadErrorsAsMiss          bool
	mutatingOpsAffectCacheWithinTx bool
	tracer                         trace.Tracer
	readGroup                      singleflight.Group
}

var _ partstore.PartStore = (*cachePartStore)(nil)

func New(cache cachepkg.Cache, innerPartStore partstore.PartStore, opts Options) (partstore.PartStore, error) {
	maxPartSizeBytes := opts.MaxPartSizeBytes
	if maxPartSizeBytes <= 0 {
		maxPartSizeBytes = defaultMaxPartSizeBytes
	}

	return &cachePartStore{
		innerPartStore:                 innerPartStore,
		cache:                          cache,
		maxPartSizeBytes:               maxPartSizeBytes,
		cacheReadErrorsAsMiss:          opts.CacheReadErrorsAsMiss,
		mutatingOpsAffectCacheWithinTx: opts.MutatingOpsAffectCacheWithinTx,
		tracer:                         otel.Tracer("internal/storage/metadatapart/partstore/cache"),
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

func (ps *cachePartStore) PutPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId, reader io.Reader) error {
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
	if tx != nil && !ps.mutatingOpsAffectCacheWithinTx {
		slog.DebugContext(ctx, "Skipping cache mutation for PutPart inside transaction", "cacheKey", cacheKey)
		return nil
	}

	if teed.cacheEligible {
		if err := ps.cache.Set(cacheKey, buf); err != nil {
			slog.WarnContext(ctx, "Failed to write part to cache", "cacheKey", cacheKey, "error", err)
			_ = ps.cache.Remove(cacheKey)
		}
	} else {
		if err := ps.cache.Remove(cacheKey); err != nil {
			slog.DebugContext(ctx, "Failed to remove oversized part from cache", "cacheKey", cacheKey, "error", err)
		}
	}

	return nil
}

func (ps *cachePartStore) GetPart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	ctx, span := ps.tracer.Start(ctx, "cachePartStore.GetPart")
	defer span.End()

	cacheKey := getPartCacheKey(partId)
	data, err := ps.cache.Get(cacheKey)
	if err != nil && err != cachepkg.ErrCacheMiss {
		if !ps.cacheReadErrorsAsMiss {
			return nil, err
		}
		slog.WarnContext(ctx, "Treating cache read error as cache miss", "cacheKey", cacheKey, "error", err)
	}
	if err == nil {
		return io.NopCloser(bytes.NewReader(data)), nil
	}

	v, err, _ := ps.readGroup.Do(cacheKey, func() (interface{}, error) {
		// Re-check cache while serialized to avoid duplicate backend fetches.
		cached, cacheErr := ps.cache.Get(cacheKey)
		if cacheErr == nil {
			return cached, nil
		}
		if cacheErr != nil && cacheErr != cachepkg.ErrCacheMiss && !ps.cacheReadErrorsAsMiss {
			return nil, cacheErr
		}

		rc, innerErr := ps.innerPartStore.GetPart(ctx, tx, partId)
		if innerErr != nil {
			return nil, innerErr
		}
		defer rc.Close()

		limitedReader := io.LimitReader(rc, ps.maxPartSizeBytes+1)
		payload, readErr := io.ReadAll(limitedReader)
		if readErr != nil {
			return nil, readErr
		}

		if int64(len(payload)) > ps.maxPartSizeBytes {
			return nil, errPartLargerThanCacheThreshold
		}

		if cacheErr = ps.cache.Set(cacheKey, payload); cacheErr != nil {
			slog.WarnContext(ctx, "Failed to set part cache entry after backend read", "cacheKey", cacheKey, "error", cacheErr)
		}

		return payload, nil
	})
	if err == nil {
		payload, ok := v.([]byte)
		if !ok {
			return nil, errors.New("invalid cache read payload type")
		}
		return io.NopCloser(bytes.NewReader(payload)), nil
	}
	if !errors.Is(err, errPartLargerThanCacheThreshold) {
		return nil, err
	}

	rc, err := ps.innerPartStore.GetPart(ctx, tx, partId)
	if err != nil {
		return nil, err
	}

	return &cacheOnReadCloser{
		ReadCloser:       rc,
		cache:            ps.cache,
		cacheKey:         cacheKey,
		maxPartSizeBytes: ps.maxPartSizeBytes,
		cacheEligible:    true,
		buf:              make([]byte, 0),
	}, nil
}

func (ps *cachePartStore) GetPartIds(ctx context.Context, tx *sql.Tx) ([]partstore.PartId, error) {
	ctx, span := ps.tracer.Start(ctx, "cachePartStore.GetPartIds")
	defer span.End()

	return ps.innerPartStore.GetPartIds(ctx, tx)
}

func (ps *cachePartStore) DeletePart(ctx context.Context, tx *sql.Tx, partId partstore.PartId) error {
	ctx, span := ps.tracer.Start(ctx, "cachePartStore.DeletePart")
	defer span.End()

	err := ps.innerPartStore.DeletePart(ctx, tx, partId)
	if err != nil {
		return err
	}
	if tx != nil && !ps.mutatingOpsAffectCacheWithinTx {
		slog.DebugContext(ctx, "Skipping cache mutation for DeletePart inside transaction", "cacheKey", getPartCacheKey(partId))
		return nil
	}

	cacheKey := getPartCacheKey(partId)
	if err := ps.cache.Remove(cacheKey); err != nil {
		slog.DebugContext(ctx, "Failed to remove part from cache on delete", "cacheKey", cacheKey, "error", err)
	}
	return nil
}

var errPartLargerThanCacheThreshold = errors.New("part larger than cache threshold")

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
	_ = r.cache.Set(r.cacheKey, r.buf)
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
