package cache

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"io"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	cachepkg "github.com/jdillenkofer/pithos/internal/storage/cache"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

const defaultMaxPartSizeBytes int64 = 32 * 1024 * 1024

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

	if teed.cacheEligible {
		if err := ps.cache.Set(cacheKey, buf); err != nil {
			_ = ps.cache.Remove(cacheKey)
		}
	} else {
		_ = ps.cache.Remove(cacheKey)
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
	}
	if err == nil {
		return io.NopCloser(bytes.NewReader(data)), nil
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

	_ = ps.cache.Remove(getPartCacheKey(partId))
	return nil
}

type cacheOnReadCloser struct {
	io.ReadCloser
	cache            cachepkg.Cache
	cacheKey         string
	maxPartSizeBytes int64
	cacheEligible    bool
	buf              []byte
	closed           bool
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
	if !r.cacheEligible {
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
