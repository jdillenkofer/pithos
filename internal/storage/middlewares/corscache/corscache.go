package corscache

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
)

// DefaultTTL bounds how long a resolved (or absent) CORS configuration is served
// from the cache. CORS configurations change rarely and browsers additionally
// cache preflight responses via Access-Control-Max-Age, so a short TTL keeps
// cross-instance staleness small. Same-instance writes invalidate immediately
// because they flow through this middleware.
const DefaultTTL = 60 * time.Second

type cacheEntry struct {
	config    *storage.BucketCORSConfiguration
	err       error
	expiresAt time.Time
}

// corsCacheStorageMiddleware caches per-bucket CORS configuration reads. The HTTP
// server resolves CORS rules on every Origin-bearing request; without this cache
// that would issue a storage lookup (and audit/metrics noise) per request. A
// cached "no CORS configured" result is a valid, common entry.
type corsCacheStorageMiddleware struct {
	delegator.DelegatingStorage
	ttl     time.Duration
	tracer  trace.Tracer
	mu      sync.RWMutex
	entries map[string]cacheEntry
}

var _ storage.Storage = (*corsCacheStorageMiddleware)(nil)
var _ storage.TransactionalStorage = (*corsCacheStorageMiddleware)(nil)

func NewStorageMiddleware(innerStorage storage.Storage) storage.Storage {
	return NewStorageMiddlewareWithTTL(innerStorage, DefaultTTL)
}

func NewStorageMiddlewareWithTTL(innerStorage storage.Storage, ttl time.Duration) storage.Storage {
	return &corsCacheStorageMiddleware{
		DelegatingStorage: delegator.Wrap(innerStorage),
		ttl:               ttl,
		tracer:            otel.Tracer("internal/storage/middlewares/corscache"),
		entries:           map[string]cacheEntry{},
	}
}

func (m *corsCacheStorageMiddleware) WithTransaction(ctx context.Context, opts *sql.TxOptions, fn func(ctx context.Context, txStorage storage.Storage) error) error {
	return delegator.WithTransaction(ctx, opts, m.Next, m, fn)
}

func (m *corsCacheStorageMiddleware) GetBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) (*storage.BucketCORSConfiguration, error) {
	ctx, span := m.tracer.Start(ctx, "CORSCacheStorageMiddleware.GetBucketCORSConfiguration")
	defer span.End()

	key := bucketName.String()
	if entry, ok := m.lookup(key); ok {
		return entry.config, entry.err
	}

	config, err := m.Next.GetBucketCORSConfiguration(ctx, bucketName)
	// Cache successful reads and the common "no CORS configured" miss. Transient
	// errors are not cached so they can recover on the next request.
	if err == nil || err == storage.ErrNoSuchCORSConfiguration {
		m.store(key, config, err)
	}
	return config, err
}

func (m *corsCacheStorageMiddleware) PutBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName, config *storage.BucketCORSConfiguration) error {
	ctx, span := m.tracer.Start(ctx, "CORSCacheStorageMiddleware.PutBucketCORSConfiguration")
	defer span.End()

	err := m.Next.PutBucketCORSConfiguration(ctx, bucketName, config)
	m.invalidate(bucketName.String())
	return err
}

func (m *corsCacheStorageMiddleware) DeleteBucketCORSConfiguration(ctx context.Context, bucketName storage.BucketName) error {
	ctx, span := m.tracer.Start(ctx, "CORSCacheStorageMiddleware.DeleteBucketCORSConfiguration")
	defer span.End()

	err := m.Next.DeleteBucketCORSConfiguration(ctx, bucketName)
	m.invalidate(bucketName.String())
	return err
}

func (m *corsCacheStorageMiddleware) lookup(key string) (cacheEntry, bool) {
	m.mu.RLock()
	entry, ok := m.entries[key]
	m.mu.RUnlock()
	if !ok || time.Now().After(entry.expiresAt) {
		return cacheEntry{}, false
	}
	return entry, true
}

func (m *corsCacheStorageMiddleware) store(key string, config *storage.BucketCORSConfiguration, err error) {
	m.mu.Lock()
	m.entries[key] = cacheEntry{config: config, err: err, expiresAt: time.Now().Add(m.ttl)}
	m.mu.Unlock()
}

func (m *corsCacheStorageMiddleware) invalidate(key string) {
	m.mu.Lock()
	delete(m.entries, key)
	m.mu.Unlock()
}
