// Package readerlease records which parts long-running readers still
// stream, so the part garbage collector does not condemn them.
//
// A lease expires TTL after it was acquired or last renewed. Readers stop
// serving content before their lease may have expired, and GC skips parts
// with an unexpired lease. The lease of a reader must be committed while
// GC retention alone still protects its parts; see metadatapart.
package readerlease

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	pgxPartReaderLease "github.com/jdillenkofer/pithos/internal/storage/database/pgx/repository/partreaderlease"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partreaderlease"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

type Store interface {
	// TTL is how long an acquired or renewed lease protects its parts.
	TTL() time.Duration
	// Acquire is idempotent: retrying it with the same lease id after an
	// error whose commit outcome is unknown succeeds.
	Acquire(ctx context.Context, leaseId string, partIds []partstore.PartId) error
	// Renew extends the unexpired leases among leaseIds. A lease the caller
	// no longer tracks, such as one whose release failed, is not renewed and
	// expires on its own.
	Renew(ctx context.Context, leaseIds []string) error
	// Release ends the leases among leaseIds in one statement.
	Release(ctx context.Context, leaseIds []string) error
	// Leased returns the subset of partIds held by an unexpired lease of any
	// instance sharing the database. It reads in tx when tx is not nil.
	Leased(ctx context.Context, tx database.Tx, partIds []partstore.PartId) (map[partstore.PartId]struct{}, error)
	DeleteExpired(ctx context.Context) (int64, error)
}

// New returns the lease store for db. PostgreSQL shares leases between all
// instances through the database. SQLite keeps them in memory: only one
// process opens a SQLite database (see sqlite.OldestOpenTransactionAge),
// and a lease written to the file would wait for its only write lock,
// which long uploads hold.
func New(db database.Database, ttl time.Duration) (Store, error) {
	if ttl <= 0 {
		return nil, fmt.Errorf("reader lease TTL must be positive")
	}
	switch db.GetDatabaseType() {
	case database.DB_TYPE_SQLITE:
		return NewMemoryStore(ttl), nil
	case database.DB_TYPE_POSTGRES:
		repository, err := pgxPartReaderLease.NewRepository()
		if err != nil {
			return nil, err
		}
		return &sqlStore{db: db, repository: repository, ttl: ttl}, nil
	default:
		return nil, fmt.Errorf("unknown database type %d", db.GetDatabaseType())
	}
}

type sqlStore struct {
	db         database.Database
	repository partreaderlease.Repository
	ttl        time.Duration
}

func (s *sqlStore) TTL() time.Duration { return s.ttl }

// withTx runs fn in a transaction of its own, never in a transaction the
// caller holds: a lease must commit independently of the read it protects.
func (s *sqlStore) withTx(ctx context.Context, readOnly bool, fn func(ctx context.Context, tx *sql.Tx) error) error {
	return database.WithTx(database.ContextWithoutTx(ctx), s.db, &sql.TxOptions{ReadOnly: readOnly}, func(ctx context.Context, tx database.Tx) error {
		return fn(ctx, tx.SqlTx())
	})
}

func (s *sqlStore) Acquire(ctx context.Context, leaseId string, partIds []partstore.PartId) error {
	return s.withTx(ctx, false, func(ctx context.Context, tx *sql.Tx) error {
		return s.repository.Acquire(ctx, tx, leaseId, partIds, s.ttl)
	})
}

func (s *sqlStore) Renew(ctx context.Context, leaseIds []string) error {
	return s.withTx(ctx, false, func(ctx context.Context, tx *sql.Tx) error {
		return s.repository.Renew(ctx, tx, leaseIds, s.ttl)
	})
}

func (s *sqlStore) Release(ctx context.Context, leaseIds []string) error {
	return s.withTx(ctx, false, func(ctx context.Context, tx *sql.Tx) error {
		return s.repository.Release(ctx, tx, leaseIds)
	})
}

func (s *sqlStore) Leased(ctx context.Context, tx database.Tx, partIds []partstore.PartId) (map[partstore.PartId]struct{}, error) {
	leased := make(map[partstore.PartId]struct{})
	if len(partIds) == 0 {
		return leased, nil
	}
	findLeased := func(ctx context.Context, tx *sql.Tx) error {
		ids, err := s.repository.FindLeased(ctx, tx, partIds)
		for _, id := range ids {
			leased[id] = struct{}{}
		}
		return err
	}
	if tx != nil {
		return leased, findLeased(ctx, tx.SqlTx())
	}
	return leased, s.withTx(ctx, true, findLeased)
}

func (s *sqlStore) DeleteExpired(ctx context.Context) (int64, error) {
	var deleted int64
	err := s.withTx(ctx, false, func(ctx context.Context, tx *sql.Tx) error {
		var err error
		deleted, err = s.repository.DeleteExpired(ctx, tx)
		return err
	})
	return deleted, err
}

type memoryLease struct {
	partIds   []partstore.PartId
	expiresAt time.Time
}

type memoryStore struct {
	mu     sync.Mutex
	ttl    time.Duration
	leases map[string]*memoryLease
}

// NewMemoryStore returns a store whose leases are visible to this process
// only.
func NewMemoryStore(ttl time.Duration) Store {
	return &memoryStore{ttl: ttl, leases: make(map[string]*memoryLease)}
}

func (s *memoryStore) TTL() time.Duration { return s.ttl }

func (s *memoryStore) Acquire(ctx context.Context, leaseId string, partIds []partstore.PartId) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leases[leaseId] = &memoryLease{partIds: append([]partstore.PartId(nil), partIds...), expiresAt: time.Now().Add(s.ttl)}
	return nil
}

func (s *memoryStore) Renew(ctx context.Context, leaseIds []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for _, id := range leaseIds {
		if lease, ok := s.leases[id]; ok && now.Before(lease.expiresAt) {
			lease.expiresAt = now.Add(s.ttl)
		}
	}
	return nil
}

func (s *memoryStore) Release(ctx context.Context, leaseIds []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range leaseIds {
		delete(s.leases, id)
	}
	return nil
}

func (s *memoryStore) Leased(ctx context.Context, tx database.Tx, partIds []partstore.PartId) (map[partstore.PartId]struct{}, error) {
	wanted := make(map[partstore.PartId]struct{}, len(partIds))
	for _, id := range partIds {
		wanted[id] = struct{}{}
	}
	leased := make(map[partstore.PartId]struct{})
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for _, lease := range s.leases {
		if !now.Before(lease.expiresAt) {
			continue
		}
		for _, id := range lease.partIds {
			if _, ok := wanted[id]; ok {
				leased[id] = struct{}{}
			}
		}
	}
	return leased, nil
}

func (s *memoryStore) DeleteExpired(ctx context.Context) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	var deleted int64
	for id, lease := range s.leases {
		if !now.Before(lease.expiresAt) {
			delete(s.leases, id)
			deleted++
		}
	}
	return deleted, nil
}
