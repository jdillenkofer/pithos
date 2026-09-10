package objectlock

import (
	"context"
	"database/sql"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/oklog/ulid/v2"
)

// Repository contains database-specific persistence and locking. All methods
// participate in the caller's transaction; locks are held until it ends.
type Repository interface {
	LockBucket(context.Context, *sql.Tx, metadatastore.BucketName) error
	LockObject(context.Context, *sql.Tx, ulid.ULID) error
	FindBucketConfiguration(context.Context, *sql.Tx, metadatastore.BucketName) (*metadatastore.ObjectLockConfiguration, error)
	SaveBucketConfiguration(context.Context, *sql.Tx, metadatastore.BucketName, *metadatastore.ObjectLockConfiguration) error
	FindObjectLock(context.Context, *sql.Tx, ulid.ULID) (metadatastore.ObjectLock, error)
	SaveObjectLock(context.Context, *sql.Tx, ulid.ULID, metadatastore.ObjectLock) error
}
