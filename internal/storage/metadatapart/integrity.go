package metadatapart

import (
	"context"
	"database/sql"
	"io"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

var _ storage.IntegrityInspector = (*metadataPartStorage)(nil)

func (mbs *metadataPartStorage) InspectObject(ctx context.Context, bucket storage.BucketName, key storage.ObjectKey, versionID *string, inspect func(storage.Object, []storage.IntegrityPart) error) error {
	snapshotAt := database.SnapshotTime(ctx, mbs.db)
	return database.WithTx(ctx, mbs.db, &sql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		var object *metadatastore.Object
		var err error
		if versionID != nil {
			object, err = mbs.metadataStore.HeadObjectVersion(ctx, tx.SqlTx(), bucket, key, *versionID)
		} else {
			object, err = mbs.metadataStore.HeadObject(ctx, tx.SqlTx(), bucket, key)
		}
		if err != nil {
			return err
		}
		if object.IsDeleteMarker {
			return storage.ErrNoSuchKey
		}
		partIds := make([]partstore.PartId, 0, len(object.Parts))
		for _, part := range object.Parts {
			partIds = append(partIds, part.Id)
		}
		lease := mbs.readerLeases.begin(ctx, snapshotAt, partIds)
		defer lease.done()
		parts := make([]storage.IntegrityPart, 0, len(object.Parts))
		for _, part := range object.Parts {
			parts = append(parts, storage.IntegrityPart{
				ID: part.Id.String(), Size: part.Size,
				Checksums: storage.ChecksumValues{
					ETag:          ptrutils.ToPtr(part.ETag),
					ChecksumCRC32: part.ChecksumCRC32, ChecksumCRC32C: part.ChecksumCRC32C,
					ChecksumCRC64NVME: part.ChecksumCRC64NVME, ChecksumSHA1: part.ChecksumSHA1,
					ChecksumSHA256: part.ChecksumSHA256,
				},
				Open: func() (io.ReadCloser, error) {
					store, err := mbs.partStores.ByName(part.StoreName)
					if err != nil {
						return nil, err
					}
					rc, err := store.GetPart(ctx, tx, part.Id)
					if err != nil {
						return nil, err
					}
					return lease.wrap(rc), nil
				},
			})
		}
		return inspect(convertObject(*object), parts)
	})
}
