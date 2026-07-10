package metadatapart

import (
	"context"
	"log/slog"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
)

func normalizedPartStoreName(name *string) string {
	if name == nil {
		return ""
	}
	return *name
}

func dedupEntryForPart(storeName *string, id partstore.PartId, checksums *checksumutils.ChecksumValues, size int64) (metadatastore.PartDedupEntry, bool) {
	if checksums.ETag == nil || checksums.ChecksumCRC32 == nil || checksums.ChecksumCRC32C == nil || checksums.ChecksumCRC64NVME == nil || checksums.ChecksumSHA1 == nil || checksums.ChecksumSHA256 == nil {
		return metadatastore.PartDedupEntry{}, false
	}
	return metadatastore.PartDedupEntry{PartStoreName: normalizedPartStoreName(storeName), ChecksumSHA256: *checksums.ChecksumSHA256, Size: size, ETag: *checksums.ETag, ChecksumCRC32: *checksums.ChecksumCRC32, ChecksumCRC32C: *checksums.ChecksumCRC32C, ChecksumCRC64NVME: *checksums.ChecksumCRC64NVME, ChecksumSHA1: *checksums.ChecksumSHA1, PartId: id}, true
}

func dedupEntriesMatch(a, b metadatastore.PartDedupEntry) bool {
	return a.PartStoreName == b.PartStoreName && a.ChecksumSHA256 == b.ChecksumSHA256 && a.Size == b.Size && a.ETag == b.ETag && a.ChecksumCRC32 == b.ChecksumCRC32 && a.ChecksumCRC32C == b.ChecksumCRC32C && a.ChecksumCRC64NVME == b.ChecksumCRC64NVME && a.ChecksumSHA1 == b.ChecksumSHA1
}

func (mbs *metadataPartStorage) dedupeFreshPart(ctx context.Context, tx database.Tx, storeName *string, store partstore.PartStore, freshID partstore.PartId, checksums *checksumutils.ChecksumValues, size int64) (partstore.PartId, bool, error) {
	fresh, ok := dedupEntryForPart(storeName, freshID, checksums, size)
	if !ok {
		return freshID, false, nil
	}
	existing, err := mbs.metadataStore.LookupDedupPart(ctx, tx.SqlTx(), fresh.PartStoreName, fresh.ChecksumSHA256, fresh.Size)
	if err != nil {
		return freshID, false, err
	}
	if existing != nil {
		if dedupEntriesMatch(*existing, fresh) {
			added, err := mbs.metadataStore.TryAddPartReferences(ctx, tx.SqlTx(), []partstore.PartId{existing.PartId})
			if err != nil {
				return freshID, false, err
			}
			if added {
				if err := store.DeletePart(ctx, tx, freshID); err != nil {
					return freshID, false, err
				}
				return existing.PartId, true, nil
			}
			if err := mbs.metadataStore.DeletePartDedupEntries(ctx, tx.SqlTx(), []partstore.PartId{existing.PartId}); err != nil {
				return freshID, false, err
			}
		} else {
			slog.Warn("Part dedup index checksum mismatch", "part_id", existing.PartId.String(), "checksum_sha256", fresh.ChecksumSHA256, "size", fresh.Size)
		}
	}
	_, err = mbs.metadataStore.TryIndexDedupPart(ctx, tx.SqlTx(), fresh)
	return freshID, false, err
}
