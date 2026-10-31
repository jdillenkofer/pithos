package sql

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/partregistry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/oklog/ulid/v2"
)

func refsForEntities(entities []part.Entity) []partregistry.Ref {
	ids := make([]partstore.PartId, len(entities))
	for i, entity := range entities {
		ids[i] = entity.PartId
	}
	return partregistry.RefsFromPartIds(ids)
}

// removePartEntities releases the references of deleted part rows. Parts
// that lose their last reference are reclaimed by GC after retention.
func (sms *sqlMetadataStore) removePartEntities(ctx context.Context, tx *sql.Tx, entities []part.Entity) error {
	return sms.partRegistryRepository.RemoveReferences(ctx, tx, refsForEntities(entities))
}

func (sms *sqlMetadataStore) removePartRowsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) error {
	entities, err := sms.partRepository.DeletePartsByObjectIdReturning(ctx, tx, objectId)
	if err != nil {
		return err
	}
	return sms.removePartEntities(ctx, tx, entities)
}

func (sms *sqlMetadataStore) removePartRowsByObjectIdAndSequenceNumber(ctx context.Context, tx *sql.Tx, objectId ulid.ULID, sequenceNumber int) error {
	entities, err := sms.partRepository.DeletePartsByObjectIdAndSequenceNumberReturning(ctx, tx, objectId, sequenceNumber)
	if err != nil {
		return err
	}
	return sms.removePartEntities(ctx, tx, entities)
}

func (sms *sqlMetadataStore) savePartRows(ctx context.Context, tx *sql.Tx, objectId ulid.ULID, parts []metadatastore.Part, sequenceOffset int) error {
	newRefs := map[partstore.PartId]*partregistry.Ref{}
	for i, p := range parts {
		entity := part.Entity{PartId: p.Id, ObjectId: objectId, ETag: p.ETag, ChecksumCRC32: p.ChecksumCRC32, ChecksumCRC32C: p.ChecksumCRC32C, ChecksumCRC64NVME: p.ChecksumCRC64NVME, ChecksumSHA1: p.ChecksumSHA1, ChecksumSHA256: p.ChecksumSHA256, Size: p.Size, SequenceNumber: sequenceOffset + i, PartStoreName: p.StoreName}
		if err := sms.partRepository.SavePart(ctx, tx, &entity); err != nil {
			return err
		}
		if p.RefPreAcquired {
			continue
		}
		if ref, ok := newRefs[p.Id]; ok {
			ref.Delta++
		} else {
			newRefs[p.Id] = &partregistry.Ref{PartId: p.Id, Delta: 1, StoreName: p.StoreName}
		}
	}
	refs := make([]partregistry.Ref, 0, len(newRefs))
	for _, ref := range newRefs {
		refs = append(refs, *ref)
	}
	return sms.partRegistryRepository.RegisterParts(ctx, tx, refs)
}
