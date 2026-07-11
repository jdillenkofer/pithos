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

func partFromEntity(entity part.Entity) metadatastore.Part {
	return metadatastore.Part{Id: entity.PartId, ETag: entity.ETag, ChecksumCRC32: entity.ChecksumCRC32, ChecksumCRC32C: entity.ChecksumCRC32C, ChecksumCRC64NVME: entity.ChecksumCRC64NVME, ChecksumSHA1: entity.ChecksumSHA1, ChecksumSHA256: entity.ChecksumSHA256, Size: entity.Size, StoreName: entity.PartStoreName}
}

func refsForEntities(entities []part.Entity) []partregistry.Ref {
	ids := make([]partstore.PartId, len(entities))
	for i, entity := range entities {
		ids[i] = entity.PartId
	}
	return partregistry.RefsFromPartIds(ids)
}

func (sms *sqlMetadataStore) removePartEntities(ctx context.Context, tx *sql.Tx, entities []part.Entity) ([]metadatastore.Part, error) {
	zeroIds, err := sms.partRegistryRepository.RemoveReferences(ctx, tx, refsForEntities(entities))
	if err != nil {
		return nil, err
	}
	if err := sms.partDedupIndexRepository.DeleteByPartIds(ctx, tx, zeroIds); err != nil {
		return nil, err
	}
	byId := map[partstore.PartId]metadatastore.Part{}
	for _, entity := range entities {
		byId[entity.PartId] = partFromEntity(entity)
	}
	unreferenced := make([]metadatastore.Part, 0, len(zeroIds))
	for _, id := range zeroIds {
		if p, ok := byId[id]; ok {
			unreferenced = append(unreferenced, p)
		}
	}
	return unreferenced, nil
}

func (sms *sqlMetadataStore) removePartRowsByObjectId(ctx context.Context, tx *sql.Tx, objectId ulid.ULID) ([]metadatastore.Part, error) {
	entities, err := sms.partRepository.DeletePartsByObjectIdReturning(ctx, tx, objectId)
	if err != nil {
		return nil, err
	}
	return sms.removePartEntities(ctx, tx, entities)
}

func (sms *sqlMetadataStore) removePartRowsByObjectIdAndSequenceNumber(ctx context.Context, tx *sql.Tx, objectId ulid.ULID, sequenceNumber int) ([]metadatastore.Part, error) {
	entities, err := sms.partRepository.DeletePartsByObjectIdAndSequenceNumberReturning(ctx, tx, objectId, sequenceNumber)
	if err != nil {
		return nil, err
	}
	return sms.removePartEntities(ctx, tx, entities)
}

func (sms *sqlMetadataStore) savePartRows(ctx context.Context, tx *sql.Tx, objectId ulid.ULID, parts []metadatastore.Part, sequenceOffset int) error {
	newIds := []partstore.PartId{}
	for i, p := range parts {
		entity := part.Entity{PartId: p.Id, ObjectId: objectId, ETag: p.ETag, ChecksumCRC32: p.ChecksumCRC32, ChecksumCRC32C: p.ChecksumCRC32C, ChecksumCRC64NVME: p.ChecksumCRC64NVME, ChecksumSHA1: p.ChecksumSHA1, ChecksumSHA256: p.ChecksumSHA256, Size: p.Size, SequenceNumber: sequenceOffset + i, PartStoreName: p.StoreName}
		if err := sms.partRepository.SavePart(ctx, tx, &entity); err != nil {
			return err
		}
		if !p.RefPreAcquired {
			newIds = append(newIds, p.Id)
		}
	}
	return sms.partRegistryRepository.RegisterParts(ctx, tx, partregistry.RefsFromPartIds(newIds))
}
