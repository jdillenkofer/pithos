package repository

import (
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type BlobOutboxEntryRepository struct {
}

func NewBlobOutboxEntryRepository() BlobOutboxEntryRepository {
	return BlobOutboxEntryRepository{}
}

const (
	PutOperation    = "PUT"
	DeleteOperation = "DELETE"
)

type BlobOutboxEntryEntity struct {
	Id        *ulid.ULID
	Operation string
	BlobId    ulid.ULID
	Content   []byte
	Ordinal   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

func convertRowToBlobOutboxEntryEntity(blobOutboxRow *sql.Row) (*BlobOutboxEntryEntity, error) {
	var id string
	var operation string
	var blobId string
	var content []byte
	var ordinal int
	var createdAt time.Time
	var updatedAt time.Time
	err := blobOutboxRow.Scan(&id, &operation, &blobId, &content, &ordinal, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	ulidBlobId := ulid.MustParse(blobId)
	return &BlobOutboxEntryEntity{
		Id:        &ulidId,
		Operation: operation,
		BlobId:    ulidBlobId,
		Content:   content,
		Ordinal:   ordinal,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}
func (bor *BlobOutboxEntryRepository) NextOrdinal(tx *sql.Tx) (*int, error) {
	row := tx.QueryRow("SELECT COALESCE(MAX(ordinal), 0) + 1 FROM blob_outbox_entries")
	var ordinal int
	err := row.Scan(&ordinal)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return &ordinal, nil
}

func (bor *BlobOutboxEntryRepository) FindLastBlobOutboxEntryByBlobId(tx *sql.Tx, blobId ulid.ULID) (*BlobOutboxEntryEntity, error) {
	row := tx.QueryRow("SELECT id, operation, blob_id, content, ordinal, created_at, updated_at FROM blob_outbox_entries WHERE blob_id = ? ORDER BY ordinal DESC LIMIT 1", blobId.String())
	blobOutboxEntryEntity, err := convertRowToBlobOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return blobOutboxEntryEntity, nil
}

func (bor *BlobOutboxEntryRepository) FindFirstBlobOutboxEntry(tx *sql.Tx) (*BlobOutboxEntryEntity, error) {
	row := tx.QueryRow("SELECT id, operation, blob_id, content, ordinal, created_at, updated_at FROM blob_outbox_entries ORDER BY ordinal ASC LIMIT 1")
	blobOutboxEntryEntity, err := convertRowToBlobOutboxEntryEntity(row)
	if err != nil {
		return nil, err
	}
	return blobOutboxEntryEntity, nil
}

func (bor *BlobOutboxEntryRepository) SaveBlobOutboxEntry(tx *sql.Tx, blobOutboxEntry *BlobOutboxEntryEntity) error {
	if blobOutboxEntry.Id == nil {
		id := ulid.Make()
		blobOutboxEntry.Id = &id
		blobOutboxEntry.CreatedAt = time.Now()
		blobOutboxEntry.UpdatedAt = blobOutboxEntry.CreatedAt
		_, err := tx.Exec("INSERT INTO blob_outbox_entries (id, operation, blob_id, content, ordinal, created_at, updated_at) VALUES(?, ?, ?, ?, ?, ?, ?)", blobOutboxEntry.Id.String(), blobOutboxEntry.Operation, blobOutboxEntry.BlobId.String(), blobOutboxEntry.Content, blobOutboxEntry.Ordinal, blobOutboxEntry.CreatedAt, blobOutboxEntry.UpdatedAt)
		return err
	}

	blobOutboxEntry.UpdatedAt = time.Now()
	_, err := tx.Exec("UPDATE blob_outbox_entries SET operation = ?, blob_id = ?, content = ?, ordinal = ?, updated_at = ? WHERE id = ?", blobOutboxEntry.Operation, blobOutboxEntry.BlobId.String(), blobOutboxEntry.Content, blobOutboxEntry.Ordinal, blobOutboxEntry.UpdatedAt, blobOutboxEntry.Id.String())
	return err
}

func (bor *BlobOutboxEntryRepository) DeleteBlobOutboxEntryById(tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.Exec("DELETE FROM blob_outbox_entries WHERE id = ?", id.String())
	return err
}
