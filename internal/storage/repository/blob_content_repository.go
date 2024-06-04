package repository

import (
	"database/sql"
	"time"

	"github.com/oklog/ulid/v2"
)

type BlobContentRepository struct {
}

func NewBlobContentRepository() BlobContentRepository {
	return BlobContentRepository{}
}

type BlobContentEntity struct {
	Id        *ulid.ULID
	Content   []byte
	CreatedAt time.Time
	UpdatedAt time.Time
}

func convertRowToBlobContentEntity(blobContentRow *sql.Row) (*BlobContentEntity, error) {
	var id string
	var content []byte
	var createdAt time.Time
	var updatedAt time.Time
	err := blobContentRow.Scan(&id, &content, &createdAt, &updatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	ulidId := ulid.MustParse(id)
	return &BlobContentEntity{
		Id:        &ulidId,
		Content:   content,
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
	}, nil
}

func (bcr *BlobContentRepository) FindBlobContentById(tx *sql.Tx, blobContentId ulid.ULID) (*BlobContentEntity, error) {
	row := tx.QueryRow("SELECT id, content, created_at, updated_at FROM blob_contents WHERE id = ?", blobContentId.String())
	blobContentEntity, err := convertRowToBlobContentEntity(row)
	if err != nil {
		return nil, err
	}
	return blobContentEntity, nil
}

func (bcr *BlobContentRepository) SaveBlobContent(tx *sql.Tx, blobContent *BlobContentEntity) error {
	if blobContent.Id == nil {
		id := ulid.Make()
		blobContent.Id = &id
		blobContent.CreatedAt = time.Now()
		blobContent.UpdatedAt = blobContent.CreatedAt
		_, err := tx.Exec("INSERT INTO blob_contents (id, content, created_at, updated_at) VALUES(?, ?, ?, ?)", blobContent.Id.String(), blobContent.Content, blobContent.CreatedAt, blobContent.UpdatedAt)
		return err
	}

	blobContent.UpdatedAt = time.Now()
	_, err := tx.Exec("UPDATE blob_contents SET content = ?, updated_at = ? WHERE id = ?", blobContent.Content, blobContent.UpdatedAt, blobContent.Id.String())
	return err
}

func (bcr *BlobContentRepository) DeleteBlobContentById(tx *sql.Tx, id ulid.ULID) error {
	_, err := tx.Exec("DELETE FROM blob_contents WHERE id = ?", id.String())
	return err
}
