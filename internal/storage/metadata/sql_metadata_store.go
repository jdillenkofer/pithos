package metadata

import (
	"database/sql"
	"embed"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/blob"
	"github.com/oklog/ulid/v2"

	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/sqlite3"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

//go:embed migrations/*.sql
var migrationsFilesystem embed.FS

type SqlMetadataStore struct {
	db *sql.DB
}

func NewSqlMetadataStore(db *sql.DB) (*SqlMetadataStore, error) {
	enableForeignKeysStmt := `
	PRAGMA foreign_keys = ON;
	`
	_, err := db.Exec(enableForeignKeysStmt)
	if err != nil {
		return nil, err
	}

	sourceDriver, err := iofs.New(migrationsFilesystem, "migrations")
	if err != nil {
		return nil, err
	}

	databaseDriver, err := sqlite3.WithInstance(db, &sqlite3.Config{})
	if err != nil {
		return nil, err
	}
	m, err := migrate.NewWithInstance("iofs", sourceDriver, "sqlite3", databaseDriver)
	if err != nil {
		return nil, err
	}
	m.Up()

	return &SqlMetadataStore{
		db: db,
	}, nil
}

func (sms *SqlMetadataStore) CreateBucket(bucketName string) error {
	tx, err := sms.db.Begin()
	if err != nil {
		return err
	}
	result, err := tx.Query("SELECT id FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer result.Close()
	if result.Next() {
		tx.Rollback()
		return ErrBucketAlreadyExists
	}
	_, err = tx.Exec("INSERT INTO buckets (id, name, created_at, updated_at) VALUES(?, ?, datetime('now'), datetime('now'))", ulid.Make(), bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
}

func (sms *SqlMetadataStore) DeleteBucket(bucketName string) error {
	tx, err := sms.db.Begin()
	if err != nil {
		return err
	}
	result, err := tx.Query("SELECT id FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer result.Close()
	if !result.Next() {
		tx.Rollback()
		return ErrNoSuchBucket
	}
	result, err = tx.Query("SELECT id FROM objects WHERE bucket_name = ?", bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer result.Close()
	if result.Next() {
		tx.Rollback()
		return ErrBucketNotEmpty
	}
	_, err = tx.Exec("DELETE FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
}

func (sms *SqlMetadataStore) ListBuckets() ([]Bucket, error) {
	rows, err := sms.db.Query("SELECT name, created_at FROM buckets")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	buckets := []Bucket{}
	for rows.Next() {
		var name string
		var creationDate time.Time
		err = rows.Scan(&name, &creationDate)
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, Bucket{
			Name:         name,
			CreationDate: creationDate,
		})
	}
	return buckets, nil
}

func (sms *SqlMetadataStore) HeadBucket(bucketName string) (*Bucket, error) {
	rows, err := sms.db.Query("SELECT created_at FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, ErrNoSuchBucket
	}
	var creationDate time.Time
	err = rows.Scan(&creationDate)
	if err != nil {
		return nil, err
	}
	bucket := Bucket{
		Name:         bucketName,
		CreationDate: creationDate,
	}
	return &bucket, nil
}

func (sms *SqlMetadataStore) listObjects(bucketName string, prefix string, delimiter string, startAfter string, maxKeys int, tx *sql.Tx) ([]Object, []string, error) {
	rows, err := tx.Query("SELECT id, key, updated_at, etag, size FROM objects WHERE bucket_name = ? AND key LIKE ? || '%' AND key > ?", bucketName, prefix, startAfter)
	if err != nil {
		tx.Rollback()
		return nil, nil, err
	}
	defer rows.Close()
	commonPrefixes := []string{}
	objects := []Object{}
	for rows.Next() {
		var objectId string
		var key string
		var lastModified time.Time
		var etag string
		var size int64
		err = rows.Scan(&objectId, &key, &lastModified, &etag, &size)
		if err != nil {
			tx.Rollback()
			return nil, nil, err
		}
		blobRows, err := tx.Query("SELECT id FROM blobs WHERE object_id = ? ORDER BY sequence_number ASC", objectId)
		if err != nil {
			tx.Rollback()
			return nil, nil, err
		}
		defer blobRows.Close()
		blobIds := []blob.BlobId{}
		for blobRows.Next() {
			var blobId string
			err = blobRows.Scan(&blobId)
			if err != nil {
				tx.Rollback()
				return nil, nil, err
			}
			blobIds = append(blobIds, ulid.MustParse(blobId))
		}
		objects = append(objects, Object{
			Key:          key,
			LastModified: lastModified,
			ETag:         etag,
			Size:         size,
			BlobIds:      blobIds,
		})
		if len(objects)+len(commonPrefixes) == maxKeys {
			break
		}
	}
	tx.Commit()
	return objects, commonPrefixes, nil
}

func (sms *SqlMetadataStore) ListObjects(bucketName string, prefix string, delimiter string, startAfter string, maxKeys int) ([]Object, []string, error) {
	tx, err := sms.db.Begin()
	if err != nil {
		return nil, nil, err
	}
	rows, err := tx.Query("SELECT id FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		tx.Rollback()
		return nil, nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		tx.Rollback()
		return nil, nil, ErrNoSuchBucket
	}
	return sms.listObjects(bucketName, prefix, delimiter, startAfter, maxKeys, tx)
}

func (sms *SqlMetadataStore) HeadObject(bucketName string, key string) (*Object, error) {
	tx, err := sms.db.Begin()
	if err != nil {
		return nil, err
	}
	rows, err := tx.Query("SELECT id FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		tx.Rollback()
		return nil, ErrNoSuchBucket
	}
	rows, err = tx.Query("SELECT id, key, updated_at, etag, size FROM objects WHERE bucket_name = ? AND key = ?", bucketName, key)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		tx.Rollback()
		return nil, ErrNoSuchKey
	}
	var objectId string
	var lastModified time.Time
	var etag string
	var size int64
	err = rows.Scan(&objectId, &key, &lastModified, &etag, &size)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	rows, err = tx.Query("SELECT id FROM blobs WHERE object_id = ? ORDER BY sequence_number ASC", objectId)
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	defer rows.Close()
	blobIds := []blob.BlobId{}
	for rows.Next() {
		var blobId string
		err = rows.Scan(&blobId)
		if err != nil {
			tx.Rollback()
			return nil, err
		}
		blobIds = append(blobIds, ulid.MustParse(blobId))
	}
	err = tx.Commit()
	if err != nil {
		return nil, err
	}
	object := Object{
		Key:          key,
		LastModified: lastModified,
		ETag:         etag,
		Size:         size,
		BlobIds:      blobIds,
	}
	return &object, nil
}

func (sms *SqlMetadataStore) PutObject(bucketName string, object *Object) error {
	tx, err := sms.db.Begin()
	if err != nil {
		return err
	}
	rows, err := tx.Query("SELECT id FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		tx.Rollback()
		return ErrNoSuchBucket
	}
	rows, err = tx.Query("SELECT id FROM objects WHERE bucket_name = ? AND key = ?", bucketName, object.Key)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer rows.Close()
	if rows.Next() {
		// object already exists
		var objectId string
		err = rows.Scan(&objectId)
		if err != nil {
			tx.Rollback()
			return err
		}
		_, err = tx.Exec("DELETE FROM blobs WHERE object_id = ?", objectId)
		if err != nil {
			tx.Rollback()
			return err
		}
		_, err = tx.Exec("DELETE FROM objects WHERE id = ?", objectId)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	objectId := ulid.Make()
	_, err = tx.Exec("INSERT INTO objects (id, bucket_name, key, etag, size, created_at, updated_at) VALUES(?, ?, ?, ?, ?, datetime('now'), datetime('now'))", objectId.String(), bucketName, object.Key, object.ETag, object.Size)
	if err != nil {
		tx.Rollback()
		return err
	}
	sequenceNumber := 0
	for _, blobId := range object.BlobIds {
		_, err = tx.Exec("INSERT INTO blobs (id, object_id, sequence_number, created_at, updated_at) VALUES(?, ?, ?, datetime('now'), datetime('now'))", blobId.String(), objectId.String(), sequenceNumber)
		if err != nil {
			tx.Rollback()
			return err
		}
		sequenceNumber += 1
	}
	tx.Commit()
	return nil
}

func (sms *SqlMetadataStore) DeleteObject(bucketName string, key string) error {
	tx, err := sms.db.Begin()
	if err != nil {
		return err
	}
	rows, err := tx.Query("SELECT id FROM buckets WHERE name = ?", bucketName)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		tx.Rollback()
		return ErrNoSuchBucket
	}
	rows, err = tx.Query("SELECT id FROM objects WHERE bucket_name = ? AND key = ?", bucketName, key)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer rows.Close()
	if !rows.Next() {
		tx.Rollback()
		return ErrNoSuchKey
	}
	var objectId string
	err = rows.Scan(&objectId)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("DELETE FROM blobs WHERE object_id = ?", objectId)
	if err != nil {
		tx.Rollback()
		return err
	}

	_, err = tx.Exec("DELETE FROM objects WHERE id = ?", objectId)
	if err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}

func (sms *SqlMetadataStore) Clear() error {
	tx, err := sms.db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec("DELETE FROM blobs")
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = tx.Exec("DELETE FROM objects")
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = tx.Exec("DELETE FROM buckets")
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	return err
}
