package sqlite

import (
	"context"
	"database/sql"

	"github.com/jdillenkofer/pithos/internal/storage/database/repository/buckettag"
)

type sqliteRepository struct{}

func NewRepository() (buckettag.Repository, error) { return &sqliteRepository{}, nil }

func (r *sqliteRepository) FindTagsByBucketNameOrderByKeyAsc(ctx context.Context, tx *sql.Tx, bucketName string) ([]buckettag.Entity, error) {
	rows, err := tx.QueryContext(ctx, `SELECT bucket_name, key, value FROM bucket_tags WHERE bucket_name = $1 ORDER BY key ASC`, bucketName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tags := []buckettag.Entity{}
	for rows.Next() {
		var tag buckettag.Entity
		if err := rows.Scan(&tag.BucketName, &tag.Key, &tag.Value); err != nil {
			return nil, err
		}
		tags = append(tags, tag)
	}
	return tags, rows.Err()
}

func (r *sqliteRepository) SaveTag(ctx context.Context, tx *sql.Tx, tag buckettag.Entity) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO bucket_tags (bucket_name, key, value) VALUES ($1, $2, $3)`, tag.BucketName, tag.Key, tag.Value)
	return err
}

func (r *sqliteRepository) DeleteTagsByBucketName(ctx context.Context, tx *sql.Tx, bucketName string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM bucket_tags WHERE bucket_name = $1`, bucketName)
	return err
}
