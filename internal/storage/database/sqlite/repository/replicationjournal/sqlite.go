package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/replicationjournal"
	"io"
	"sort"
)

type repository struct{}

func NewRepository() (replicationjournal.Repository, error) { return &repository{}, nil }

func (r *repository) RegisterTopology(ctx context.Context, tx *sql.Tx, id string, secondaries []string) error {
	ids := append([]string(nil), secondaries...)
	sort.Strings(ids)
	encoded, err := json.Marshal(ids)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO replication_topologies (replication_id, secondary_ids) VALUES ($1, $2) ON CONFLICT (replication_id) DO NOTHING`, id, string(encoded)); err != nil {
		return err
	}
	var existing string
	if err := tx.QueryRowContext(ctx, `SELECT secondary_ids FROM replication_topologies WHERE replication_id = $1`, id).Scan(&existing); err != nil {
		return err
	}
	if existing != string(encoded) {
		return fmt.Errorf("replication topology %q has different secondary IDs; reconcile topology before changing it", id)
	}
	return nil
}

func (r *repository) SaveOperation(ctx context.Context, tx *sql.Tx, op *replicationjournal.Operation) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO replication_operations (id, replication_id, bucket_name, object_key, operation, payload, primary_result, state, attempts, last_error) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT(id) DO UPDATE SET payload = excluded.payload, primary_result = excluded.primary_result, state = excluded.state, attempts = excluded.attempts, last_error = excluded.last_error`, op.ID, op.ReplicationID, op.Bucket, op.Key, op.Name, op.Payload, op.PrimaryResult, op.State, op.Attempts, op.LastError)
	return err
}
func (r *repository) Pending(ctx context.Context, tx *sql.Tx, id string) ([]replicationjournal.Operation, error) {
	rows, err := tx.QueryContext(ctx, `SELECT id, replication_id, bucket_name, object_key, operation, payload, primary_result, state, attempts, last_error FROM replication_operations WHERE replication_id = $1 AND state <> 'COMPLETE' ORDER BY id`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []replicationjournal.Operation{}
	for rows.Next() {
		var op replicationjournal.Operation
		if err := rows.Scan(&op.ID, &op.ReplicationID, &op.Bucket, &op.Key, &op.Name, &op.Payload, &op.PrimaryResult, &op.State, &op.Attempts, &op.LastError); err != nil {
			return nil, err
		}
		result = append(result, op)
	}
	return result, rows.Err()
}
func (r *repository) Acknowledge(ctx context.Context, tx *sql.Tx, id, secondary, result string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO replication_acknowledgments (operation_id, secondary_id, result) VALUES ($1,$2,$3) ON CONFLICT(operation_id,secondary_id) DO UPDATE SET result = excluded.result`, id, secondary, result)
	return err
}
func (r *repository) Acknowledgments(ctx context.Context, tx *sql.Tx, id string) (map[string]string, error) {
	rows, err := tx.QueryContext(ctx, `SELECT secondary_id,result FROM replication_acknowledgments WHERE operation_id = $1`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]string{}
	for rows.Next() {
		var id, value string
		if err := rows.Scan(&id, &value); err != nil {
			return nil, err
		}
		result[id] = value
	}
	return result, rows.Err()
}
func (r *repository) SaveProgress(ctx context.Context, tx *sql.Tx, operationID, secondaryID, progress string) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO replication_progress (operation_id,secondary_id,progress) VALUES ($1,$2,$3) ON CONFLICT(operation_id,secondary_id) DO UPDATE SET progress = excluded.progress`, operationID, secondaryID, progress)
	return err
}
func (r *repository) FindProgress(ctx context.Context, tx *sql.Tx, operationID, secondaryID string) (*string, error) {
	var progress string
	err := tx.QueryRowContext(ctx, `SELECT progress FROM replication_progress WHERE operation_id=$1 AND secondary_id=$2`, operationID, secondaryID).Scan(&progress)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &progress, err
}
func (r *repository) DeleteProgress(ctx context.Context, tx *sql.Tx, operationID, secondaryID string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM replication_progress WHERE operation_id=$1 AND secondary_id=$2`, operationID, secondaryID)
	return err
}
func (r *repository) SaveMapping(ctx context.Context, tx *sql.Tx, m replicationjournal.Mapping) error {
	_, err := tx.ExecContext(ctx, `INSERT INTO replication_mappings (replication_id,secondary_id,bucket_name,object_key,kind,primary_id,secondary_object_id) VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(replication_id,secondary_id,bucket_name,object_key,kind,primary_id) DO UPDATE SET secondary_object_id = excluded.secondary_object_id`, m.ReplicationID, m.SecondaryID, m.Bucket, m.Key, m.Kind, m.PrimaryID, m.SecondaryObjectID)
	return err
}
func (r *repository) FindMapping(ctx context.Context, tx *sql.Tx, m replicationjournal.Mapping) (*string, error) {
	var id string
	err := tx.QueryRowContext(ctx, `SELECT secondary_object_id FROM replication_mappings WHERE replication_id=$1 AND secondary_id=$2 AND bucket_name=$3 AND object_key=$4 AND kind=$5 AND primary_id=$6`, m.ReplicationID, m.SecondaryID, m.Bucket, m.Key, m.Kind, m.PrimaryID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &id, err
}
func (r *repository) SaveData(ctx context.Context, tx *sql.Tx, id string, reader io.Reader) error {
	if err := r.DeleteData(ctx, tx, id); err != nil {
		return err
	}
	data := make([]byte, 1024*1024)
	for sequence := 0; ; sequence++ {
		n, err := io.ReadFull(reader, data)
		if n > 0 {
			if _, writeErr := tx.ExecContext(ctx, `INSERT INTO replication_data (operation_id,sequence_number,data) VALUES ($1,$2,$3)`, id, sequence, data[:n]); writeErr != nil {
				return writeErr
			}
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
func (r *repository) DeleteData(ctx context.Context, tx *sql.Tx, id string) error {
	_, err := tx.ExecContext(ctx, `DELETE FROM replication_data WHERE operation_id=$1`, id)
	return err
}
func (r *repository) ReadData(ctx context.Context, tx *sql.Tx, id string) (io.ReadCloser, error) {
	rows, err := tx.QueryContext(ctx, `SELECT data FROM replication_data WHERE operation_id=$1 ORDER BY sequence_number`, id)
	if err != nil {
		return nil, err
	}
	return &dataReader{rows: rows}, nil
}

type dataReader struct {
	rows *sql.Rows
	data []byte
}

func (r *dataReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for len(r.data) == 0 {
		if !r.rows.Next() {
			if err := r.rows.Err(); err != nil {
				return 0, err
			}
			return 0, io.EOF
		}
		if err := r.rows.Scan(&r.data); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}
func (r *dataReader) Close() error { return r.rows.Close() }
