// Package session persists admin sessions without storing bearer tokens or secrets.
package session

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
)

//go:embed migrations/*/*.sql
var migrations embed.FS

var ErrExpired = errors.New("admin session expired")

type Record struct {
	IDHash, AccessKeyID, AccountID, PrincipalID, CredentialHash, CSRFToken string
	CreatedAt, LastSeenAt, ExpiresAt                                       time.Time
	ClientIP, UserAgent                                                    string
}

type Repository struct {
	db   database.Database
	idle time.Duration
}

func NewRepository(ctx context.Context, db database.Database, idle time.Duration) (*Repository, error) {
	if db == nil || idle <= 0 {
		return nil, errors.New("admin sessions require a database and positive idle timeout")
	}
	dialect := "sqlite"
	switch db.GetDatabaseType() {
	case database.DB_TYPE_SQLITE:
	case database.DB_TYPE_POSTGRES:
		dialect = "postgres"
	default:
		return nil, errors.New("unsupported admin session database")
	}
	migration, err := migrations.ReadFile("migrations/" + dialect + "/001_admin_session.sql")
	if err != nil {
		return nil, err
	}
	err = database.WithTx(ctx, db, nil, func(ctx context.Context, tx database.Tx) error {
		_, err := tx.SqlTx().ExecContext(ctx, string(migration))
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("migrate admin sessions: %w", err)
	}
	return &Repository{db: db, idle: idle}, nil
}

func NewToken() (string, error) {
	token := make([]byte, 32)
	if _, err := rand.Read(token); err != nil {
		return "", err
	}
	return hex.EncodeToString(token), nil
}
func Hash(token string) string { h := sha256.Sum256([]byte(token)); return hex.EncodeToString(h[:]) }

func (r *Repository) Create(ctx context.Context, record Record) error {
	return database.WithTx(ctx, r.db, nil, func(ctx context.Context, tx database.Tx) error {
		_, err := tx.SqlTx().ExecContext(ctx, `INSERT INTO admin_session (id_hash,access_key_id,account_id,principal_id,credential_hash,csrf_token,created_at,last_seen_at,expires_at,client_ip,user_agent) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, record.IDHash, record.AccessKeyID, record.AccountID, record.PrincipalID, record.CredentialHash, record.CSRFToken, record.CreatedAt.UnixNano(), record.LastSeenAt.UnixNano(), record.ExpiresAt.UnixNano(), record.ClientIP, record.UserAgent)
		return err
	})
}

// GetAndTouch checks both expiry limits and touches only an unexpired session.
// The conditional UPDATE prevents concurrent requests from reviving an expired session.
func (r *Repository) GetAndTouch(ctx context.Context, idHash string, now time.Time) (Record, error) {
	var record Record
	err := database.WithTx(ctx, r.db, nil, func(ctx context.Context, tx database.Tx) error {
		result, err := tx.SqlTx().ExecContext(ctx, `UPDATE admin_session SET last_seen_at = CASE WHEN last_seen_at < $1 THEN $1 ELSE last_seen_at END WHERE id_hash=$2 AND expires_at>$1 AND last_seen_at>$3`, now.UnixNano(), idHash, now.Add(-r.idle).UnixNano())
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return ErrExpired
		}
		var created, last, expires int64
		err = tx.SqlTx().QueryRowContext(ctx, `SELECT id_hash,access_key_id,account_id,principal_id,credential_hash,csrf_token,created_at,last_seen_at,expires_at,client_ip,user_agent FROM admin_session WHERE id_hash=$1`, idHash).Scan(&record.IDHash, &record.AccessKeyID, &record.AccountID, &record.PrincipalID, &record.CredentialHash, &record.CSRFToken, &created, &last, &expires, &record.ClientIP, &record.UserAgent)
		record.CreatedAt = time.Unix(0, created)
		record.LastSeenAt = time.Unix(0, last)
		record.ExpiresAt = time.Unix(0, expires)
		return err
	})
	return record, err
}
func (r *Repository) Delete(ctx context.Context, idHash string) error {
	return database.WithTx(ctx, r.db, nil, func(ctx context.Context, tx database.Tx) error {
		_, err := tx.SqlTx().ExecContext(ctx, `DELETE FROM admin_session WHERE id_hash=$1`, idHash)
		return err
	})
}
func (r *Repository) Cleanup(ctx context.Context, now time.Time) error {
	return database.WithTx(ctx, r.db, nil, func(ctx context.Context, tx database.Tx) error {
		_, err := tx.SqlTx().ExecContext(ctx, `DELETE FROM admin_session WHERE expires_at <= $1 OR last_seen_at <= $2`, now.UnixNano(), now.Add(-r.idle).UnixNano())
		return err
	})
}

// RunCleanup exits when the server context is cancelled.
func (r *Repository) RunCleanup(ctx context.Context, onError func(error)) {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			if err := r.Cleanup(ctx, now); err != nil && onError != nil {
				onError(err)
			}
		}
	}
}
