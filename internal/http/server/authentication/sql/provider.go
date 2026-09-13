package sql

import (
	"context"
	dbsql "database/sql"
	"fmt"
	"log/slog"
	"maps"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/storage/database"
)

type credentialSnapshot struct {
	credentials map[string]authentication.Credential
}

// CredentialProvider periodically loads the complete enabled credential set
// from a Pithos database and serves request lookups from an immutable snapshot.
type CredentialProvider struct {
	database    database.Database
	coordinator *authentication.SnapshotCoordinator[credentialSnapshot]
}

var _ authentication.CredentialProvider = (*CredentialProvider)(nil)

func NewCredentialProvider(ctx context.Context, db database.Database, reloadInterval time.Duration) (*CredentialProvider, error) {
	if db == nil {
		return nil, fmt.Errorf("credentials database must not be nil")
	}
	provider := &CredentialProvider{database: db}
	coordinator, err := authentication.NewSnapshotCoordinator(ctx, "sql", reloadInterval, provider.loadSnapshot,
		func(current, next *credentialSnapshot) bool { return maps.Equal(current.credentials, next.credentials) },
		func(next *credentialSnapshot) {
			slog.Info("Reloaded SQL credentials", "credentialCount", len(next.credentials))
		},
	)
	if err != nil {
		return nil, err
	}
	provider.coordinator = coordinator
	return provider, nil
}

func (p *CredentialProvider) loadSnapshot(ctx context.Context) (*credentialSnapshot, error) {
	credentials := make(map[string]authentication.Credential)
	err := database.WithTx(ctx, p.database, &dbsql.TxOptions{ReadOnly: true}, func(ctx context.Context, tx database.Tx) error {
		rows, err := tx.SqlTx().QueryContext(ctx, `
			SELECT access_key_id, secret_access_key, principal_id
			FROM authentication_credentials
			WHERE enabled = TRUE`)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var accessKeyID string
			var secretAccessKey string
			var principalID dbsql.NullString
			if err := rows.Scan(&accessKeyID, &secretAccessKey, &principalID); err != nil {
				return err
			}
			credential := authentication.Credential{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
			}
			if principalID.Valid {
				credential.PrincipalID = principalID.String
			}
			if err := authentication.ValidateCredential(credential); err != nil {
				return fmt.Errorf("invalid credential %q: %w", accessKeyID, err)
			}
			if _, exists := credentials[accessKeyID]; exists {
				return fmt.Errorf("duplicate access key ID %q", accessKeyID)
			}
			credentials[accessKeyID] = credential
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("load SQL credentials: %w", err)
	}
	return &credentialSnapshot{credentials: credentials}, nil
}

func (p *CredentialProvider) Lookup(ctx context.Context, accessKeyID string) (authentication.Credential, bool, error) {
	if err := ctx.Err(); err != nil {
		return authentication.Credential{}, false, err
	}
	snapshot := p.coordinator.Snapshot()
	credential, found := snapshot.credentials[accessKeyID]
	return credential, found, nil
}

func (p *CredentialProvider) Close() error { return p.coordinator.Close() }
