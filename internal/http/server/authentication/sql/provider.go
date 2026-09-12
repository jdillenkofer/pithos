package sql

import (
	"context"
	dbsql "database/sql"
	"fmt"
	"log/slog"
	"maps"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/storage/database"
)

type credentialSnapshot struct {
	credentials map[string]authentication.Credential
}

// CredentialProvider periodically loads the complete enabled credential set
// from a Pithos database and serves request lookups from an immutable snapshot.
// Failed reloads retain the last-known-good snapshot.
type CredentialProvider struct {
	database       database.Database
	reloadInterval time.Duration
	lastCheck      time.Time
	reloadMu       sync.Mutex
	snapshot       atomic.Pointer[credentialSnapshot]
}

var _ authentication.CredentialProvider = (*CredentialProvider)(nil)

func NewCredentialProvider(ctx context.Context, db database.Database, reloadInterval time.Duration) (*CredentialProvider, error) {
	if db == nil {
		return nil, fmt.Errorf("credentials database must not be nil")
	}
	if reloadInterval < 0 {
		return nil, fmt.Errorf("credentials reload interval must not be negative")
	}

	provider := &CredentialProvider{database: db, reloadInterval: reloadInterval}
	snapshot, err := provider.loadSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	provider.snapshot.Store(snapshot)
	provider.lastCheck = time.Now()
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

func (p *CredentialProvider) reloadIfDue(ctx context.Context) error {
	p.reloadMu.Lock()
	defer p.reloadMu.Unlock()

	if p.reloadInterval > 0 && time.Since(p.lastCheck) < p.reloadInterval {
		return nil
	}
	p.lastCheck = time.Now()

	next, err := p.loadSnapshot(ctx)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		slog.Error("Failed to reload SQL credentials; retaining last-known-good credentials", "error", err)
		return nil
	}
	current := p.snapshot.Load()
	if current != nil && maps.Equal(current.credentials, next.credentials) {
		return nil
	}
	p.snapshot.Store(next)
	slog.Info("Reloaded SQL credentials", "credentialCount", len(next.credentials))
	return nil
}

func (p *CredentialProvider) Lookup(ctx context.Context, accessKeyID string) (authentication.Credential, bool, error) {
	if err := ctx.Err(); err != nil {
		return authentication.Credential{}, false, err
	}
	if err := p.reloadIfDue(ctx); err != nil {
		return authentication.Credential{}, false, err
	}
	snapshot := p.snapshot.Load()
	credential, found := snapshot.credentials[accessKeyID]
	return credential, found, nil
}
