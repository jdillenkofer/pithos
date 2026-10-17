package sql

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/storage/database"
)

const mutationRefreshTimeout = 10 * time.Second

var _ authentication.CredentialLister = (*CredentialProvider)(nil)
var _ authentication.CredentialStore = (*CredentialProvider)(nil)

func (p *CredentialProvider) List(ctx context.Context) ([]authentication.CredentialInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return authentication.CredentialMetadata(p.coordinator.Snapshot().credentials), nil
}

func (p *CredentialProvider) Create(ctx context.Context, credential authentication.Credential) error {
	if err := authentication.ValidateCredential(credential); err != nil {
		return err
	}
	if err := authentication.ValidateCredentialAccountID(credential.AccountID); err != nil {
		return err
	}
	p.mutationMu.Lock()
	defer p.mutationMu.Unlock()
	err := database.WithTx(ctx, p.database, nil, func(ctx context.Context, tx database.Tx) error {
		result, err := tx.SqlTx().ExecContext(ctx, `INSERT INTO authentication_credentials
   (access_key_id, secret_access_key, account_id, principal_id)
   VALUES ($1, $2, $3, $4) ON CONFLICT (access_key_id) DO NOTHING`,
			credential.AccessKeyID, credential.SecretAccessKey, credential.AccountID, credential.PrincipalID)
		if err != nil {
			return authentication.ErrCredentialMutation
		}
		count, err := result.RowsAffected()
		if err != nil {
			return authentication.ErrCredentialMutation
		}
		if count == 0 {
			return authentication.ErrCredentialExists
		}
		return nil
	})
	if err == nil {
		p.publish(func(credentials map[string]authentication.Credential) {
			credentials[credential.AccessKeyID] = credential
		})
	}
	return p.finishMutation(ctx, err)
}

func (p *CredentialProvider) Delete(ctx context.Context, accessKeyID string) error {
	if len(accessKeyID) == 0 || len(accessKeyID) > authentication.MaxAccessKeyIDLength {
		return authentication.ErrCredentialNotFound
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mutationMu.Lock()
	defer p.mutationMu.Unlock()
	err := database.WithTx(ctx, p.database, nil, func(ctx context.Context, tx database.Tx) error {
		result, err := tx.SqlTx().ExecContext(ctx, `DELETE FROM authentication_credentials WHERE access_key_id = $1`, accessKeyID)
		if err != nil {
			return authentication.ErrCredentialMutation
		}
		count, err := result.RowsAffected()
		if err != nil {
			return authentication.ErrCredentialMutation
		}
		if count == 0 {
			return authentication.ErrCredentialNotFound
		}
		return nil
	})
	// Revocation fails closed: once a deletion was attempted, the key stops
	// authenticating even if the commit outcome is unknown, the row was already
	// removed out of band, or the follow-up reload fails. A later successful
	// reload restores the key only if it still exists in the database.
	p.publish(func(credentials map[string]authentication.Credential) {
		delete(credentials, accessKeyID)
	})
	return p.finishMutation(ctx, err)
}

func (p *CredentialProvider) publish(mutate func(map[string]authentication.Credential)) {
	p.coordinator.Update(func(current *credentialSnapshot) *credentialSnapshot {
		credentials := maps.Clone(current.credentials)
		if credentials == nil {
			credentials = make(map[string]authentication.Credential)
		}
		mutate(credentials)
		return &credentialSnapshot{credentials: credentials}
	})
}

func (p *CredentialProvider) finishMutation(ctx context.Context, err error) error {
	if err != nil {
		if errors.Is(err, authentication.ErrCredentialExists) || errors.Is(err, authentication.ErrCredentialNotFound) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return authentication.ErrCredentialMutation
	}
	// The committed change is already published; a full reload additionally
	// picks up concurrent out-of-band changes. Do not let a disconnected HTTP
	// client or a failed reload turn a committed mutation into an error.
	refreshCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), mutationRefreshTimeout)
	defer cancel()
	if err := p.coordinator.Refresh(refreshCtx); err != nil {
		slog.Warn("Failed to reload SQL credentials after a committed change; serving the updated last-known-good snapshot", "error", err)
	}
	return nil
}
