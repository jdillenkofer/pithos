package sql

import (
	"context"
	dbsql "database/sql"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestCredentialManagement(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, interval := range []time.Duration{0, time.Millisecond} {
		t.Run(interval.String(), func(t *testing.T) {
			ctx := context.Background()
			db := openTestDatabase(t)
			execute(t, db, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, account_id, principal_id, enabled) VALUES (?, ?, ?, ?, FALSE)`, "disabled", "disabled-secret", "account", "principal")
			provider, err := NewCredentialProvider(ctx, db, interval)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, provider.Close()) })
			credential := authentication.Credential{AccessKeyID: "key", SecretAccessKey: "never-list-this-secret", AccountID: "account_1", PrincipalID: "principal"}
			require.NoError(t, provider.Create(ctx, credential))
			foundCredential, found, err := provider.Lookup(ctx, "key")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, credential, foundCredential)
			infos, err := provider.List(ctx)
			require.NoError(t, err)
			require.Equal(t, []authentication.CredentialInfo{{AccessKeyID: "key", AccountID: "account_1", PrincipalID: "principal"}}, infos)
			encoded, err := json.Marshal(infos)
			require.NoError(t, err)
			require.NotContains(t, strings.ToLower(string(encoded)), "secret")
			infos[0].AccountID = "changed"
			foundCredential, _, err = provider.Lookup(ctx, "key")
			require.NoError(t, err)
			require.Equal(t, "account_1", foundCredential.AccountID)
			require.ErrorIs(t, provider.Create(ctx, credential), authentication.ErrCredentialExists)
			require.NoError(t, provider.Delete(ctx, "key"))
			_, found, err = provider.Lookup(ctx, "key")
			require.NoError(t, err)
			require.False(t, found)
			infos, err = provider.List(ctx)
			require.NoError(t, err)
			require.Empty(t, infos)
			require.ErrorIs(t, provider.Delete(ctx, "key"), authentication.ErrCredentialNotFound)
			require.NoError(t, provider.Create(ctx, credential))
		})
	}
}

func TestCredentialManagementValidationAndSafeErrors(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db := openTestDatabase(t)
	provider, err := NewCredentialProvider(ctx, db, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	credential := authentication.Credential{AccessKeyID: "key", SecretAccessKey: "sensitive-secret", AccountID: "../unsafe", PrincipalID: "principal"}
	require.Error(t, provider.Create(ctx, credential))
	credential.AccountID = "account"
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	require.ErrorIs(t, provider.Create(canceled, credential), context.Canceled)
	_, err = provider.List(canceled)
	require.ErrorIs(t, err, context.Canceled)
	require.ErrorIs(t, provider.Delete(canceled, "key"), context.Canceled)
	require.NoError(t, db.Close())
	err = provider.Create(ctx, credential)
	require.ErrorIs(t, err, authentication.ErrCredentialMutation)
	require.NotContains(t, err.Error(), credential.SecretAccessKey)
}

// failingReloadDatabase fails the read-only transactions used by snapshot
// reloads while letting credential mutations commit.
type failingReloadDatabase struct {
	database.Database
	failReloads atomic.Bool
}

func (d *failingReloadDatabase) BeginTx(ctx context.Context, opts *dbsql.TxOptions) (*database.TxController, error) {
	if opts != nil && opts.ReadOnly && d.failReloads.Load() {
		return nil, errors.New("injected reload failure")
	}
	return d.Database.BeginTx(ctx, opts)
}

func TestCredentialMutationsArePublishedWhenReloadFails(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, interval := range []time.Duration{0, time.Millisecond} {
		t.Run(interval.String(), func(t *testing.T) {
			ctx := context.Background()
			db := &failingReloadDatabase{Database: openTestDatabase(t)}
			execute(t, db, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, account_id, principal_id) VALUES (?, ?, ?, ?)`, "revoked", "secret", "account", "principal")
			provider, err := NewCredentialProvider(ctx, db, interval)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, provider.Close()) })
			db.failReloads.Store(true)

			require.NoError(t, provider.Delete(ctx, "revoked"))
			_, found, err := provider.Lookup(ctx, "revoked")
			require.NoError(t, err)
			require.False(t, found, "deleted credential must be revoked although the reload failed")

			created := authentication.Credential{AccessKeyID: "created", SecretAccessKey: "secret", AccountID: "account", PrincipalID: "principal"}
			require.NoError(t, provider.Create(ctx, created))
			credential, found, err := provider.Lookup(ctx, "created")
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, created, credential)

			// Background reloads keep failing; the published state must persist.
			time.Sleep(20 * time.Millisecond)
			_, found, err = provider.Lookup(ctx, "revoked")
			require.NoError(t, err)
			require.False(t, found)

			db.failReloads.Store(false)
			require.ErrorIs(t, provider.Delete(ctx, "revoked"), authentication.ErrCredentialNotFound)
			infos, err := provider.List(ctx)
			require.NoError(t, err)
			require.Equal(t, []authentication.CredentialInfo{{AccessKeyID: "created", AccountID: "account", PrincipalID: "principal"}}, infos)
		})
	}
}

func TestCredentialDeleteRetryRevokesRowRemovedOutOfBand(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db := openTestDatabase(t)
	execute(t, db, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, account_id, principal_id) VALUES (?, ?, ?, ?)`, "key", "secret", "account", "principal")
	provider, err := NewCredentialProvider(ctx, db, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	execute(t, db, `DELETE FROM authentication_credentials WHERE access_key_id = ?`, "key")
	_, found, err := provider.Lookup(ctx, "key")
	require.NoError(t, err)
	require.True(t, found, "reload is disabled, so the startup snapshot still contains the key")

	require.ErrorIs(t, provider.Delete(ctx, "key"), authentication.ErrCredentialNotFound)
	_, found, err = provider.Lookup(ctx, "key")
	require.NoError(t, err)
	require.False(t, found)
}

func TestCredentialDeleteRevokesWhenCommitFails(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db := openTestDatabase(t)
	execute(t, db, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, account_id, principal_id) VALUES (?, ?, ?, ?)`, "key", "secret", "account", "principal")
	provider, err := NewCredentialProvider(ctx, db, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	require.NoError(t, db.Close())

	require.ErrorIs(t, provider.Delete(ctx, "key"), authentication.ErrCredentialMutation)
	_, found, err := provider.Lookup(ctx, "key")
	require.NoError(t, err)
	require.False(t, found)
}

func TestConcurrentCredentialMutationsPublishInCommitOrder(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	db := &failingReloadDatabase{Database: openTestDatabase(t)}
	provider, err := NewCredentialProvider(ctx, db, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	db.failReloads.Store(true)
	credential := authentication.Credential{AccessKeyID: "key", SecretAccessKey: "secret", AccountID: "account", PrincipalID: "principal"}
	for range 50 {
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = provider.Create(ctx, credential)
		}()
		_ = provider.Delete(ctx, "key")
		<-done
		// Whatever order won, the published snapshot must match the database.
		var rows int
		require.NoError(t, database.WithTx(ctx, db.Database, nil, func(ctx context.Context, tx database.Tx) error {
			return tx.SqlTx().QueryRowContext(ctx, `SELECT COUNT(*) FROM authentication_credentials WHERE access_key_id = ?`, "key").Scan(&rows)
		}))
		_, found, err := provider.Lookup(ctx, "key")
		require.NoError(t, err)
		require.Equal(t, rows == 1, found)
		_ = provider.Delete(ctx, "key")
	}
}
