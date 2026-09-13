package sql

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/sqlite"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTestDatabase(t *testing.T) database.Database {
	t.Helper()
	db, err := sqlite.OpenDatabase(filepath.Join(t.TempDir(), "credentials.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func execute(t *testing.T, db database.Database, statement string, args ...any) {
	t.Helper()
	err := database.WithTx(context.Background(), db, nil, func(ctx context.Context, tx database.Tx) error {
		_, err := tx.SqlTx().ExecContext(ctx, statement, args...)
		return err
	})
	require.NoError(t, err)
}

func TestCredentialProviderReloadsDatabaseSnapshot(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db := openTestDatabase(t)
	execute(t, db, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, account_id, principal_id) VALUES (?, ?, ?, ?)`, "old-key", "old-secret", "account", "client")
	execute(t, db, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, account_id, principal_id) VALUES (?, ?, ?, ?)`, "rotated-key", "rotated-secret", "account", "client")

	provider, err := NewCredentialProvider(context.Background(), db, 10*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })

	credential, found, err := provider.Lookup(context.Background(), "old-key")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "old-secret", credential.SecretAccessKey)
	assert.Equal(t, "client", credential.PrincipalID)

	credential, found, err = provider.Lookup(context.Background(), "rotated-key")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "account", credential.AccountID)

	execute(t, db, `UPDATE authentication_credentials SET enabled = FALSE, version = version + 1, updated_at = CURRENT_TIMESTAMP WHERE access_key_id = ?`, "old-key")
	execute(t, db, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, account_id, principal_id) VALUES (?, ?, ?, ?)`, "new-key", "new-secret", "account", "client")

	require.Eventually(t, func() bool {
		credential, found, err = provider.Lookup(context.Background(), "new-key")
		return err == nil && found && credential.PrincipalID == "client"
	}, time.Second, 5*time.Millisecond)
	_, found, err = provider.Lookup(context.Background(), "old-key")
	require.NoError(t, err)
	assert.False(t, found)
}

func TestCredentialProviderHonorsReloadInterval(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db := openTestDatabase(t)
	execute(t, db, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, account_id, principal_id) VALUES (?, ?, ?, ?)`, "key", "old-secret", "account", "principal")
	provider, err := NewCredentialProvider(context.Background(), db, time.Hour)
	require.NoError(t, err)
	execute(t, db, `UPDATE authentication_credentials SET secret_access_key = ? WHERE access_key_id = ?`, "new-secret", "key")

	credential, found, err := provider.Lookup(context.Background(), "key")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "old-secret", credential.SecretAccessKey)
}

func TestCredentialProviderRetainsSnapshotOnReloadFailure(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db := openTestDatabase(t)
	execute(t, db, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, account_id, principal_id) VALUES (?, ?, ?, ?)`, "key", "secret", "account", "principal")
	provider, err := NewCredentialProvider(context.Background(), db, 10*time.Millisecond)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	require.NoError(t, db.Close())

	require.Eventually(t, func() bool {
		credential, found, err := provider.Lookup(context.Background(), "key")
		return err == nil && found && credential.SecretAccessKey == "secret"
	}, time.Second, 5*time.Millisecond)
}

func TestCredentialProviderZeroReloadIntervalKeepsStartupSnapshot(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db := openTestDatabase(t)
	execute(t, db, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, account_id, principal_id) VALUES (?, ?, ?, ?)`, "key", "old-secret", "account", "principal")
	provider, err := NewCredentialProvider(context.Background(), db, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, provider.Close()) })
	execute(t, db, `UPDATE authentication_credentials SET secret_access_key = ? WHERE access_key_id = ?`, "new-secret", "key")
	time.Sleep(25 * time.Millisecond)

	credential, found, err := provider.Lookup(context.Background(), "key")
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "old-secret", credential.SecretAccessKey)
}

func TestCredentialProviderHonorsContext(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db := openTestDatabase(t)
	provider, err := NewCredentialProvider(context.Background(), db, 0)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err = provider.Lookup(ctx, "key")
	assert.ErrorIs(t, err, context.Canceled)
}

func TestCredentialTableConstraints(t *testing.T) {
	testutils.SkipIfIntegration(t)
	db := openTestDatabase(t)
	tests := map[string]struct {
		accessKeyID     string
		secretAccessKey string
		principalID     any
	}{
		"empty access key ID":         {accessKeyID: "", secretAccessKey: "secret"},
		"multibyte access key ID":     {accessKeyID: strings.Repeat("é", 65), secretAccessKey: "secret"},
		"multibyte secret access key": {accessKeyID: "key", secretAccessKey: strings.Repeat("é", 129)},
		"multibyte principal ID":      {accessKeyID: "key", secretAccessKey: "secret", principalID: strings.Repeat("é", 129)},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			err := database.WithTx(context.Background(), db, nil, func(ctx context.Context, tx database.Tx) error {
				_, err := tx.SqlTx().ExecContext(ctx, `INSERT INTO authentication_credentials (access_key_id, secret_access_key, principal_id) VALUES (?, ?, ?)`, tc.accessKeyID, tc.secretAccessKey, tc.principalID)
				return err
			})
			assert.Error(t, err)
		})
	}
}
