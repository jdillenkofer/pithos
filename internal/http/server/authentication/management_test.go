package authentication

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReadOnlyCredentialListsAreSafeAndIndependent(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	writeCredentialsFile(t, path, `{"credentials":[{"accessKeyId":"z-key","secretAccessKey":"never-list-secret","accountId":"account","principalId":"principal"},{"accessKeyId":"a-key","secretAccessKey":"never-list-secret","accountId":"account","principalId":"principal"}]}`)
	file, err := NewFileCredentialProvider(path, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	env := &EnvCredentialProvider{credentials: map[string]Credential{
		"z-key": {AccessKeyID: "z-key", SecretAccessKey: "never-list-secret", AccountID: "account", PrincipalID: "principal"},
		"a-key": {AccessKeyID: "a-key", SecretAccessKey: "never-list-secret", AccountID: "account", PrincipalID: "principal"},
	}}
	for _, provider := range []interface {
		CredentialProvider
		CredentialLister
	}{file, env} {
		_, writable := provider.(CredentialStore)
		require.False(t, writable)
		infos, err := provider.List(context.Background())
		require.NoError(t, err)
		require.Equal(t, []CredentialInfo{{AccessKeyID: "a-key", AccountID: "account", PrincipalID: "principal"}, {AccessKeyID: "z-key", AccountID: "account", PrincipalID: "principal"}}, infos)
		encoded, err := json.Marshal(infos)
		require.NoError(t, err)
		require.NotContains(t, string(encoded), "secret")
		infos[0].AccessKeyID = "mutated"
		again, err := provider.List(context.Background())
		require.NoError(t, err)
		require.Equal(t, "a-key", again[0].AccessKeyID)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err = provider.List(ctx)
		require.ErrorIs(t, err, context.Canceled)
	}
}

func TestNewAccountIDsArePathSafe(t *testing.T) {
	for _, account := range []string{"", "..", "../account", "a/b", `a\b`, "a b", "a\x00b"} {
		require.Error(t, ValidateCredentialAccountID(account), account)
	}
	require.NoError(t, ValidateCredentialAccountID("account_123-ABC"))
}
