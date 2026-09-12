package authentication

import (
	"context"
	"os"
	"strconv"
)

const credentialEnvPrefix = "PITHOS_CREDENTIALS_"

type Credential struct {
	AccessKeyID     string
	SecretAccessKey string
}

type CredentialProvider interface {
	Lookup(ctx context.Context, accessKeyID string) (Credential, bool, error)
}

// EnvCredentialProvider resolves credentials from the process environment on
// every lookup, allowing credentials to be changed without restarting Pithos.
type EnvCredentialProvider struct{}

func NewEnvCredentialProvider() *EnvCredentialProvider {
	return &EnvCredentialProvider{}
}

func (p *EnvCredentialProvider) Lookup(ctx context.Context, accessKeyID string) (Credential, bool, error) {
	for i := 0; ; i++ {
		if err := ctx.Err(); err != nil {
			return Credential{}, false, err
		}

		prefix := credentialEnvPrefix + strconv.Itoa(i)
		configuredAccessKeyID := os.Getenv(prefix + "_ACCESS_KEY_ID")
		secretAccessKey := os.Getenv(prefix + "_SECRET_ACCESS_KEY")
		if configuredAccessKeyID == "" || secretAccessKey == "" {
			// Preserve compatibility with configurations whose first index is 1.
			if i == 0 {
				continue
			}
			return Credential{}, false, nil
		}
		if configuredAccessKeyID == accessKeyID {
			return Credential{AccessKeyID: configuredAccessKeyID, SecretAccessKey: secretAccessKey}, true, nil
		}
	}
}
