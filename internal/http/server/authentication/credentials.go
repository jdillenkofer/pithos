package authentication

import (
	"context"
	"fmt"
	"os"
	"strconv"
)

const credentialEnvPrefix = "PITHOS_CREDENTIALS_"

const (
	MaxAccessKeyIDLength     = 128
	MaxSecretAccessKeyLength = 256
	MaxPrincipalIDLength     = 256
)

type Credential struct {
	AccessKeyID     string
	SecretAccessKey string
	PrincipalID     string
}

type AuthenticatedIdentity struct {
	AccessKeyID string
	PrincipalID string
}

type CredentialProvider interface {
	Lookup(ctx context.Context, accessKeyID string) (Credential, bool, error)
}

func validateCredential(credential Credential) error {
	if len(credential.AccessKeyID) == 0 {
		return fmt.Errorf("access key ID must not be empty")
	}
	if len(credential.AccessKeyID) > MaxAccessKeyIDLength {
		return fmt.Errorf("access key ID exceeds maximum length of %d bytes", MaxAccessKeyIDLength)
	}
	if len(credential.SecretAccessKey) == 0 {
		return fmt.Errorf("secret access key must not be empty")
	}
	if len(credential.SecretAccessKey) > MaxSecretAccessKeyLength {
		return fmt.Errorf("secret access key exceeds maximum length of %d bytes", MaxSecretAccessKeyLength)
	}
	if len(credential.PrincipalID) > MaxPrincipalIDLength {
		return fmt.Errorf("principal ID exceeds maximum length of %d bytes", MaxPrincipalIDLength)
	}
	return nil
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
		principalID := os.Getenv(prefix + "_PRINCIPAL_ID")
		if configuredAccessKeyID == "" || secretAccessKey == "" {
			// Preserve compatibility with configurations whose first index is 1.
			if i == 0 {
				continue
			}
			return Credential{}, false, nil
		}
		if configuredAccessKeyID == accessKeyID {
			return Credential{AccessKeyID: configuredAccessKeyID, SecretAccessKey: secretAccessKey, PrincipalID: principalID}, true, nil
		}
	}
}
