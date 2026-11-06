package authentication

import (
	"context"
	"errors"
	"sort"
)

// CredentialInfo is public credential metadata. It deliberately cannot carry a secret.
type CredentialInfo struct {
	AccessKeyID string `json:"accessKeyId"`
	AccountID   string `json:"accountId"`
	PrincipalID string `json:"principalId"`
}

// CredentialLister is optional; lookup-only providers remain supported.
type CredentialLister interface {
	List(ctx context.Context) ([]CredentialInfo, error)
}

// CredentialStore is implemented only by mutable credential providers.
// Successful mutations are immediately visible to Lookup and List.
type CredentialStore interface {
	Create(ctx context.Context, credential Credential) error
	Delete(ctx context.Context, accessKeyID string) error
}

var (
	ErrCredentialExists   = errors.New("credential already exists")
	ErrCredentialNotFound = errors.New("credential not found")
	ErrCredentialMutation = errors.New("unable to update credentials")
)

// ValidateCredentialAccountID restricts new account IDs to path-safe identifiers.
// Existing configured account IDs are not reinterpreted or changed.
func ValidateCredentialAccountID(accountID string) error {
	if len(accountID) == 0 || len(accountID) > MaxAccountIDLength {
		return errors.New("account ID must contain 1 to 256 letters, digits, underscores or hyphens")
	}
	for _, c := range accountID {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			return errors.New("account ID must contain only letters, digits, underscores or hyphens")
		}
	}
	return nil
}

// CredentialMetadata returns a fresh, deterministic metadata-only view.
func CredentialMetadata(credentials map[string]Credential) []CredentialInfo {
	result := make([]CredentialInfo, 0, len(credentials))
	for _, c := range credentials {
		result = append(result, CredentialInfo{AccessKeyID: c.AccessKeyID, AccountID: c.AccountID, PrincipalID: c.PrincipalID})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].AccessKeyID < result[j].AccessKeyID })
	return result
}

func (p *FileCredentialProvider) List(ctx context.Context) ([]CredentialInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return CredentialMetadata(p.coordinator.Snapshot().credentials), nil
}

func (p *EnvCredentialProvider) List(ctx context.Context) ([]CredentialInfo, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return CredentialMetadata(p.credentials), nil
}
