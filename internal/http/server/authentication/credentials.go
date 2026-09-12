package authentication

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const credentialEnvPrefix = "PITHOS_CREDENTIALS_"

const (
	MaxAccessKeyIDLength     = 128
	MaxSecretAccessKeyLength = 256
	MaxPrincipalIDLength     = 256
)

type Credential struct {
	AccessKeyID     string `json:"accessKeyId"`
	SecretAccessKey string `json:"secretAccessKey"`
	PrincipalID     string `json:"principalId,omitempty"`
}

type CredentialProvider interface {
	Lookup(ctx context.Context, accessKeyID string) (Credential, bool, error)
}

const maxCredentialsFileSize = 1024 * 1024

type credentialsFile struct {
	Credentials *[]Credential `json:"credentials"`
}

type credentialSnapshot struct {
	credentials map[string]Credential
	digest      [sha256.Size]byte
}

// FileCredentialProvider loads credentials from a JSON file and periodically
// checks it for changes during lookups. A new snapshot becomes visible only
// after the complete file has been read and validated successfully.
type FileCredentialProvider struct {
	path           string
	reloadInterval time.Duration
	lastCheck      time.Time
	reloadMu       sync.Mutex
	snapshot       atomic.Pointer[credentialSnapshot]
}

func NewFileCredentialProvider(path string, reloadInterval time.Duration) (*FileCredentialProvider, error) {
	if path == "" {
		return nil, fmt.Errorf("credentials path must not be empty")
	}
	if reloadInterval < 0 {
		return nil, fmt.Errorf("credentials reload interval must not be negative")
	}

	provider := &FileCredentialProvider{path: path, reloadInterval: reloadInterval}
	snapshot, err := loadCredentialSnapshot(path)
	if err != nil {
		return nil, err
	}
	provider.snapshot.Store(snapshot)
	provider.lastCheck = time.Now()
	return provider, nil
}

func loadCredentialSnapshot(path string) (*credentialSnapshot, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open credentials file: %w", err)
	}
	defer file.Close()

	contents, err := io.ReadAll(io.LimitReader(file, maxCredentialsFileSize+1))
	if err != nil {
		return nil, fmt.Errorf("read credentials file: %w", err)
	}
	if len(contents) > maxCredentialsFileSize {
		return nil, fmt.Errorf("credentials file exceeds maximum size of %d bytes", maxCredentialsFileSize)
	}

	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var document credentialsFile
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("decode credentials file: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, fmt.Errorf("decode credentials file: %w", err)
	}
	if document.Credentials == nil {
		return nil, fmt.Errorf("decode credentials file: credentials must be an array")
	}

	credentials := make(map[string]Credential, len(*document.Credentials))
	for index, credential := range *document.Credentials {
		if err := validateCredential(credential); err != nil {
			return nil, fmt.Errorf("credential %d: %w", index, err)
		}
		if _, exists := credentials[credential.AccessKeyID]; exists {
			return nil, fmt.Errorf("credential %d: duplicate access key ID", index)
		}
		credentials[credential.AccessKeyID] = credential
	}

	return &credentialSnapshot{credentials: credentials, digest: sha256.Sum256(contents)}, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func (p *FileCredentialProvider) reloadIfDue() {
	p.reloadMu.Lock()
	defer p.reloadMu.Unlock()

	if p.reloadInterval > 0 && time.Since(p.lastCheck) < p.reloadInterval {
		return
	}
	p.lastCheck = time.Now()

	next, err := loadCredentialSnapshot(p.path)
	if err != nil {
		slog.Error("Failed to reload credentials file; retaining last-known-good credentials", "path", p.path, "error", err)
		return
	}
	current := p.snapshot.Load()
	if current != nil && current.digest == next.digest {
		return
	}
	p.snapshot.Store(next)
	slog.Info("Reloaded credentials file", "path", p.path, "credentialCount", len(next.credentials))
}

func (p *FileCredentialProvider) Lookup(ctx context.Context, accessKeyID string) (Credential, bool, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, false, err
	}
	p.reloadIfDue()
	snapshot := p.snapshot.Load()
	credential, found := snapshot.credentials[accessKeyID]
	return credential, found, nil
}

func ValidateCredential(credential Credential) error {
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

func validateCredential(credential Credential) error {
	return ValidateCredential(credential)
}

// EnvCredentialProvider resolves credentials from an immutable snapshot of the
// process environment captured when the provider is created. Environment
// credential changes require restarting Pithos.
type EnvCredentialProvider struct {
	credentials map[string]Credential
}

func NewEnvCredentialProvider() *EnvCredentialProvider {
	credentials := make(map[string]Credential)
	for i := 0; ; i++ {
		prefix := credentialEnvPrefix + strconv.Itoa(i)
		accessKeyID := os.Getenv(prefix + "_ACCESS_KEY_ID")
		secretAccessKey := os.Getenv(prefix + "_SECRET_ACCESS_KEY")
		principalID := os.Getenv(prefix + "_PRINCIPAL_ID")
		if accessKeyID == "" || secretAccessKey == "" {
			// Preserve compatibility with configurations whose first index is 1.
			if i == 0 {
				continue
			}
			break
		}
		// Preserve the previous lookup behavior when an access key is listed
		// more than once: the lowest configured index wins.
		if _, exists := credentials[accessKeyID]; !exists {
			credentials[accessKeyID] = Credential{
				AccessKeyID:     accessKeyID,
				SecretAccessKey: secretAccessKey,
				PrincipalID:     principalID,
			}
		}
	}
	return &EnvCredentialProvider{credentials: credentials}
}

func (p *EnvCredentialProvider) Lookup(ctx context.Context, accessKeyID string) (Credential, bool, error) {
	if err := ctx.Err(); err != nil {
		return Credential{}, false, err
	}
	credential, found := p.credentials[accessKeyID]
	return credential, found, nil
}
