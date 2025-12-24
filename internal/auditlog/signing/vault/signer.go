package vault

import (
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/vault/api"
	"github.com/jdillenkofer/pithos/internal/pkg/vault"
)

// Signer implements the signing.Signer interface using HashiCorp Vault's Transit secret engine.
type Signer struct {
	client        *api.Client
	keyName       string
	mountPath     string
	vaultAddr     string
	vaultRoleID   string
	vaultSecretID string
	tlsConfig     *tls.Config
	tokenExpiry   time.Time
	tokenMutex    sync.RWMutex
	stopRefresh   chan struct{}
	refreshDone   chan struct{}
}

// NewSigner creates a new Vault Signer.
// vaultAddr: e.g. "https://vault.example.com:8200"
// token: Vault token (use empty string "" if using AppRole)
// roleID: Vault AppRole role ID (use empty string "" if using token)
// secretID: Vault AppRole secret ID (use empty string "" if using token)
// keyPath: path to the key in Transit engine (e.g. "transit/keys/my-key")
func NewSigner(vaultAddr, token, roleID, secretID, keyPath string, tlsConfig *tls.Config) (*Signer, error) {
	lastSlash := strings.LastIndex(keyPath, "/")
	if lastSlash == -1 {
		return nil, fmt.Errorf("invalid keyPath: %s. Expected format: mount/keyname", keyPath)
	}
	mountPath := keyPath[:lastSlash]
	keyName := keyPath[lastSlash+1:]

	config := api.DefaultConfig()
	config.Address = vaultAddr
	if tlsConfig != nil {
		config.HttpClient.Transport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}
	}

	client, err := api.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vault client: %w", err)
	}

	// Authenticate
	var actualToken string
	var tokenExpiry time.Time

	hasToken := token != ""
	hasAppRole := roleID != "" && secretID != ""

	if !hasToken && !hasAppRole {
		return nil, fmt.Errorf("either vaultToken or (vaultRoleId and vaultSecretId) must be provided")
	}

	if hasToken && hasAppRole {
		return nil, fmt.Errorf("cannot use both vaultToken and AppRole authentication")
	}

	if hasAppRole {
		actualToken, tokenExpiry, err = vault.AuthenticateWithAppRole(vaultAddr, roleID, secretID, tlsConfig)
		if err != nil {
			return nil, fmt.Errorf("AppRole authentication failed: %w", err)
		}
	} else {
		actualToken = token
		tokenExpiry = time.Now().Add(100 * 365 * 24 * time.Hour)
	}

	client.SetToken(actualToken)

	signer := &Signer{
		client:        client,
		keyName:       keyName,
		mountPath:     mountPath,
		vaultAddr:     vaultAddr,
		vaultRoleID:   roleID,
		vaultSecretID: secretID,
		tlsConfig:     tlsConfig,
		tokenExpiry:   tokenExpiry,
		stopRefresh:   make(chan struct{}),
		refreshDone:   make(chan struct{}),
	}

	if hasAppRole {
		go signer.startTokenRefreshLoop()
	}

	return signer, nil
}

func (s *Signer) Sign(data []byte) ([]byte, error) {
	path := fmt.Sprintf("%s/sign/%s", s.mountPath, s.keyName)
	input := base64.StdEncoding.EncodeToString(data)

	secret, err := s.client.Logical().Write(path, map[string]interface{}{
		"input": input,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to sign data in Vault: %w", err)
	}

	if secret == nil || secret.Data == nil {
		return nil, fmt.Errorf("empty response from Vault sign operation")
	}

	signature, ok := secret.Data["signature"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid signature format in Vault response")
	}

	// Vault returns "vault:v1:signature_base64"
	// We might want to return just the raw signature bytes, or keep the format.
	// The interface returns []byte.
	// Usually for Ed25519, we want the raw 64 bytes signature.
	// But Vault returns it in a specific format.
	// Let's decode it if it starts with "vault:v1:".
	
	if strings.HasPrefix(signature, "vault:v") {
		parts := strings.Split(signature, ":")
		if len(parts) == 3 {
			// version := parts[1]
			sigBase64 := parts[2]
			sigBytes, err := base64.StdEncoding.DecodeString(sigBase64)
			if err != nil {
				return nil, fmt.Errorf("failed to decode signature: %w", err)
			}
			return sigBytes, nil
		}
	}

	return []byte(signature), nil
}

func (s *Signer) Close() {
	if s.vaultRoleID != "" && s.vaultSecretID != "" {
		close(s.stopRefresh)
		<-s.refreshDone
	}
}

func (s *Signer) startTokenRefreshLoop() {
	defer close(s.refreshDone)
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopRefresh:
			return
		case <-ticker.C:
			s.tokenMutex.RLock()
			needsRefresh := time.Now().After(s.tokenExpiry)
			s.tokenMutex.RUnlock()

			if needsRefresh {
				if err := s.refreshToken(); err != nil {
					fmt.Printf("Warning: failed to refresh Vault token for signer: %v\n", err)
				}
			}
		}
	}
}

func (s *Signer) refreshToken() error {
	s.tokenMutex.Lock()
	defer s.tokenMutex.Unlock()

	newToken, newExpiry, err := vault.AuthenticateWithAppRole(s.vaultAddr, s.vaultRoleID, s.vaultSecretID, s.tlsConfig)
	if err != nil {
		return err
	}

	s.client.SetToken(newToken)
	s.tokenExpiry = newExpiry
	return nil
}
