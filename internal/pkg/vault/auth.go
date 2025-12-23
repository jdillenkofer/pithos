package vault

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// AuthResponse represents the response from Vault authentication
type AuthResponse struct {
	Auth struct {
		ClientToken   string `json:"client_token"`
		LeaseDuration int    `json:"lease_duration"`
		Renewable     bool   `json:"renewable"`
	} `json:"auth"`
}

// AuthenticateWithAppRole authenticates with Vault using AppRole and returns the client token and expiry
func AuthenticateWithAppRole(vaultAddr, roleID, secretID string, tlsConfig *tls.Config) (string, time.Time, error) {
	// Convert vault address to proper HTTP URL
	httpAddr := vaultAddr
	if strings.HasPrefix(vaultAddr, "hcvault://") {
		httpAddr = "https://" + vaultAddr[len("hcvault://"):]
	} else if !strings.HasPrefix(vaultAddr, "http://") && !strings.HasPrefix(vaultAddr, "https://") {
		httpAddr = "https://" + vaultAddr
	}

	// Prepare the AppRole login request
	loginData := map[string]string{
		"role_id":   roleID,
		"secret_id": secretID,
	}
	loginJSON, err := json.Marshal(loginData)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to marshal AppRole login data: %w", err)
	}

	// Configure HTTP client
	transport := &http.Transport{
		TLSClientConfig: tlsConfig,
	}
	client := &http.Client{Transport: transport}

	// Make the login request
	loginURL := strings.TrimRight(httpAddr, "/") + "/v1/auth/approle/login"
	resp, err := client.Post(loginURL, "application/json", bytes.NewReader(loginJSON))
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to authenticate with AppRole: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return "", time.Time{}, fmt.Errorf("AppRole authentication failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Parse the response
	var authResp AuthResponse
	if err := json.NewDecoder(resp.Body).Decode(&authResp); err != nil {
		return "", time.Time{}, fmt.Errorf("failed to decode AppRole response: %w", err)
	}

	if authResp.Auth.ClientToken == "" {
		return "", time.Time{}, fmt.Errorf("AppRole authentication returned empty token")
	}

	// Calculate token expiry (refresh at 80% of lease duration to be safe)
	leaseDuration := time.Duration(authResp.Auth.LeaseDuration) * time.Second
	refreshBuffer := leaseDuration * 20 / 100 // 20% buffer
	expiry := time.Now().Add(leaseDuration - refreshBuffer)

	return authResp.Auth.ClientToken, expiry, nil
}
