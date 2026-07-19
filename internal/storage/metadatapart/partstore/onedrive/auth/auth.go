package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/onedrive"
	"golang.org/x/oauth2"
)

func OAuthConfig(tenantID, clientID string) *oauth2.Config {
	if tenantID == "" {
		tenantID = "consumers"
	}
	base := "https://login.microsoftonline.com/" + tenantID + "/oauth2/v2.0"
	return &oauth2.Config{ClientID: clientID, Scopes: []string{onedrive.Scope, "offline_access"}, Endpoint: oauth2.Endpoint{AuthURL: base + "/authorize", TokenURL: base + "/token", DeviceAuthURL: base + "/devicecode", AuthStyle: oauth2.AuthStyleInParams}}
}

func RunDeviceFlow(ctx context.Context, tenantID, clientID string, out io.Writer) (*oauth2.Token, error) {
	cfg := OAuthConfig(tenantID, clientID)
	d, e := cfg.DeviceAuth(ctx)
	if e != nil {
		return nil, fmt.Errorf("device authorization request failed: %w", e)
	}
	verification := d.VerificationURIComplete
	if verification == "" {
		verification = d.VerificationURI
	}
	fmt.Fprintf(out, "Open %s and enter code %s to grant pithos access to OneDrive.\n", verification, d.UserCode)
	t, e := cfg.DeviceAccessToken(ctx, d)
	if e != nil {
		return nil, fmt.Errorf("waiting for device authorization failed: %w", e)
	}
	if t.RefreshToken == "" {
		return nil, fmt.Errorf("Microsoft returned no refresh token")
	}
	return t, nil
}
func FormatToken(t *oauth2.Token) (string, error) { b, e := json.Marshal(t); return string(b), e }
