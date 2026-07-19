// Package auth implements the one-time OAuth device flow used to obtain the
// Google Drive token for the GoogleDrivePartStore. The flow prints a
// verification URL and a short user code, so it works on headless servers:
// the consent step can be completed in a browser on any other device.
package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"

	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/gdrive"
)

func newOauthConfig(clientId string, clientSecret string) *oauth2.Config {
	return &oauth2.Config{
		ClientID:     clientId,
		ClientSecret: clientSecret,
		Endpoint:     google.Endpoint,
		Scopes:       []string{gdrive.Scope},
	}
}

// RunDeviceFlow requests a device code, prints the consent instructions to
// out, and polls until the user has approved (or ctx expires). The returned
// token contains the long-lived refresh token the part store needs.
//
// The OAuth client must be of type "TVs and Limited Input devices" in the
// Google Cloud Console, otherwise Google rejects the device code request.
func RunDeviceFlow(ctx context.Context, clientId string, clientSecret string, out io.Writer) (*oauth2.Token, error) {
	oauthConfig := newOauthConfig(clientId, clientSecret)
	deviceAuthResponse, err := oauthConfig.DeviceAuth(ctx)
	if err != nil {
		return nil, fmt.Errorf("device authorization request failed: %w", err)
	}

	verificationURI := deviceAuthResponse.VerificationURIComplete
	if verificationURI != "" {
		fmt.Fprintf(out, "Open %s on any device to grant pithos access to Google Drive.\n", verificationURI)
		fmt.Fprintf(out, "If the code is not prefilled, enter: %s\n", deviceAuthResponse.UserCode)
	} else {
		fmt.Fprintf(out, "Open %s on any device and enter the code: %s\n", deviceAuthResponse.VerificationURI, deviceAuthResponse.UserCode)
	}
	fmt.Fprintf(out, "Waiting for approval (expires %s)...\n", deviceAuthResponse.Expiry.Format("15:04:05"))

	token, err := oauthConfig.DeviceAccessToken(ctx, deviceAuthResponse)
	if err != nil {
		return nil, fmt.Errorf("waiting for device authorization failed: %w", err)
	}
	if token.RefreshToken == "" {
		return nil, fmt.Errorf("google returned no refresh token; the token would expire within the hour")
	}
	return token, nil
}

// FormatToken renders the token as the JSON expected by the "token" field of
// the GoogleDrivePartStore configuration.
func FormatToken(token *oauth2.Token) (string, error) {
	tokenJson, err := json.Marshal(token)
	if err != nil {
		return "", err
	}
	return string(tokenJson), nil
}
