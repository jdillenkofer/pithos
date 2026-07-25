package gdrive

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestProactiveTokenSourceRefreshesAccessToken(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"refreshed-access","token_type":"Bearer","expires_in":3600,"refresh_token":"refresh-token"}`))
	}))
	defer server.Close()

	cfg := &oauth2.Config{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Endpoint: oauth2.Endpoint{
			TokenURL: server.URL,
		},
	}

	initialToken := &oauth2.Token{
		AccessToken:  "old-access",
		RefreshToken: "refresh-token",
		Expiry:       time.Now().Add(1 * time.Hour),
	}

	source := NewProactiveTokenSource(cfg, initialToken, time.Hour, nil)
	refreshed, err := source.Token()
	require.NoError(t, err)
	assert.Equal(t, "refreshed-access", refreshed.AccessToken)
	assert.EqualValues(t, 1, requests.Load())
}

func TestProactiveTokenSourceRefreshesTokenWithoutExpiry(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"refreshed-access","token_type":"Bearer","expires_in":3600,"refresh_token":"refresh-token"}`))
	}))
	defer server.Close()

	cfg := &oauth2.Config{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Endpoint: oauth2.Endpoint{
			TokenURL: server.URL,
		},
	}

	initialToken := &oauth2.Token{
		AccessToken:  "old-access",
		RefreshToken: "refresh-token",
	}

	source := NewProactiveTokenSource(cfg, initialToken, time.Hour, nil)
	refreshed, err := source.Token()
	require.NoError(t, err)
	assert.Equal(t, "refreshed-access", refreshed.AccessToken)
	assert.EqualValues(t, 1, requests.Load())
}

func TestProactiveTokenSourcePersistenceFailureDoesNotFailRefresh(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"refreshed-access","token_type":"Bearer","expires_in":3600,"refresh_token":"refresh-token"}`))
	}))
	defer server.Close()

	cfg := &oauth2.Config{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Endpoint: oauth2.Endpoint{
			TokenURL: server.URL,
		},
	}
	initialToken := &oauth2.Token{
		AccessToken:  "expired-access",
		RefreshToken: "refresh-token",
		Expiry:       time.Now().Add(-time.Minute),
	}

	source := NewProactiveTokenSource(cfg, initialToken, time.Minute, func(*oauth2.Token) error {
		return errors.New("provider is read-only")
	})
	refreshed, err := source.Token()

	require.NoError(t, err)
	assert.Equal(t, "refreshed-access", refreshed.AccessToken)
}

func TestProactiveTokenSourceDoesNotRefreshInBackground(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"refreshed-access","token_type":"Bearer","expires_in":3600,"refresh_token":"refresh-token"}`))
	}))
	defer server.Close()

	cfg := &oauth2.Config{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Endpoint: oauth2.Endpoint{
			TokenURL: server.URL,
		},
	}
	initialToken := &oauth2.Token{
		AccessToken:  "old-access",
		RefreshToken: "refresh-token",
		Expiry:       time.Now().Add(time.Minute),
	}

	source := NewProactiveTokenSource(cfg, initialToken, time.Hour, nil)
	time.Sleep(25 * time.Millisecond)
	assert.Zero(t, requests.Load())

	_, err := source.Token()
	require.NoError(t, err)
	assert.EqualValues(t, 1, requests.Load())
}

func TestProactiveTokenSourceBoundsRefreshDuration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(200 * time.Millisecond):
		}
	}))
	defer server.Close()

	cfg := &oauth2.Config{
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		Endpoint:     oauth2.Endpoint{TokenURL: server.URL},
	}
	initialToken := &oauth2.Token{
		AccessToken:  "expired-access",
		RefreshToken: "refresh-token",
		Expiry:       time.Now().Add(-time.Minute),
	}

	source := newProactiveTokenSource(cfg, initialToken, time.Minute, nil, 20*time.Millisecond)
	startedAt := time.Now()
	_, err := source.Token()

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(startedAt), time.Second)
}
