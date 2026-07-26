package tlsmanager

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-acme/lego/v5/challenge"
	"github.com/jdillenkofer/pithos/internal/settings"
	_ "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/mholt/acmez/v3/acme"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStaticManagerLoadsCertificateAndEnablesHTTP2(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)

	manager, err := newStaticManager(certFile, keyFile)

	require.NoError(t, err)
	assert.Equal(t, uint16(tls.VersionTLS12), manager.TLSConfig().MinVersion)
	assert.Contains(t, manager.TLSConfig().NextProtos, "h2")
	assert.Contains(t, manager.TLSConfig().NextProtos, "http/1.1")
	assert.Len(t, manager.TLSConfig().Certificates, 1)
}

func TestStaticManagerRejectsInvalidPair(t *testing.T) {
	_, err := newStaticManager(filepath.Join(t.TempDir(), "missing.pem"), filepath.Join(t.TempDir(), "missing.key"))
	assert.ErrorContains(t, err, "load static TLS certificate")
}

func TestStaticManagerServesHTTPAndHTTPSWithHTTP2(t *testing.T) {
	certFile, keyFile := writeTestCertificate(t)
	manager, err := newStaticManager(certFile, keyFile)
	require.NoError(t, err)

	var sawTLS atomic.Bool
	handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		sawTLS.Store(request.TLS != nil)
		writer.WriteHeader(http.StatusNoContent)
	})
	httpListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("local listeners unavailable: %v", err)
	}
	httpServer := &httptest.Server{
		Listener: httpListener,
		Config:   &http.Server{Handler: handler},
	}
	httpServer.Start()
	t.Cleanup(httpServer.Close)
	httpsListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("local listeners unavailable: %v", err)
	}
	httpsServer := &httptest.Server{
		Listener: httpsListener,
		Config:   &http.Server{Handler: handler},
	}
	httpsServer.EnableHTTP2 = true
	httpsServer.TLS = manager.TLSConfig()
	httpsServer.StartTLS()
	t.Cleanup(httpsServer.Close)

	response, err := httpsServer.Client().Get(httpsServer.URL)
	require.NoError(t, err)
	_ = response.Body.Close()
	assert.Equal(t, 2, response.ProtoMajor)
	assert.True(t, sawTLS.Load())

	response, err = http.Get(httpServer.URL)
	require.NoError(t, err)
	_ = response.Body.Close()
	assert.False(t, sawTLS.Load())
}

func TestACMEHTTPHandlerDelegatesOrdinaryTraffic(t *testing.T) {
	appSettings, err := settings.LoadSettings([]string{
		"-httpsEnabled=true",
		"-acmeEnabled=true",
		"-acmeDomains=example.com",
		"-acmeCacheDir=" + t.TempDir(),
		"-acmeChallenge=http-01",
	})
	require.NoError(t, err)
	require.NoError(t, appSettings.Validate())
	manager, err := New(appSettings)
	require.NoError(t, err)
	t.Cleanup(manager.Close)

	handler := manager.HTTPChallengeHandler(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.WriteHeader(http.StatusTeapot)
		_, _ = writer.Write([]byte("s3"))
	}))
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "http://example.com/object", nil))

	assert.Equal(t, http.StatusTeapot, recorder.Code)
	assert.Equal(t, "s3", recorder.Body.String())
	assert.Contains(t, manager.TLSConfig().NextProtos, "h2")
	assert.Contains(t, manager.TLSConfig().NextProtos, "acme-tls/1")
}

func TestACMEManagerUsesUsableCachedCertificateOnIssuanceFailure(t *testing.T) {
	appSettings, err := settings.LoadSettings([]string{
		"-httpsEnabled=true",
		"-acmeEnabled=true",
		"-acmeDomains=example.com",
		"-acmeCacheDir=" + t.TempDir(),
		"-acmeChallenge=http-01",
	})
	require.NoError(t, err)
	manager, err := newACMEManager(appSettings)
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	certFile, keyFile := writeTestCertificateFor(t, "example.com")
	_, err = manager.config.CacheUnmanagedCertificatePEMFile(context.Background(), certFile, keyFile, nil)
	require.NoError(t, err)
	manager.manage = func(context.Context, []string) error {
		return errors.New("issuer unavailable")
	}

	assert.NoError(t, manager.ManageSync(context.Background()))
}

func TestACMEManagerFailsIssuanceWithoutUsableCache(t *testing.T) {
	appSettings, err := settings.LoadSettings([]string{
		"-httpsEnabled=true",
		"-acmeEnabled=true",
		"-acmeDomains=example.com",
		"-acmeCacheDir=" + t.TempDir(),
		"-acmeChallenge=http-01",
	})
	require.NoError(t, err)
	manager, err := newACMEManager(appSettings)
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	manager.manage = func(context.Context, []string) error {
		return errors.New("issuer unavailable")
	}

	assert.ErrorContains(t, manager.ManageSync(context.Background()), "obtain or load ACME certificates")
}

func TestCuratedDNSProviderRegistry(t *testing.T) {
	for _, name := range []string{
		"cloudflare", "route53", "gcloud", "azuredns", "digitalocean",
		"hetzner", "dnsupdate", "rfc2136", "exec", "httpreq",
	} {
		assert.Contains(t, providerFactories, name)
	}
	_, err := newDNSProvider("unknown")
	assert.ErrorContains(t, err, "unsupported ACME DNS provider")
}

func TestCloudflareProviderRejectsMissingCredentials(t *testing.T) {
	for _, name := range []string{
		"CLOUDFLARE_EMAIL", "CLOUDFLARE_EMAIL_FILE",
		"CLOUDFLARE_API_KEY", "CLOUDFLARE_API_KEY_FILE",
		"CLOUDFLARE_DNS_API_TOKEN", "CLOUDFLARE_DNS_API_TOKEN_FILE",
		"CLOUDFLARE_ZONE_API_TOKEN", "CLOUDFLARE_ZONE_API_TOKEN_FILE",
		"CF_API_EMAIL", "CF_API_EMAIL_FILE",
		"CF_API_KEY", "CF_API_KEY_FILE",
		"CF_DNS_API_TOKEN", "CF_DNS_API_TOKEN_FILE",
		"CF_ZONE_API_TOKEN", "CF_ZONE_API_TOKEN_FILE",
	} {
		t.Setenv(name, "")
	}

	_, err := newDNSProvider("cloudflare")

	assert.ErrorContains(t, err, "credentials")
}

type fakeDNSProvider struct {
	present func(context.Context, string, string, string) error
	cleanup func(context.Context, string, string, string) error
	timeout time.Duration
	period  time.Duration
}

func (p *fakeDNSProvider) Present(ctx context.Context, domain, token, keyAuth string) error {
	if p.present != nil {
		return p.present(ctx, domain, token, keyAuth)
	}
	return nil
}

func (p *fakeDNSProvider) CleanUp(ctx context.Context, domain, token, keyAuth string) error {
	if p.cleanup != nil {
		return p.cleanup(ctx, domain, token, keyAuth)
	}
	return nil
}

func (p *fakeDNSProvider) Timeout() (time.Duration, time.Duration) {
	return p.timeout, p.period
}

var _ challenge.ProviderTimeout = (*fakeDNSProvider)(nil)

func TestDNSAdapterWaitsForRecursiveAndAuthoritativePropagation(t *testing.T) {
	adapter := NewDNSAdapter(&fakeDNSProvider{timeout: time.Second, period: time.Millisecond})
	var recursiveCalls atomic.Int32
	var authorityCalls atomic.Int32
	expectedChallenge := dnsChallenge()
	expected := expectedChallenge.DNS01KeyAuthorization()
	adapter.recursiveTXT = func(context.Context, string) ([]string, error) {
		if recursiveCalls.Add(1) < 2 {
			return nil, nil
		}
		return []string{expected}, nil
	}
	adapter.authorityTXT = func(context.Context, string) ([]string, error) {
		if authorityCalls.Add(1) < 3 {
			return nil, nil
		}
		return []string{expected}, nil
	}

	err := adapter.Wait(context.Background(), expectedChallenge)

	assert.NoError(t, err)
	assert.GreaterOrEqual(t, recursiveCalls.Load(), int32(3))
	assert.GreaterOrEqual(t, authorityCalls.Load(), int32(3))
}

func TestDNSAdapterPropagationTimeout(t *testing.T) {
	adapter := NewDNSAdapter(&fakeDNSProvider{timeout: 10 * time.Millisecond, period: time.Millisecond})
	adapter.recursiveTXT = func(context.Context, string) ([]string, error) { return nil, nil }
	adapter.authorityTXT = func(context.Context, string) ([]string, error) { return nil, nil }

	err := adapter.Wait(context.Background(), dnsChallenge())

	assert.ErrorContains(t, err, "wait for DNS TXT propagation")
}

func TestDNSAdapterSerializesProviderOperations(t *testing.T) {
	var active atomic.Int32
	var maximum atomic.Int32
	call := func(context.Context, string, string, string) error {
		current := active.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
		active.Add(-1)
		return nil
	}
	adapter := NewDNSAdapter(&fakeDNSProvider{present: call, cleanup: call})
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			require.NoError(t, adapter.Present(context.Background(), dnsChallenge()))
		}()
	}
	wait.Wait()

	assert.Equal(t, int32(1), maximum.Load())
	require.NoError(t, adapter.CleanUp(context.Background(), dnsChallenge()))
}

func TestDNSAdapterCleanupSurvivesCanceledChallengeContext(t *testing.T) {
	var cleanupContextErr error
	adapter := NewDNSAdapter(&fakeDNSProvider{
		cleanup: func(ctx context.Context, _, _, _ string) error {
			cleanupContextErr = ctx.Err()
			return nil
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, adapter.CleanUp(ctx, dnsChallenge()))
	assert.NoError(t, cleanupContextErr)
}

func dnsChallenge() acme.Challenge {
	return acme.Challenge{
		Token:            "token",
		KeyAuthorization: "token.key",
		Identifier:       acme.Identifier{Type: "dns", Value: "example.com"},
	}
}

func writeTestCertificate(t *testing.T) (string, string) {
	return writeTestCertificateFor(t, "localhost")
}

func writeTestCertificateFor(t *testing.T, domain string) (string, string) {
	t.Helper()
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: domain},
		DNSNames:     []string{domain},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	require.NoError(t, err)

	certFile := filepath.Join(t.TempDir(), "cert.pem")
	keyFile := filepath.Join(t.TempDir(), "key.pem")
	require.NoError(t, os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600))
	keyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
	require.NoError(t, os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyBytes}), 0o600))
	return certFile, keyFile
}
