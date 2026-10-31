package tlsmanager

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/caddyserver/certmagic"
	"github.com/jdillenkofer/pithos/internal/settings"
)

// Manager owns the certificate source shared by every HTTPS listener.
type Manager interface {
	TLSConfig() *tls.Config
	HTTPChallengeHandler(http.Handler) http.Handler
	ManageSync(context.Context) error
	Close()
}

func New(appSettings *settings.Settings) (Manager, error) {
	if !appSettings.HTTPSEnabled() && !appSettings.MonitoringHTTPSEnabled() {
		return nil, nil
	}
	if appSettings.ACMEEnabled() {
		return newACMEManager(appSettings)
	}
	return newStaticManager(appSettings.TLSCertFile(), appSettings.TLSKeyFile())
}

type staticManager struct {
	tlsConfig *tls.Config
}

func newStaticManager(certFile, keyFile string) (*staticManager, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load static TLS certificate: %w", err)
	}
	return &staticManager{tlsConfig: &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
		NextProtos:   []string{"h2", "http/1.1"},
	}}, nil
}

func (m *staticManager) TLSConfig() *tls.Config { return m.tlsConfig }

func (m *staticManager) HTTPChallengeHandler(next http.Handler) http.Handler { return next }

func (m *staticManager) ManageSync(context.Context) error { return nil }

func (m *staticManager) Close() {}

type acmeManager struct {
	cache     *certmagic.Cache
	config    *certmagic.Config
	issuer    *certmagic.ACMEIssuer
	tlsConfig *tls.Config
	domains   []string
	manage    func(context.Context, []string) error
	close     sync.Once
}

func newACMEManager(appSettings *settings.Settings) (*acmeManager, error) {
	if err := prepareCacheDirectory(appSettings.ACMECacheDir()); err != nil {
		return nil, err
	}

	logger := newZapLogger(slog.Default())
	var configReference atomic.Pointer[certmagic.Config]
	cache := certmagic.NewCache(certmagic.CacheOptions{
		GetConfigForCert: func(certmagic.Certificate) (*certmagic.Config, error) {
			config := configReference.Load()
			if config == nil {
				return nil, errors.New("ACME certificate manager is still initializing")
			}
			return config, nil
		},
		Logger: logger,
	})
	config := certmagic.New(cache, certmagic.Config{
		Storage: &certmagic.FileStorage{Path: appSettings.ACMECacheDir()},
		Logger:  logger,
	})
	configReference.Store(config)

	issuerTemplate := certmagic.ACMEIssuer{
		CA:                      appSettings.ACMECADirectoryURL(),
		Email:                   appSettings.ACMEEmail(),
		Agreed:                  true,
		DisableHTTPChallenge:    false,
		DisableTLSALPNChallenge: false,
		AltHTTPPort:             firstHTTPPort(appSettings),
		AltTLSALPNPort:          firstHTTPSPort(appSettings),
		Logger:                  logger,
	}
	switch appSettings.ACMEChallenge() {
	case "http-01":
		issuerTemplate.DisableTLSALPNChallenge = true
	case "tls-alpn-01":
		issuerTemplate.DisableHTTPChallenge = true
	case "dns-01":
		provider, err := newDNSProvider(appSettings.ACMEDNSProvider())
		if err != nil {
			cache.Stop()
			return nil, err
		}
		issuerTemplate.DisableHTTPChallenge = true
		issuerTemplate.DisableTLSALPNChallenge = true
		issuerTemplate.DNS01Solver = NewDNSAdapter(provider)
	case "auto":
		issuerTemplate.DisableHTTPChallenge = !anyHTTPEnabled(appSettings)
		issuerTemplate.DisableTLSALPNChallenge = !anyHTTPSEnabled(appSettings)
	}
	issuer := certmagic.NewACMEIssuer(config, issuerTemplate)
	config.Issuers = []certmagic.Issuer{issuer}

	tlsConfig := config.TLSConfig()
	tlsConfig.MinVersion = tls.VersionTLS12
	for _, protocol := range []string{"http/1.1", "h2"} {
		if !slices.Contains(tlsConfig.NextProtos, protocol) {
			tlsConfig.NextProtos = append([]string{protocol}, tlsConfig.NextProtos...)
		}
	}

	return &acmeManager{
		cache:     cache,
		config:    config,
		issuer:    issuer,
		tlsConfig: tlsConfig,
		domains:   append([]string(nil), appSettings.ACMEDomains()...),
		manage:    config.ManageSync,
	}, nil
}

func (m *acmeManager) TLSConfig() *tls.Config {
	return m.tlsConfig
}

func (m *acmeManager) HTTPChallengeHandler(next http.Handler) http.Handler {
	return m.issuer.HTTPChallengeHandler(next)
}

func (m *acmeManager) ManageSync(ctx context.Context) error {
	if err := m.manage(ctx, m.domains); err != nil {
		if m.hasUsableCachedCertificates() {
			slog.Warn("ACME renewal failed; continuing with usable cached certificates", "err", err)
			return nil
		}
		return fmt.Errorf("obtain or load ACME certificates: %w", err)
	}
	return nil
}

func (m *acmeManager) hasUsableCachedCertificates() bool {
	for _, domain := range m.domains {
		certificates := m.cache.AllMatchingCertificates(domain)
		usable := false
		for _, certificate := range certificates {
			if !certificate.Empty() && !certificate.Expired() {
				usable = true
				break
			}
		}
		if !usable {
			return false
		}
	}
	return true
}

func (m *acmeManager) Close() {
	m.close.Do(m.cache.Stop)
}

func prepareCacheDirectory(path string) error {
	if err := os.MkdirAll(path, 0o700); err != nil {
		return fmt.Errorf("create ACME cache directory: %w", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat ACME cache directory: %w", err)
	}
	if !info.IsDir() {
		return fmt.Errorf("ACME cache path %q is not a directory", path)
	}
	file, err := os.CreateTemp(path, ".pithos-write-test-*")
	if err != nil {
		return fmt.Errorf("ACME cache directory is not writable: %w", err)
	}
	name := file.Name()
	closeErr := file.Close()
	removeErr := os.Remove(name)
	if closeErr != nil || removeErr != nil {
		return fmt.Errorf("verify ACME cache directory: %w", errors.Join(closeErr, removeErr))
	}
	return nil
}

func firstHTTPPort(appSettings *settings.Settings) int {
	if appSettings.HTTPEnabled() {
		return appSettings.Port()
	}
	if appSettings.MonitoringPortEnabled() {
		return appSettings.MonitoringPort()
	}
	return 0
}

func firstHTTPSPort(appSettings *settings.Settings) int {
	if appSettings.HTTPSEnabled() {
		return appSettings.HTTPSPort()
	}
	if appSettings.MonitoringHTTPSEnabled() {
		return appSettings.MonitoringHTTPSPort()
	}
	return 0
}

func anyHTTPEnabled(appSettings *settings.Settings) bool {
	return appSettings.HTTPEnabled() || appSettings.MonitoringPortEnabled()
}

func anyHTTPSEnabled(appSettings *settings.Settings) bool {
	return appSettings.HTTPSEnabled() || appSettings.MonitoringHTTPSEnabled()
}
