package settings

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

var supportedDNSProviders = map[string]struct{}{
	"cloudflare":   {},
	"route53":      {},
	"gcloud":       {},
	"azuredns":     {},
	"digitalocean": {},
	"hetzner":      {},
	"dnsupdate":    {},
	"rfc2136":      {},
	"exec":         {},
	"httpreq":      {},
}

// Validate checks configuration combinations which must be known to be safe
// before storage is started or any network socket is bound.
func (s *Settings) Validate() error {
	if !s.HTTPEnabled() && !s.HTTPSEnabled() {
		return errors.New("at least one S3 listener must be enabled")
	}

	type listener struct {
		name    string
		enabled bool
		port    int
	}
	listeners := []listener{
		{"S3 HTTP", s.HTTPEnabled(), s.Port()},
		{"S3 HTTPS", s.HTTPSEnabled(), s.HTTPSPort()},
		{"monitoring HTTP", s.MonitoringPortEnabled(), s.MonitoringPort()},
		{"monitoring HTTPS", s.MonitoringHTTPSEnabled(), s.MonitoringHTTPSPort()},
	}
	addresses := make(map[string]string)
	for _, listener := range listeners {
		if listener.port < 0 || listener.port > 65535 {
			return fmt.Errorf("%s port must be in 0..65535", listener.name)
		}
		if !listener.enabled {
			continue
		}
		address := net.JoinHostPort(s.BindAddress(), fmt.Sprint(listener.port))
		// Port zero asks the kernel for a distinct ephemeral port per bind.
		if listener.port != 0 {
			if previous, ok := addresses[address]; ok {
				return fmt.Errorf("%s and %s resolve to duplicate address %s", previous, listener.name, address)
			}
			addresses[address] = listener.name
		}
	}

	httpsEnabled := s.HTTPSEnabled() || s.MonitoringHTTPSEnabled()
	certSet := s.TLSCertFile() != ""
	keySet := s.TLSKeyFile() != ""
	if certSet != keySet {
		return errors.New("static TLS requires both certificate and key files")
	}
	if s.ACMEEnabled() && certSet {
		return errors.New("static TLS files and ACME cannot be enabled together")
	}
	if httpsEnabled && !certSet && !s.ACMEEnabled() {
		return errors.New("an enabled HTTPS listener requires static TLS files or ACME")
	}
	if !httpsEnabled && (certSet || s.ACMEEnabled()) {
		return errors.New("a TLS certificate source requires an enabled HTTPS listener")
	}
	if !s.ACMEEnabled() {
		return nil
	}
	if len(s.ACMEDomains()) == 0 {
		return errors.New("ACME requires at least one domain")
	}
	if strings.TrimSpace(s.ACMECacheDir()) == "" {
		return errors.New("ACME requires a persistent cache directory")
	}
	if strings.TrimSpace(s.ACMECADirectoryURL()) == "" {
		return errors.New("ACME CA directory URL cannot be empty")
	}
	caURL, err := url.ParseRequestURI(s.ACMECADirectoryURL())
	if err != nil || (caURL.Scheme != "https" && caURL.Scheme != "http") || caURL.Host == "" {
		return errors.New("ACME CA directory URL must be an absolute HTTP(S) URL")
	}

	challenge := s.ACMEChallenge()
	switch challenge {
	case "auto", "http-01", "tls-alpn-01", "dns-01":
	default:
		return fmt.Errorf("unsupported ACME challenge %q", challenge)
	}
	httpEnabled := s.HTTPEnabled() || s.MonitoringPortEnabled()
	if challenge == "http-01" && !httpEnabled {
		return errors.New("ACME http-01 requires an enabled HTTP listener")
	}
	if challenge == "dns-01" {
		provider := s.ACMEDNSProvider()
		if provider == "" {
			return errors.New("ACME dns-01 requires a DNS provider")
		}
		if _, ok := supportedDNSProviders[provider]; !ok {
			return fmt.Errorf("unsupported ACME DNS provider %q", provider)
		}
	} else if s.ACMEDNSProvider() != "" {
		return errors.New("an ACME DNS provider can only be used with dns-01")
	}
	for _, domain := range s.ACMEDomains() {
		if strings.TrimSpace(domain) == "" {
			return errors.New("ACME domains cannot be empty")
		}
		if strings.HasPrefix(domain, "*.") && challenge != "dns-01" {
			return fmt.Errorf("wildcard domain %q requires dns-01", domain)
		}
	}
	return nil
}
