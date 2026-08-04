package tlsmanager

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/go-acme/lego/v5/challenge"
	legolog "github.com/go-acme/lego/v5/log"
	"github.com/go-acme/lego/v5/providers/dns/azuredns"
	"github.com/go-acme/lego/v5/providers/dns/cloudflare"
	"github.com/go-acme/lego/v5/providers/dns/digitalocean"
	"github.com/go-acme/lego/v5/providers/dns/dnsupdate"
	"github.com/go-acme/lego/v5/providers/dns/exec"
	"github.com/go-acme/lego/v5/providers/dns/gcloud"
	"github.com/go-acme/lego/v5/providers/dns/hetzner"
	"github.com/go-acme/lego/v5/providers/dns/httpreq"
	"github.com/go-acme/lego/v5/providers/dns/route53"
)

type providerFactory func() (challenge.Provider, error)

// This explicit registry is deliberately curated. Importing lego's generated
// all-provider factory would pull platform-specific providers into every build.
var providerFactories = map[string]providerFactory{
	"azuredns": func() (challenge.Provider, error) {
		return azuredns.NewDNSProvider()
	},
	"cloudflare": func() (challenge.Provider, error) {
		return cloudflare.NewDNSProvider()
	},
	"digitalocean": func() (challenge.Provider, error) {
		return digitalocean.NewDNSProvider()
	},
	"dnsupdate": func() (challenge.Provider, error) {
		return dnsupdate.NewDNSProvider()
	},
	"rfc2136": func() (challenge.Provider, error) {
		return dnsupdate.NewDNSProvider()
	},
	"exec": func() (challenge.Provider, error) {
		return exec.NewDNSProvider()
	},
	"gcloud": func() (challenge.Provider, error) {
		return gcloud.NewDNSProvider()
	},
	"hetzner": func() (challenge.Provider, error) {
		return hetzner.NewDNSProvider()
	},
	"httpreq": func() (challenge.Provider, error) {
		return httpreq.NewDNSProvider()
	},
	"route53": func() (challenge.Provider, error) {
		return route53.NewDNSProvider()
	},
}

func newDNSProvider(name string) (challenge.Provider, error) {
	legolog.SetDefault(slog.Default())
	normalized := strings.ToLower(strings.TrimSpace(name))
	factory, ok := providerFactories[normalized]
	if !ok {
		return nil, fmt.Errorf("unsupported ACME DNS provider %q", name)
	}
	provider, err := factory()
	if err != nil {
		return nil, fmt.Errorf("configure ACME DNS provider %q: %w", normalized, err)
	}
	return provider, nil
}
