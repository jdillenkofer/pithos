package tlsmanager

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-acme/lego/v5/challenge"
	"github.com/mholt/acmez/v3/acme"
	"github.com/miekg/dns"
)

const (
	defaultDNSPropagationTimeout = 60 * time.Second
	defaultDNSPollingInterval    = 2 * time.Second
)

type txtLookup func(context.Context, string) ([]string, error)

// DNSAdapter makes the curated lego DNS providers usable by CertMagic. Provider
// mutations are serialized because several lego implementations maintain
// mutable record state internally.
type DNSAdapter struct {
	provider     challenge.Provider
	timeout      time.Duration
	interval     time.Duration
	recursiveTXT txtLookup
	authorityTXT txtLookup
	providerMu   sync.Mutex
}

func NewDNSAdapter(provider challenge.Provider) *DNSAdapter {
	timeout, interval := defaultDNSPropagationTimeout, defaultDNSPollingInterval
	if providerTimeout, ok := provider.(challenge.ProviderTimeout); ok {
		timeout, interval = providerTimeout.Timeout()
	}
	if timeout <= 0 {
		timeout = defaultDNSPropagationTimeout
	}
	if interval <= 0 {
		interval = defaultDNSPollingInterval
	}
	return &DNSAdapter{
		provider:     provider,
		timeout:      timeout,
		interval:     interval,
		recursiveTXT: defaultRecursiveTXTLookup,
		authorityTXT: defaultAuthoritativeTXTLookup,
	}
}

func (a *DNSAdapter) Present(ctx context.Context, acmeChallenge acme.Challenge) error {
	a.providerMu.Lock()
	defer a.providerMu.Unlock()
	return a.provider.Present(ctx, acmeChallenge.Identifier.Value, acmeChallenge.Token, acmeChallenge.KeyAuthorization)
}

func (a *DNSAdapter) CleanUp(ctx context.Context, acmeChallenge acme.Challenge) error {
	a.providerMu.Lock()
	defer a.providerMu.Unlock()
	return a.provider.CleanUp(context.WithoutCancel(ctx), acmeChallenge.Identifier.Value, acmeChallenge.Token, acmeChallenge.KeyAuthorization)
}

func (a *DNSAdapter) Wait(ctx context.Context, acmeChallenge acme.Challenge) error {
	fqdn := dns.Fqdn(acmeChallenge.DNS01TXTRecordName())
	expected := acmeChallenge.DNS01KeyAuthorization()
	waitCtx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()

	var recursiveErr, authorityErr error
	for {
		var recursiveValues, authorityValues []string
		recursiveValues, recursiveErr = a.recursiveTXT(waitCtx, fqdn)
		authorityValues, authorityErr = a.authorityTXT(waitCtx, fqdn)
		if recursiveErr == nil && authorityErr == nil &&
			containsTXT(recursiveValues, expected) && containsTXT(authorityValues, expected) {
			return nil
		}

		timer := time.NewTimer(a.interval)
		select {
		case <-waitCtx.Done():
			timer.Stop()
			return fmt.Errorf("wait for DNS TXT propagation at %s: %w (recursive: %v; authoritative: %v)",
				fqdn, waitCtx.Err(), recursiveErr, authorityErr)
		case <-timer.C:
		}
	}
}

func containsTXT(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func defaultRecursiveTXTLookup(ctx context.Context, fqdn string) ([]string, error) {
	return net.DefaultResolver.LookupTXT(ctx, strings.TrimSuffix(fqdn, "."))
}

func defaultAuthoritativeTXTLookup(ctx context.Context, fqdn string) ([]string, error) {
	canonicalName, err := net.DefaultResolver.LookupCNAME(ctx, strings.TrimSuffix(fqdn, "."))
	if err != nil {
		return nil, fmt.Errorf("resolve DNS challenge CNAME: %w", err)
	}
	fqdn = dns.Fqdn(canonicalName)
	nameservers, err := findAuthoritativeNameservers(ctx, fqdn)
	if err != nil {
		return nil, err
	}

	var commonValues map[string]struct{}
	for index, nameserver := range nameservers {
		if _, _, err := net.SplitHostPort(nameserver); err != nil {
			nameserver = net.JoinHostPort(strings.TrimSuffix(nameserver, "."), "53")
		}
		message := new(dns.Msg)
		message.SetQuestion(fqdn, dns.TypeTXT)
		response, _, queryErr := (&dns.Client{}).ExchangeContext(ctx, message, nameserver)
		if queryErr != nil {
			return nil, fmt.Errorf("query authoritative nameserver %s: %w", nameserver, queryErr)
		}
		if response == nil {
			return nil, fmt.Errorf("empty response from authoritative nameserver %s", nameserver)
		}
		if response.Rcode != dns.RcodeSuccess {
			return nil, fmt.Errorf("authoritative nameserver %s returned %s", nameserver, dns.RcodeToString[response.Rcode])
		}
		values := make(map[string]struct{})
		for _, answer := range response.Answer {
			if txt, ok := answer.(*dns.TXT); ok {
				values[strings.Join(txt.Txt, "")] = struct{}{}
			}
		}
		if index == 0 {
			commonValues = values
			continue
		}
		for value := range commonValues {
			if _, ok := values[value]; !ok {
				delete(commonValues, value)
			}
		}
	}
	values := make([]string, 0, len(commonValues))
	for value := range commonValues {
		values = append(values, value)
	}
	return values, nil
}

func findAuthoritativeNameservers(ctx context.Context, fqdn string) ([]string, error) {
	labels := dns.SplitDomainName(fqdn)
	for index := 0; index < len(labels); index++ {
		candidate := strings.Join(labels[index:], ".")
		records, err := net.DefaultResolver.LookupNS(ctx, candidate)
		if err == nil && len(records) > 0 {
			nameservers := make([]string, 0, len(records))
			for _, record := range records {
				nameservers = append(nameservers, record.Host)
			}
			return nameservers, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("could not find authoritative nameservers for %s", fqdn)
}
