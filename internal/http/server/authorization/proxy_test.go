package authorization_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestProxyResolverTrustBoundary(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		name, remote       string
		headers            map[string][]string
		wantIP, wantScheme string
	}{
		{"untrusted peer", "203.0.113.9", map[string][]string{"X-Forwarded-For": {"198.51.100.7"}, "X-Forwarded-Proto": {"https"}}, "203.0.113.9", "http"},
		{"appended real client", "10.0.0.1", map[string][]string{"X-Forwarded-For": {"198.51.100.7, 203.0.113.9"}}, "203.0.113.9", "http"},
		{"trusted intermediate", "10.0.0.1", map[string][]string{"X-Forwarded-For": {"198.51.100.7, 203.0.113.9, 10.0.0.2"}}, "203.0.113.9", "http"},
		{"multiple header lines", "10.0.0.1", map[string][]string{"X-Forwarded-For": {"198.51.100.7", "203.0.113.9, 10.0.0.2"}}, "203.0.113.9", "http"},
		{"ignore untrusted claims", "10.0.0.1", map[string][]string{"X-Forwarded-For": {"invalid, 203.0.113.9"}}, "203.0.113.9", "http"},
		{"malformed trusted hop", "10.0.0.1", map[string][]string{"X-Forwarded-For": {"198.51.100.7, invalid"}, "CF-Connecting-IP": {"198.51.100.7"}}, "10.0.0.1", "http"},
		{"empty hop", "10.0.0.1", map[string][]string{"X-Forwarded-For": {"198.51.100.7,"}}, "10.0.0.1", "http"},
		{"IPv6", "fd00::1", map[string][]string{"X-Forwarded-For": {"198.51.100.7, 2001:db8::1, fd00::2"}}, "2001:db8::1", "http"},
		{"trusted ingress CF", "10.0.0.1", map[string][]string{"CF-Connecting-IP": {"198.51.100.7"}, "X-Forwarded-Proto": {"https"}}, "198.51.100.7", "https"},
		{"chain before CF", "10.0.0.1", map[string][]string{"CF-Connecting-IP": {"198.51.100.7"}, "X-Forwarded-For": {"203.0.113.9"}}, "203.0.113.9", "http"},
		{"ambiguous CF", "10.0.0.1", map[string][]string{"CF-Connecting-IP": {"198.51.100.7", "203.0.113.9"}}, "10.0.0.1", "http"},
		{"scheme list", "10.0.0.1", map[string][]string{"X-Forwarded-Proto": {"https, http"}}, "10.0.0.1", "http"},
		{"repeated scheme", "10.0.0.1", map[string][]string{"X-Forwarded-Proto": {"https", "http"}}, "10.0.0.1", "http"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resolver, err := authorization.NewProxyResolver(authorization.ProxyOptions{TrustForwardedHeaders: true, TrustedProxyCIDRs: []string{"10.0.0.0/8", "fc00::/7"}})
			require.NoError(t, err)
			ip, scheme := resolver.Resolve(authorization.HTTPRequest{RemoteIP: &tc.remote, Scheme: "http", Headers: tc.headers})
			require.NotNil(t, ip)
			require.Equal(t, tc.wantIP, *ip)
			require.Equal(t, tc.wantScheme, scheme)
		})
	}
}

func TestProxyTrustDisabledIgnoresForwardingHeaders(t *testing.T) {
	testutils.SkipIfIntegration(t)
	resolver, err := authorization.NewProxyResolver(authorization.ProxyOptions{})
	require.NoError(t, err)
	remote := "10.0.0.1"
	ip, scheme := resolver.Resolve(authorization.HTTPRequest{RemoteIP: &remote, Scheme: "https", Headers: map[string][]string{"X-Forwarded-For": {"198.51.100.7"}, "X-Forwarded-Proto": {"http"}}})
	require.Equal(t, remote, *ip)
	require.Equal(t, "https", scheme)
}

func proxyPolicyFile(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"IpAddress":{"aws:SourceIp":"198.51.100.7"}}}}},"bindings":[{"policy":"p","subjects":[{"type":"anonymous"}]}]}`), 0600))
	return path
}

func TestBothAuthorizersRejectInvalidProxyConfiguration(t *testing.T) {
	testutils.SkipIfIntegration(t)
	path := proxyPolicyFile(t)
	for _, cidrs := range [][]string{nil, {"invalid"}, {"10.0.0.0/8", "invalid"}} {
		_, err := lua.NewLuaAuthorizerWithOptions(`function authorizeRequest(r) return true end`, lua.Options{TrustForwardedHeaders: true, TrustedProxyCIDRs: cidrs})
		require.Error(t, err)
		_, err = policy.NewAuthorizerWithOptions(path, 0, policy.Options{TrustForwardedHeaders: true, TrustedProxyCIDRs: cidrs})
		require.Error(t, err)
	}
	_, err := authorization.NewProxyResolver(authorization.ProxyOptions{TrustedProxyCIDRs: []string{"invalid"}})
	require.Error(t, err)
}

func TestBothAuthorizersRejectSpoofedForwardedAllowlistedIP(t *testing.T) {
	testutils.SkipIfIntegration(t)
	l, err := lua.NewLuaAuthorizerWithOptions(`function authorizeRequest(r) return r.httpRequest:clientIPInCIDR("198.51.100.7/32") end`, lua.Options{TrustForwardedHeaders: true, TrustedProxyCIDRs: []string{"10.0.0.0/8"}})
	require.NoError(t, err)
	p, err := policy.NewAuthorizerWithOptions(proxyPolicyFile(t), 0, policy.Options{TrustForwardedHeaders: true, TrustedProxyCIDRs: []string{"10.0.0.0/8"}})
	require.NoError(t, err)
	defer p.Close()
	for name, authorizer := range map[string]authorization.RequestAuthorizer{"lua": l, "policy": p} {
		t.Run(name, func(t *testing.T) {
			remote := "10.0.0.1"
			r := &authorization.Request{Operation: authorization.OperationGetObject, HttpRequest: authorization.HTTPRequest{RemoteIP: &remote, Headers: map[string][]string{"X-Forwarded-For": {"198.51.100.7, 203.0.113.9"}, "CF-Connecting-IP": {"198.51.100.7"}}}}
			d, err := authorizer.AuthorizeRequest(context.Background(), r)
			require.NoError(t, err)
			require.NotEqual(t, authorization.Allow, d.Effect)
			r.HttpRequest.Headers = map[string][]string{"X-Forwarded-For": {"198.51.100.7"}}
			d, err = authorizer.AuthorizeRequest(context.Background(), r)
			require.NoError(t, err)
			require.Equal(t, authorization.Allow, d.Effect)
		})
	}
}
