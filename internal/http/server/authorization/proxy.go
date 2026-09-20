package authorization

import (
	"log/slog"
	"net"
	"net/textproto"
	"strings"
)

type ProxyOptions struct {
	TrustForwardedHeaders bool
	TrustedProxyCIDRs     []string
}

type ProxyResolver struct {
	trustForwardedHeaders bool
	trustedProxyCIDRs     []*net.IPNet
}

func NewProxyResolver(options ProxyOptions) *ProxyResolver {
	resolver := &ProxyResolver{trustForwardedHeaders: options.TrustForwardedHeaders}
	for _, cidrString := range options.TrustedProxyCIDRs {
		_, cidr, err := net.ParseCIDR(cidrString)
		if err != nil {
			slog.Warn("Ignoring invalid trusted proxy CIDR", "cidr", cidrString, "error", err)
			continue
		}
		resolver.trustedProxyCIDRs = append(resolver.trustedProxyCIDRs, cidr)
	}
	return resolver
}

func (r *ProxyResolver) Resolve(request HTTPRequest) (*string, string) {
	clientIP := request.RemoteIP
	scheme := request.Scheme
	if scheme == "" {
		scheme = "http"
	}
	if !r.trustForwardedHeaders || !r.isTrustedProxy(request.RemoteIP) {
		return clientIP, scheme
	}

	if connectingIP := headerValue(request.Headers, "CF-Connecting-IP"); connectingIP != nil {
		if ip := net.ParseIP(strings.TrimSpace(*connectingIP)); ip != nil {
			parsed := ip.String()
			clientIP = &parsed
		}
	} else if forwardedFor := headerValue(request.Headers, "X-Forwarded-For"); forwardedFor != nil {
		if parsed := parseForwardedClientIP(*forwardedFor); parsed != nil {
			clientIP = parsed
		}
	}
	if forwardedProto := headerValue(request.Headers, "X-Forwarded-Proto"); forwardedProto != nil {
		if parsed := parseForwardedScheme(*forwardedProto); parsed != nil {
			scheme = *parsed
		}
	}
	return clientIP, scheme
}

func (r *ProxyResolver) isTrustedProxy(remoteIP *string) bool {
	if remoteIP == nil {
		return false
	}
	ip := net.ParseIP(*remoteIP)
	if ip == nil {
		return false
	}
	if len(r.trustedProxyCIDRs) == 0 {
		return true
	}
	for _, cidr := range r.trustedProxyCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

func headerValue(headers map[string][]string, key string) *string {
	canonicalKey := textproto.CanonicalMIMEHeaderKey(key)
	values, ok := headers[canonicalKey]
	if !ok {
		for headerName, candidate := range headers {
			if strings.EqualFold(headerName, key) {
				values = candidate
				break
			}
		}
	}
	if len(values) == 0 {
		return nil
	}
	value := values[0]
	return &value
}

func parseForwardedClientIP(forwardedFor string) *string {
	first, _, _ := strings.Cut(forwardedFor, ",")
	ip := net.ParseIP(strings.TrimSpace(first))
	if ip == nil {
		return nil
	}
	parsed := ip.String()
	return &parsed
}

func parseForwardedScheme(forwardedProto string) *string {
	first, _, _ := strings.Cut(forwardedProto, ",")
	parsed := strings.ToLower(strings.TrimSpace(first))
	if parsed != "http" && parsed != "https" {
		return nil
	}
	return &parsed
}
