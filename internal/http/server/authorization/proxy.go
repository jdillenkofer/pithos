package authorization

import (
	"fmt"
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

func NewProxyResolver(options ProxyOptions) (*ProxyResolver, error) {
	if options.TrustForwardedHeaders && len(options.TrustedProxyCIDRs) == 0 {
		return nil, fmt.Errorf("trusted proxy CIDRs are required when trusting forwarded headers")
	}
	resolver := &ProxyResolver{trustForwardedHeaders: options.TrustForwardedHeaders}
	for _, cidrString := range options.TrustedProxyCIDRs {
		_, cidr, err := net.ParseCIDR(cidrString)
		if err != nil {
			return nil, fmt.Errorf("invalid trusted proxy CIDR %q: %w", cidrString, err)
		}
		resolver.trustedProxyCIDRs = append(resolver.trustedProxyCIDRs, cidr)
	}
	return resolver, nil
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

	if forwardedFor := headerValues(request.Headers, "X-Forwarded-For"); len(forwardedFor) > 0 {
		if parsed := r.parseForwardedClientIP(strings.Join(forwardedFor, ",")); parsed != nil {
			clientIP = parsed
		}
	} else if connectingIP := headerValue(request.Headers, "CF-Connecting-IP"); connectingIP != nil {
		if ip := net.ParseIP(strings.TrimSpace(*connectingIP)); ip != nil {
			parsed := ip.String()
			clientIP = &parsed
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
	for _, cidr := range r.trustedProxyCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

func headerValues(headers map[string][]string, key string) []string {
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
	return values
}

func headerValue(headers map[string][]string, key string) *string {
	values := headerValues(headers, key)
	if len(values) != 1 {
		return nil
	}
	value := values[0]
	return &value
}

func (r *ProxyResolver) parseForwardedClientIP(forwardedFor string) *string {
	chain := strings.Split(forwardedFor, ",")
	var candidate *string
	for i := len(chain) - 1; i >= 0; i-- {
		ip := net.ParseIP(strings.TrimSpace(chain[i]))
		if ip == nil {
			return nil
		}
		parsed := ip.String()
		candidate = &parsed
		if !r.isTrustedProxy(candidate) {
			return candidate
		}
	}
	return candidate
}

func parseForwardedScheme(forwardedProto string) *string {
	// The trusted ingress must overwrite this header. A list of schemes has
	// no reliable correspondence to the client-IP chain and is not trusted.
	parsed := strings.ToLower(strings.TrimSpace(forwardedProto))
	if parsed != "http" && parsed != "https" {
		return nil
	}
	return &parsed
}
