package middleware

import (
	"net/http"
	"strings"
)

// HostWithoutPort strips the port from a Host header value, keeping IPv6
// literals intact.
func HostWithoutPort(host string) string {
	if colonIdx := strings.LastIndex(host, ":"); colonIdx != -1 {
		if bracketIdx := strings.LastIndex(host, "]"); bracketIdx < colonIdx {
			return host[:colonIdx]
		}
	}
	return host
}

// IsAPIHost reports whether a request for host is routed to the S3 API;
// every other host is served by the website endpoint.
func IsAPIHost(host string, apiEndpoint string) bool {
	host = HostWithoutPort(host)
	return host == apiEndpoint || strings.HasSuffix(host, "."+apiEndpoint)
}

func MakeHostnameRoutingHandler(apiEndpoint string, apiHandler http.Handler, websiteEndpoint string, websiteHandler http.Handler, fallbackHandler http.Handler) http.Handler {
	websiteSuffix := "." + websiteEndpoint

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if IsAPIHost(r.Host, apiEndpoint) {
			apiHandler.ServeHTTP(w, r)
			return
		}

		host := HostWithoutPort(r.Host)

		if strings.HasSuffix(host, websiteSuffix) {
			bucket := strings.TrimSuffix(host, websiteSuffix)
			if bucket != "" {
				r.URL.Path = "/" + bucket + r.URL.Path
				websiteHandler.ServeHTTP(w, r)
				return
			}
		}

		fallbackHandler.ServeHTTP(w, r)
	})
}
