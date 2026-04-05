package middleware

import (
	"net/http"
	"strings"
)

func MakeVirtualHostBucketAddressingMiddleware(baseEndpoint string, next http.Handler) http.Handler {
	endpointSuffix := "." + baseEndpoint
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hostname := r.Host
		if colonIdx := strings.LastIndex(hostname, ":"); colonIdx != -1 {
			if bracketIdx := strings.LastIndex(hostname, "]"); bracketIdx < colonIdx {
				hostname = hostname[:colonIdx]
			}
		}
		if hostname != baseEndpoint && strings.HasSuffix(hostname, endpointSuffix) {
			bucket := strings.TrimSuffix(hostname, endpointSuffix)
			if bucket != "" {
				r.URL.Path = strings.TrimSuffix("/"+bucket+r.URL.Path, "/")
			}
		}
		next.ServeHTTP(w, r)
	})
}
