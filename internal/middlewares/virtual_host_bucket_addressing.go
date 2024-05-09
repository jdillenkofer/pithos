package middlewares

import (
	"net/http"
	"strings"
)

func MakeVirtualHostBucketAddressingMiddleware(baseEndpoint string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hostname := r.Host
		if hostname != baseEndpoint {
			endpointSplit := strings.SplitN(hostname, ".", 2)
			if len(endpointSplit) == 2 {
				bucket := endpointSplit[0]
				r.URL.Path = strings.TrimSuffix("/"+bucket+r.URL.Path, "/")
			}
		}
		next.ServeHTTP(w, r)
	})
}
