package middleware

import (
	"net/http"
	"strings"
)

func MakeHostnameRoutingHandler(apiEndpoint string, apiHandler http.Handler, websiteEndpoint string, websiteHandler http.Handler, fallbackHandler http.Handler) http.Handler {
	apiSuffix := "." + apiEndpoint
	websiteSuffix := "." + websiteEndpoint

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if colonIdx := strings.LastIndex(host, ":"); colonIdx != -1 {
			if bracketIdx := strings.LastIndex(host, "]"); bracketIdx < colonIdx {
				host = host[:colonIdx]
			}
		}

		if host == apiEndpoint || strings.HasSuffix(host, apiSuffix) {
			apiHandler.ServeHTTP(w, r)
			return
		}

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
