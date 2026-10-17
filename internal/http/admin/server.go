// Package admin serves the embedded global administration UI on its own listener.
// Its authentication and permissions are independent of the S3 API.
package admin

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"net/http"
)

//go:embed templates/*.html static/*.css
var assets embed.FS

const contentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'; object-src 'none'"

// NewHandler builds the admin router. Until session authentication is installed,
// no management routes are exposed, regardless of configured admin credentials.
func NewHandler() (http.Handler, error) {
	pages, err := template.ParseFS(assets, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse admin templates: %w", err)
	}
	stylesheet, err := assets.ReadFile("static/admin.css")
	if err != nil {
		return nil, fmt.Errorf("load admin static assets: %w", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /static/admin.css", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		_, _ = w.Write(stylesheet)
	})
	mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		var body bytes.Buffer
		if err := pages.ExecuteTemplate(&body, "layout", struct{ Title string }{Title: "Admin UI"}); err != nil {
			http.Error(w, "Unable to render page", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write(body.Bytes())
	})
	return securityHeaders(mux), nil
}

// securityHeaders wraps the entire router so errors and static responses receive
// the same protections as rendered pages.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", contentSecurityPolicy)
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
