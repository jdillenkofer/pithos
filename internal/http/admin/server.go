// Package admin serves the embedded global administration UI on its own listener.
// Its authentication and permissions are independent of the S3 API.
package admin

import (
	"context"
	"embed"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/admin/session"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/oklog/ulid/v2"
)

//go:embed templates/*.html static/*
var assets embed.FS

const contentSecurityPolicy = "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' data:; media-src 'self'; connect-src 'self'; base-uri 'none'; form-action 'self'; frame-ancestors 'none'; object-src 'none'"

// NewHandler builds an isolated admin router. Only health, login and embedded
// static assets are public; all management routes require a fresh credential
// lookup and, for unsafe methods, a per-session CSRF token.
func NewHandler(options ...Options) (http.Handler, error) {
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	if opts.Context == nil {
		opts.Context = context.Background()
	}
	if opts.Settings == nil {
		opts.Settings = &settings.Settings{}
	}
	pages, err := template.ParseFS(assets, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse admin templates: %w", err)
	}
	lifetime, cancel := context.WithCancel(opts.Context)
	opts.Context = lifetime
	a := &App{options: opts, pages: pages, limiter: session.NewLoginLimiter(), started: time.Now()}
	ready := false
	defer func() {
		if !ready {
			cancel()
			a.workers.wait()
		}
	}()
	if err := a.initializeLoginKey(); err != nil {
		return nil, err
	}
	if err := a.initializeSessions(); err != nil {
		return nil, err
	}
	public := http.NewServeMux()
	public.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	for _, file := range []struct{ name, kind string }{{"admin.css", "text/css; charset=utf-8"}, {"admin.js", "text/javascript; charset=utf-8"}, {"htmx.min.js", "text/javascript; charset=utf-8"}, {"logo.svg", "image/svg+xml"}} {
		contents, err := assets.ReadFile("static/" + file.name)
		if err != nil {
			return nil, fmt.Errorf("read admin asset: %w", err)
		}
		public.HandleFunc("GET /static/"+file.name, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", file.kind)
			_, _ = w.Write(contents)
		})
	}
	public.HandleFunc("GET /login", a.loginPage)
	public.HandleFunc("POST /login", a.login)
	protected := http.NewServeMux()
	protected.HandleFunc("GET /{$}", a.dashboard)
	protected.HandleFunc("POST /logout", a.logout)
	a.registerBuckets(protected)
	a.registerObjects(protected)
	a.registerCredentials(protected)
	a.registerPolicies(protected)
	a.registerAudit(protected)
	a.registerStorage(protected)
	public.Handle("/", a.requireSession(a.requireCSRF(protected)))
	ready = true
	return &Handler{Handler: securityHeaders(public), cancel: cancel, workers: &a.workers}, nil
}

func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", contentSecurityPolicy)
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "same-origin")
		w.Header().Set("Cache-Control", "no-store")
		ctx := context.WithValue(r.Context(), authentication.RequestIDContextKey{}, ulid.Make().String())
		ctx = context.WithValue(ctx, authentication.ClientIPContextKey{}, clientIP(r))
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
