package admin

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/http/admin/session"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
)

// Options contains live server dependencies. Admin handlers operate on Storage
// directly; they never reuse or weaken S3 API authorization.
type Options struct {
	Context            context.Context
	Settings           *settings.Settings
	Storage            storage.Storage
	StorageStarted     bool
	Databases          []database.Database
	CredentialProvider authentication.CredentialProvider
	Authorizer         authorization.RequestAuthorizer
}

type App struct {
	options       Options
	pages         *template.Template
	sessions      *session.Repository
	limiter       *session.LoginLimiter
	allowed       map[string]struct{}
	loginKey      [32]byte
	dashboardMu   sync.Mutex
	lastDashboard dashboardSample
	workers       backgroundWorkers
}

type requestState struct {
	Credential authentication.Credential
	Session    session.Record
}
type stateKey struct{}

func currentCredential(r *http.Request) authentication.Credential {
	state, _ := r.Context().Value(stateKey{}).(requestState)
	return state.Credential
}
func csrfToken(r *http.Request) string {
	state, _ := r.Context().Value(stateKey{}).(requestState)
	return state.Session.CSRFToken
}

func (a *App) render(w http.ResponseWriter, r *http.Request, title, templateName string, data any) {
	var content bytes.Buffer
	if err := a.pages.ExecuteTemplate(&content, templateName, data); err != nil {
		a.fail(w, r, http.StatusInternalServerError, "Unable to render page")
		slog.ErrorContext(r.Context(), "Render admin page", "error", err)
		return
	}
	if r.Header.Get("HX-Request") == "true" && r.Header.Get("HX-Target") == "main" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(content.Bytes())
		return
	}
	var page bytes.Buffer
	view := struct {
		Title         string
		Content       template.HTML
		CSRF          string
		Authenticated bool
	}{title, template.HTML(content.String()), csrfToken(r), currentCredential(r).AccessKeyID != ""}
	if err := a.pages.ExecuteTemplate(&page, "layout", view); err != nil {
		a.fail(w, r, http.StatusInternalServerError, "Unable to render page")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(page.Bytes())
}
func (a *App) fail(w http.ResponseWriter, r *http.Request, status int, message string) {
	http.Error(w, message, status)
}
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err == nil {
		return host
	}
	return r.RemoteAddr
}

// audit records administrative actions independently of object storage calls.
// Audit-capable storage stacks additionally persist these events in their chain.
func (a *App) audit(r *http.Request, operation string) {
	c := currentCredential(r)
	status := int32(http.StatusOK)
	switch operation {
	case "AdminLoginFailed":
		status = http.StatusUnauthorized
	case "AdminLoginRejected":
		status = http.StatusForbidden
	case "AdminLoginRateLimited":
		status = http.StatusTooManyRequests
	case "AdminLoginUnavailable":
		status = http.StatusServiceUnavailable
	case "PolicyValidationFailed":
		status = http.StatusBadRequest
	case "SavePolicyFailed":
		status = http.StatusInternalServerError
	case "SavePolicyConflict":
		status = http.StatusConflict
	}
	slog.InfoContext(r.Context(), "Admin event", "operation", operation, "status", status, "accessKeyId", c.AccessKeyID, "accountId", c.AccountID, "principalId", c.PrincipalID, "clientIP", clientIP(r))
	if recorder, ok := a.options.Storage.(auditlog.AdminEventRecorder); ok {
		ctx := auditlog.WithAdminActor(r.Context(), auditlog.ActorDetails{CredentialID: c.AccessKeyID, AccountID: c.AccountID, PrincipalID: c.PrincipalID, AuthType: "admin-session"})
		recorder.RecordAdminEvent(auditlog.WithAdminStatus(ctx, status), operation)
	}
}

func (a *App) initializeSessions() error {
	a.allowed = make(map[string]struct{})
	for _, key := range a.options.Settings.AdminAccessKeys() {
		a.allowed[key] = struct{}{}
	}
	if len(a.allowed) == 0 {
		return nil
	}
	if a.options.CredentialProvider == nil {
		return fmt.Errorf("admin access requires a credential provider")
	}
	idle, max := a.options.Settings.AdminSessionIdleMinutes(), a.options.Settings.AdminSessionMaxHours()
	if idle <= 0 || idle > 525600 || max <= 0 || max > 8760 {
		return fmt.Errorf("admin session lifetimes must be positive and at most one year")
	}
	index := a.options.Settings.AdminDatabaseIndex()
	if index < 0 || index >= len(a.options.Databases) {
		return fmt.Errorf("admin database index %d is not configured", index)
	}
	repo, err := session.NewRepository(a.options.Context, a.options.Databases[index], time.Duration(idle)*time.Minute)
	if err != nil {
		return err
	}
	a.sessions = repo
	a.workers.goRun(func() {
		repo.RunCleanup(a.options.Context, func(err error) { slog.Error("Clean up admin sessions", "error", err) })
	})
	return nil
}
