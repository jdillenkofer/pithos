package admin

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
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
	Context  context.Context
	Settings *settings.Settings
	Storage  storage.Storage
	// StorageConfig is the storage configuration Storage was built from.
	StorageConfig      []byte
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
	started       time.Time
	workers       backgroundWorkers
	emptyJobs     emptyBucketJobs
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
	a.renderStatus(w, r, http.StatusOK, title, templateName, data)
}

func (a *App) renderStatus(w http.ResponseWriter, r *http.Request, status int, title, templateName string, data any) {
	var content bytes.Buffer
	if err := a.pages.ExecuteTemplate(&content, templateName, data); err != nil {
		a.fail(w, r, http.StatusInternalServerError, "Unable to render page")
		slog.ErrorContext(r.Context(), "Render admin page", "error", err)
		return
	}
	if r.Header.Get("HX-Request") == "true" && r.Header.Get("HX-Target") == "main" {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(status)
		_, _ = w.Write(content.Bytes())
		return
	}
	page, err := a.renderLayout(r, title, template.HTML(content.String()))
	if err != nil {
		a.fail(w, r, http.StatusInternalServerError, "Unable to render page")
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(page)
}

func (a *App) renderLayout(r *http.Request, title string, content template.HTML) ([]byte, error) {
	credential := currentCredential(r)
	view := struct {
		Title, Section, AccessKeyID string
		Content                     template.HTML
		CSRF                        string
		Authenticated               bool
	}{title, navSection(title), credential.AccessKeyID, content, csrfToken(r), credential.AccessKeyID != ""}
	var page bytes.Buffer
	if err := a.pages.ExecuteTemplate(&page, "layout", view); err != nil {
		return nil, err
	}
	return page.Bytes(), nil
}

// navSection maps a page title to the navigation entry it belongs to.
func navSection(title string) string {
	switch {
	case title == "Objects", title == "Object details", title == "Upload", title == "Create bucket", strings.HasPrefix(title, "Bucket: "):
		return "Buckets"
	case strings.HasPrefix(title, "Policy: "):
		return "Policies"
	case title == "Credential created":
		return "Credentials"
	}
	return title
}

// fail renders an error page inside the layout. Fragment requests and
// handlers without templates receive the message as plain text.
func (a *App) fail(w http.ResponseWriter, r *http.Request, status int, message string) {
	// Scripted uploads ask for text/plain to show the message in place.
	if a.pages == nil || r.Header.Get("HX-Request") == "true" || r.Header.Get("Accept") == "text/plain" {
		http.Error(w, message, status)
		return
	}
	var content bytes.Buffer
	data := struct {
		Status        int
		StatusText    string
		Message, Back string
	}{status, http.StatusText(status), message, sameOriginBackLink(r)}
	if err := a.pages.ExecuteTemplate(&content, "error", data); err != nil {
		http.Error(w, message, status)
		return
	}
	page, err := a.renderLayout(r, http.StatusText(status), template.HTML(content.String()))
	if err != nil {
		http.Error(w, message, status)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(page)
}

// sameOriginBackLink returns the referring admin path, or "" for any other origin.
func sameOriginBackLink(r *http.Request) string {
	referer, err := url.Parse(r.Referer())
	if err != nil || referer.Host != r.Host || (referer.Scheme != "http" && referer.Scheme != "https") {
		return ""
	}
	return referer.RequestURI()
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
