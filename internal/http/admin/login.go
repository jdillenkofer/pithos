package admin

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/admin/session"
	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/storage"
)

const sessionCookieName = "pithos_admin_session"
const loginCSRFCookieName = "pithos_admin_login_csrf"

type uploadReaderKey struct{}

func credentialHash(c authentication.Credential) string {
	b, _ := json.Marshal(c)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
func equalSecret(a, b string) bool {
	aa, bb := sha256.Sum256([]byte(a)), sha256.Sum256([]byte(b))
	return subtle.ConstantTimeCompare(aa[:], bb[:]) == 1
}
func (a *App) cookie(name, value, path string, maxAge int) *http.Cookie {
	return &http.Cookie{Name: name, Value: value, Path: path, MaxAge: maxAge, HttpOnly: true, Secure: a.options.Settings.AdminSecureCookie(), SameSite: http.SameSiteStrictMode}
}
func (a *App) clearSession(w http.ResponseWriter) {
	http.SetCookie(w, a.cookie(sessionCookieName, "", "/", -1))
}

func (a *App) loginPage(w http.ResponseWriter, r *http.Request) {
	a.renderLogin(w, r, http.StatusOK, "", "")
}

// renderLogin issues a fresh signed login CSRF token, so a failed attempt can
// be retried directly from the returned form.
func (a *App) renderLogin(w http.ResponseWriter, r *http.Request, status int, message, accessKey string) {
	nonce, err := session.NewToken()
	if err != nil {
		a.fail(w, r, 500, "Unable to initialize login")
		return
	}
	mac := hmac.New(sha256.New, a.loginKey[:])
	_, _ = mac.Write([]byte(nonce))
	token := nonce + "." + hex.EncodeToString(mac.Sum(nil))
	http.SetCookie(w, a.cookie(loginCSRFCookieName, token, "/login", 600))
	a.renderStatus(w, r, status, "Login", "login", struct {
		CSRF, Error, AccessKey string
		Disabled               bool
	}{token, message, accessKey, a.sessions == nil})
}
func (a *App) validLoginCSRF(r *http.Request) bool {
	cookie, err := r.Cookie(loginCSRFCookieName)
	if err != nil {
		return false
	}
	token := r.PostFormValue("csrf_token")
	if !equalSecret(cookie.Value, token) {
		return false
	}
	nonce, sig, ok := strings.Cut(token, ".")
	if !ok || len(nonce) != 64 {
		return false
	}
	mac := hmac.New(sha256.New, a.loginKey[:])
	_, _ = mac.Write([]byte(nonce))
	signature, err := hex.DecodeString(sig)
	return err == nil && hmac.Equal(signature, mac.Sum(nil)) && a.validOrigin(r)
}
func (a *App) validOrigin(r *http.Request) bool {
	if origin := r.Header.Get("Origin"); origin != "" {
		u, err := url.Parse(origin)
		if err != nil || u.Host != r.Host || (u.Scheme != "http" && u.Scheme != "https") {
			return false
		}
	}
	return r.Header.Get("Sec-Fetch-Site") != "cross-site"
}
func (a *App) login(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 16*1024)
	if err := r.ParseForm(); err != nil {
		a.fail(w, r, 400, "Invalid login request")
		return
	}
	if !a.validLoginCSRF(r) {
		a.audit(r, "AdminLoginRejected")
		message := "Your login form expired. Please sign in again."
		// Browsers drop Secure cookies on plain HTTP, so a missing cookie
		// usually means the UI is not served through HTTPS.
		if _, err := r.Cookie(loginCSRFCookieName); err != nil && a.options.Settings.AdminSecureCookie() {
			message = "Login cookie missing. Admin cookies require HTTPS; for plain-HTTP local use set PITHOS_ADMIN_SECURE_COOKIE=false."
		}
		a.renderLogin(w, r, 403, message, "")
		return
	}
	key, secret := r.PostForm.Get("access_key"), r.PostForm.Get("secret_key")
	if len(key) > authentication.MaxAccessKeyIDLength || len(secret) > authentication.MaxSecretAccessKeyLength {
		a.audit(r, "AdminLoginFailed")
		a.renderLogin(w, r, 401, "Invalid access key or secret.", "")
		return
	}
	if !a.limiter.Allow(clientIP(r)) {
		w.Header().Set("Retry-After", "60")
		a.audit(r, "AdminLoginRateLimited")
		a.renderLogin(w, r, 429, "Too many login attempts. Try again in a minute.", key)
		return
	}
	var credential authentication.Credential
	var found bool
	var err error
	if a.options.CredentialProvider != nil {
		credential, found, err = a.options.CredentialProvider.Lookup(r.Context(), key)
	}
	secretMatches := equalSecret(credential.SecretAccessKey, secret)
	_, allowed := a.allowed[key]
	if err != nil {
		a.audit(r, "AdminLoginUnavailable")
		a.renderLogin(w, r, 503, "The credential service is unavailable. Try again later.", key)
		return
	}
	if !found || !allowed || !secretMatches || a.sessions == nil {
		a.audit(r, "AdminLoginFailed")
		a.renderLogin(w, r, 401, "Invalid access key or secret.", key)
		return
	}
	token, err := session.NewToken()
	if err != nil {
		a.fail(w, r, 500, "Unable to create session")
		return
	}
	csrf, err := session.NewToken()
	if err != nil {
		a.fail(w, r, 500, "Unable to create session")
		return
	}
	now := time.Now()
	lifetime := time.Duration(a.options.Settings.AdminSessionMaxHours()) * time.Hour
	record := session.Record{IDHash: session.Hash(token), AccessKeyID: key, AccountID: credential.AccountID, PrincipalID: credential.PrincipalID, CredentialHash: credentialHash(credential), CSRFToken: csrf, CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(lifetime), ClientIP: clientIP(r), UserAgent: r.UserAgent()}
	if len(record.UserAgent) > 1024 {
		record.UserAgent = record.UserAgent[:1024]
	}
	if err = a.sessions.Create(r.Context(), record); err != nil {
		a.fail(w, r, 503, "Session service unavailable")
		return
	}
	if previous, err := r.Cookie(sessionCookieName); err == nil {
		_ = a.sessions.Delete(r.Context(), session.Hash(previous.Value))
	}
	http.SetCookie(w, a.cookie(sessionCookieName, token, "/", int(lifetime.Seconds())))
	http.SetCookie(w, a.cookie(loginCSRFCookieName, "", "/login", -1))
	r = a.withIdentity(r, credential, record)
	a.audit(r, "AdminLogin")
	http.Redirect(w, r, "/", http.StatusSeeOther)
}
func (a *App) withIdentity(r *http.Request, c authentication.Credential, s session.Record) *http.Request {
	ctx := context.WithValue(r.Context(), stateKey{}, requestState{Credential: c, Session: s})
	ctx = authentication.WithRequestAuthentication(ctx, authentication.RequestAuthentication{Authenticated: true, Identity: &authentication.AuthenticatedIdentity{AccessKeyID: c.AccessKeyID, AccountID: c.AccountID, PrincipalID: c.PrincipalID}, Type: authentication.AuthType("admin-session")})
	return r.WithContext(ctx)
}
func (a *App) requireSession(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cookie, err := r.Cookie(sessionCookieName)
		if err != nil || len(cookie.Value) != 64 || a.sessions == nil {
			a.clearSession(w)
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		record, err := a.sessions.GetAndTouch(r.Context(), session.Hash(cookie.Value), time.Now())
		if err != nil {
			if !errors.Is(err, session.ErrExpired) {
				a.fail(w, r, 503, "Session service unavailable")
				return
			}
			a.clearSession(w)
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		credential, found, err := a.options.CredentialProvider.Lookup(r.Context(), record.AccessKeyID)
		if err != nil {
			a.fail(w, r, 503, "Credential service unavailable")
			return
		}
		_, allowed := a.allowed[record.AccessKeyID]
		if !found || !allowed || !equalSecret(credentialHash(credential), record.CredentialHash) {
			_ = a.sessions.Delete(r.Context(), record.IDHash)
			a.clearSession(w)
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		next.ServeHTTP(w, a.withIdentity(r, credential, record))
	})
}

// csrfBodyLimit bounds the body read while extracting the CSRF token. Routes
// with larger forms still enforce their own, tighter content limits.
func csrfBodyLimit(path string) int64 {
	switch path {
	case "/objects/upload":
		return storage.MaxEntitySize + 1024*1024
	case "/policies/validate", "/policies/simulate", "/policies/save":
		return policyFormLimit
	default:
		return 1024 * 1024
	}
}

func (a *App) requireCSRF(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)
			return
		}
		if !a.validOrigin(r) {
			a.fail(w, r, 403, "Cross-origin request denied")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, csrfBodyLimit(r.URL.Path))
		token := r.Header.Get("X-CSRF-Token")
		if token == "" {
			if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
				reader, err := r.MultipartReader()
				if err != nil {
					a.fail(w, r, 400, "Invalid multipart request")
					return
				}
				part, err := reader.NextPart()
				if err != nil || part.FormName() != "csrf_token" {
					a.fail(w, r, 403, "CSRF token must be the first upload field")
					return
				}
				b, err := io.ReadAll(io.LimitReader(part, 257))
				if err != nil || len(b) > 256 {
					a.fail(w, r, 403, "Invalid CSRF token")
					return
				}
				_ = part.Close()
				token = string(b)
				r = r.WithContext(context.WithValue(r.Context(), uploadReaderKey{}, reader))
			} else {
				if err := r.ParseForm(); err != nil {
					a.fail(w, r, 400, "Invalid form")
					return
				}
				token = r.PostForm.Get("csrf_token")
			}
		}
		if token == "" || !equalSecret(token, csrfToken(r)) {
			a.fail(w, r, 403, "Invalid CSRF token")
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (a *App) logout(w http.ResponseWriter, r *http.Request) {
	state, _ := r.Context().Value(stateKey{}).(requestState)
	if err := a.sessions.Delete(r.Context(), state.Session.IDHash); err != nil {
		a.fail(w, r, 503, "Unable to end session")
		return
	}
	a.audit(r, "AdminLogout")
	a.clearSession(w)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (a *App) initializeLoginKey() error { _, err := rand.Read(a.loginKey[:]); return err }
