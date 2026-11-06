package admin

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"net/http"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
)

type credentialsPage struct {
	CSRF               string
	Credentials        []authentication.CredentialInfo
	CanList            bool
	CanManage          bool
	CurrentAccessKeyID string
}

type credentialCreatedPage struct {
	CSRF       string
	Credential authentication.Credential
}

// registerCredentials relies on the enclosing router's session and CSRF guards.
func (a *App) registerCredentials(mux *http.ServeMux) {
	mux.HandleFunc("GET /credentials", a.listCredentials)
	mux.HandleFunc("POST /credentials/create", a.createCredential)
	mux.HandleFunc("POST /credentials/delete", a.deleteCredential)
}

func (a *App) listCredentials(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	_, writable := a.options.CredentialProvider.(authentication.CredentialStore)
	page := credentialsPage{CSRF: csrfToken(r), CanManage: writable, CurrentAccessKeyID: currentCredential(r).AccessKeyID}
	if lister, ok := a.options.CredentialProvider.(authentication.CredentialLister); ok {
		credentials, err := lister.List(r.Context())
		if err != nil {
			a.fail(w, r, http.StatusInternalServerError, "Unable to list credentials")
			return
		}
		page.CanList = true
		page.Credentials = credentials
	}
	a.render(w, r, "Credentials", "credentials", page)
}

func (a *App) createCredential(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	store, ok := a.options.CredentialProvider.(authentication.CredentialStore)
	if !ok {
		a.fail(w, r, http.StatusForbidden, "This credential provider is read-only")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16*1024)
	if err := r.ParseForm(); err != nil {
		a.fail(w, r, http.StatusBadRequest, "Invalid credential form")
		return
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		a.fail(w, r, http.StatusInternalServerError, "Unable to generate credential")
		return
	}
	credential := authentication.Credential{
		AccessKeyID:     r.PostForm.Get("accessKeyId"),
		SecretAccessKey: base64.RawURLEncoding.EncodeToString(secret),
		AccountID:       r.PostForm.Get("accountId"),
		PrincipalID:     r.PostForm.Get("principalId"),
	}
	if err := authentication.ValidateCredential(credential); err != nil {
		a.fail(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if err := authentication.ValidateCredentialAccountID(credential.AccountID); err != nil {
		a.fail(w, r, http.StatusBadRequest, err.Error())
		return
	}
	if err := store.Create(r.Context(), credential); err != nil {
		if errors.Is(err, authentication.ErrCredentialExists) {
			a.fail(w, r, http.StatusConflict, "Access key ID already exists")
		} else {
			a.fail(w, r, http.StatusInternalServerError, "Unable to create credential")
		}
		return
	}
	a.audit(r, "AdminCreateCredential")
	// This POST response is the only place the generated secret is presented.
	// It is never put in a redirect, session, audit record, or listing.
	a.render(w, r, "Credential created", "credentials-created", credentialCreatedPage{CSRF: csrfToken(r), Credential: credential})
}

func (a *App) deleteCredential(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	store, ok := a.options.CredentialProvider.(authentication.CredentialStore)
	if !ok {
		a.fail(w, r, http.StatusForbidden, "This credential provider is read-only")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 16*1024)
	if err := r.ParseForm(); err != nil {
		a.fail(w, r, http.StatusBadRequest, "Invalid credential form")
		return
	}
	accessKeyID := r.PostForm.Get("accessKeyId")
	if accessKeyID == "" || len(accessKeyID) > authentication.MaxAccessKeyIDLength {
		a.fail(w, r, http.StatusBadRequest, "Invalid access key ID")
		return
	}
	if accessKeyID == currentCredential(r).AccessKeyID {
		a.fail(w, r, http.StatusBadRequest, "Cannot delete the credential used by this session")
		return
	}
	if !a.confirmed(w, r, confirmation{
		Title:   "Delete access key " + accessKeyID + "?",
		Message: "Clients using this access key immediately lose access to the S3 API and existing console sessions with it end. This cannot be undone.",
		Action:  "Delete access key",
	}) {
		return
	}
	if err := store.Delete(r.Context(), accessKeyID); err != nil {
		if errors.Is(err, authentication.ErrCredentialNotFound) {
			a.fail(w, r, http.StatusNotFound, "Credential not found")
		} else {
			a.fail(w, r, http.StatusInternalServerError, "Unable to delete credential")
		}
		return
	}
	a.audit(r, "AdminDeleteCredential")
	http.Redirect(w, r, "/credentials", http.StatusSeeOther)
}
