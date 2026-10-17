package admin

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRouter(t *testing.T) {
	handler, err := NewHandler()
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		method string
		path   string
		status int
		body   string
	}{
		{http.MethodGet, "/healthz", http.StatusOK, "ok\n"},
		{http.MethodGet, "/", http.StatusSeeOther, "/login"},
		{http.MethodGet, "/login", http.StatusOK, "Sign in"},
		{http.MethodGet, "/static/admin.css", http.StatusOK, "color-scheme"},
		{http.MethodGet, "/missing", http.StatusSeeOther, "/login"},
		{http.MethodGet, "/static/missing.js", http.StatusSeeOther, "/login"},
		{http.MethodGet, "/templates/layout.html", http.StatusSeeOther, "/login"},
		{http.MethodPost, "/healthz", http.StatusSeeOther, ""},
		{http.MethodPost, "/", http.StatusSeeOther, ""},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != tc.status {
				t.Errorf("status = %d, want %d", w.Code, tc.status)
			}
			if !strings.Contains(w.Body.String(), tc.body) {
				t.Errorf("body = %q, want to contain %q", w.Body.String(), tc.body)
			}
			for name, expected := range map[string]string{
				"Content-Security-Policy": contentSecurityPolicy,
				"X-Frame-Options":         "DENY",
				"X-Content-Type-Options":  "nosniff",
				"Referrer-Policy":         "same-origin",
				"Cache-Control":           "no-store",
			} {
				if got := w.Header().Get(name); got != expected {
					t.Errorf("%s = %q, want %q", name, got, expected)
				}
			}
		})
	}
}

func TestManagementRoutesAreNotExposed(t *testing.T) {
	handler, err := NewHandler()
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/", "/buckets", "/buckets/create", "/buckets/delete", "/buckets/detail", "/buckets/configure", "/objects", "/objects/detail", "/objects/download", "/objects/upload", "/objects/delete", "/objects/tags", "/objects/retention", "/objects/legal-hold", "/credentials", "/credentials/create", "/credentials/delete", "/policies", "/policies/save", "/policies/validate", "/policies/simulate", "/audit", "/audit/verify", "/audit/verify/progress", "/storage", "/storage/validate", "/storage/validate/progress", "/logout"} {
		for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(method, path, nil))
			if w.Code != http.StatusSeeOther || w.Header().Get("Location") != "/login" {
				t.Errorf("%s %s: status = %d, want login redirect", method, path, w.Code)
			}
		}
	}
}
