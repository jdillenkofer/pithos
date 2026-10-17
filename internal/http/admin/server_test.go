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
		{http.MethodGet, "/", http.StatusServiceUnavailable, "Login and management are not available yet."},
		{http.MethodGet, "/static/admin.css", http.StatusOK, "color-scheme"},
		{http.MethodGet, "/missing", http.StatusNotFound, "404"},
		{http.MethodGet, "/static/missing.js", http.StatusNotFound, "404"},
		{http.MethodGet, "/templates/layout.html", http.StatusNotFound, "404"},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed, "Method Not Allowed"},
		{http.MethodPost, "/", http.StatusMethodNotAllowed, "Method Not Allowed"},
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
	for _, path := range []string{"/buckets", "/objects", "/credentials", "/policies", "/audit", "/storage"} {
		for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodDelete} {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(method, path, nil))
			if w.Code != http.StatusNotFound {
				t.Errorf("%s %s: status = %d, want 404", method, path, w.Code)
			}
		}
	}
}
