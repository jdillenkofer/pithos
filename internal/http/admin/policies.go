package admin

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	policylua "github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/ioutils"
)

const policySizeLimit = 1 << 20

// policyFormLimit bounds a URL-encoded policy form. Encoding may expand every
// candidate byte threefold; the remainder covers the simulator and CSRF fields.
const policyFormLimit = 3*policySizeLimit + 16384

// Shared by every admin handler in this process, including multiple listeners.
var policiesWriter sync.Mutex
var errPolicyConflict = errors.New("policy changed; reload the page before saving")

type policiesPage struct {
	CSRF, Kind, Candidate, Hash, Message                  string
	Account, Principal, AccessKey, Operation, Bucket, Key string
	Decision                                              *authorization.Decision
}

func (a *App) registerPolicies(mux *http.ServeMux) {
	mux.HandleFunc("GET /policies", a.policies)
	mux.HandleFunc("POST /policies/validate", a.policies)
	mux.HandleFunc("POST /policies/simulate", a.policies)
	mux.HandleFunc("POST /policies/save", a.policies)
}

func (a *App) policyLocation() (kind, path string) {
	if strings.EqualFold(a.options.Settings.AuthorizerType(), "policy") {
		return "policy", a.options.Settings.PolicyPath()
	}
	return "lua", a.options.Settings.AuthorizerPath()
}

func (a *App) policies(w http.ResponseWriter, r *http.Request) {
	kind, path := a.policyLocation()
	page := policiesPage{CSRF: csrfToken(r), Kind: kind}
	if r.Method == http.MethodGet {
		data, err := readPolicyFile(path)
		if err != nil {
			a.fail(w, r, 500, "Unable to read active policy file")
			return
		}
		page.Candidate, page.Hash = string(data), policyHash(data)
	} else {
		// ParseForm must never read an unbounded request, even for
		// validation-only submissions.
		r.Body = http.MaxBytesReader(w, r.Body, policyFormLimit)
		if err := r.ParseForm(); err != nil {
			a.fail(w, r, 413, "Policy form is too large or malformed")
			return
		}
		page.Candidate, page.Hash = r.PostForm.Get("candidate"), r.PostForm.Get("hash")
		page.Account, page.Principal, page.AccessKey = r.PostForm.Get("account"), r.PostForm.Get("principal"), r.PostForm.Get("accessKey")
		page.Operation, page.Bucket, page.Key = r.PostForm.Get("operation"), r.PostForm.Get("bucket"), r.PostForm.Get("key")
		candidate := []byte(page.Candidate)
		evaluator, err := validatePolicyCandidate(kind, candidate)
		if err != nil {
			page.Message = "Validation failed: " + err.Error()
			a.audit(r, "AdminPolicyValidationFailed")
		} else {
			switch r.URL.Path {
			case "/policies/validate":
				page.Message = "Policy is valid."
				a.audit(r, "AdminValidatePolicy")
			case "/policies/simulate":
				decision, err := simulatePolicy(r.Context(), evaluator, page)
				if err != nil {
					page.Message = "Simulation failed: " + err.Error()
				} else {
					page.Decision = &decision
					page.Message = "Simulation complete (candidate only; no policy changes)."
				}
				a.audit(r, "AdminSimulatePolicy")
			case "/policies/save":
				reload, ok := a.policyReloader(kind)
				if !ok {
					a.audit(r, "AdminSavePolicyFailed")
					a.fail(w, r, 503, "Active authorizer does not support immediate reload; no changes saved")
					return
				}
				backup, err := savePolicyCandidate(path, kind, page.Hash, candidate, reload)
				if errors.Is(err, errPolicyConflict) {
					a.audit(r, "AdminSavePolicyConflict")
					a.fail(w, r, 409, err.Error())
					return
				}
				if err != nil {
					a.audit(r, "AdminSavePolicyFailed")
					a.fail(w, r, 500, err.Error())
					return
				}
				page.Hash = policyHash(candidate)
				page.Message = "Policy saved and reloaded. Backup: " + backup
				a.audit(r, "AdminSavePolicy")
			}
		}
	}
	a.render(w, r, "Policies", "policies", page)
}

func (a *App) policyReloader(kind string) (func([]byte) error, bool) {
	if kind == "policy" {
		reloader, ok := a.options.Authorizer.(interface{ Reload() error })
		if !ok {
			return nil, false
		}
		return func(_ []byte) error { return reloader.Reload() }, true
	}
	reloader, ok := a.options.Authorizer.(interface{ ReloadCode(string) error })
	if !ok {
		return nil, false
	}
	return func(data []byte) error { return reloader.ReloadCode(string(data)) }, true
}

// Compile is the same strict JSON validation path used by validate-policy.
func validatePolicyCandidate(kind string, data []byte) (authorization.RequestAuthorizer, error) {
	if len(data) > policySizeLimit {
		return nil, fmt.Errorf("policy exceeds 1 MiB")
	}
	if kind == "policy" {
		return policy.Compile(data)
	}
	if kind != "lua" {
		return nil, fmt.Errorf("unsupported policy format")
	}
	return policylua.NewSimulator(string(data))
}

func simulatePolicy(ctx context.Context, evaluator authorization.RequestAuthorizer, p policiesPage) (authorization.Decision, error) {
	if p.Operation == "" {
		return authorization.Decision{}, fmt.Errorf("operation is required")
	}
	if (p.Account == "") != (p.Principal == "") {
		return authorization.Decision{}, fmt.Errorf("account and principal must be supplied together (leave both empty for anonymous)")
	}
	optional := func(s string) *string {
		if s == "" {
			return nil
		}
		return &s
	}
	request := &authorization.Request{Operation: p.Operation, Bucket: optional(p.Bucket), Key: optional(p.Key), Authorization: authorization.Authorization{AccountId: optional(p.Account), PrincipalId: optional(p.Principal), AccessKeyId: optional(p.AccessKey)}}
	return evaluator.AuthorizeRequest(ctx, request)
}

func policyHash(data []byte) string { return fmt.Sprintf("%x", sha256.Sum256(data)) }

func readPolicyFile(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("policy must be a regular file")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err = f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("policy must be a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(f, policySizeLimit+1))
	if err != nil {
		return nil, err
	}
	if len(data) > policySizeLimit {
		return nil, fmt.Errorf("policy exceeds 1 MiB")
	}
	return data, nil
}

func savePolicyCandidate(path, kind, expectedHash string, candidate []byte, reload func([]byte) error) (string, error) {
	policiesWriter.Lock()
	defer policiesWriter.Unlock()
	// Reject symlinks: replacing the link would edit a different file than the
	// configured authorizer previously read.
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", fmt.Errorf("policy path must be a regular file, not a symlink")
	}
	old, err := readPolicyFile(path)
	if err != nil {
		return "", err
	}
	if expectedHash == "" || expectedHash != policyHash(old) {
		return "", errPolicyConflict
	}
	if _, err = validatePolicyCandidate(kind, candidate); err != nil {
		return "", fmt.Errorf("validate candidate: %w", err)
	}
	backup, err := durablePolicyTemp(path, ".bak-"+time.Now().UTC().Format("20060102T150405.000000000Z")+"-*", old)
	if err != nil {
		return "", fmt.Errorf("backup policy: %w", err)
	}
	if err = syncPolicyDirectory(path); err != nil {
		return backup, fmt.Errorf("sync backup directory: %w", err)
	}
	if err = replacePolicyFile(path, candidate); err == nil {
		err = reload(candidate)
	}
	if err != nil {
		// A failed directory sync may occur after rename. Restore disk and runtime
		// together; do not claim rollback succeeded when either restoration fails.
		diskErr := replacePolicyFile(path, old)
		reloadErr := reload(old)
		return backup, fmt.Errorf("save/reload failed: %w; rollback disk: %v; rollback reload: %v; backup: %s", err, diskErr, reloadErr, backup)
	}
	return backup, nil
}

// CreateTemp uses exclusive creation and 0600, never overwriting a backup.
func durablePolicyTemp(path, suffix string, data []byte) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+suffix)
	if err != nil {
		return "", err
	}
	name := f.Name()
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(name)
		return "", err
	}
	return name, nil
}

func replacePolicyFile(path string, data []byte) error {
	temp, err := durablePolicyTemp(path, ".tmp-*", data)
	if err != nil {
		return err
	}
	defer os.Remove(temp)
	if err = os.Rename(temp, path); err != nil {
		return err
	}
	return syncPolicyDirectory(path)
}

func syncPolicyDirectory(path string) error {
	return ioutils.SyncDirectory(filepath.Dir(path))
}
