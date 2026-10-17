package admin

import (
	"bytes"
	"context"
	"errors"
	"github.com/stretchr/testify/require"
	"html/template"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	policylua "github.com/jdillenkofer/pithos/internal/http/server/authorization/lua"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/settings"
)

const testPolicy = `{"schemaVersion":1,"policies":{"read":{"Version":"2012-10-17","Statement":[{"Sid":"read","Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::demo/*"},{"Sid":"deny","Effect":"Deny","Action":"s3:GetObject","Resource":"arn:aws:s3:::demo/private/*"}]}},"bindings":[{"policy":"read","subjects":[{"type":"principal","accountId":"account","principalId":"reader"}]}]}`

func TestPolicyValidationAndSimulation(t *testing.T) {
	for _, candidate := range []string{`{}`, strings.Replace(testPolicy, `"schemaVersion":1`, `"schemaVersion":2`, 1), strings.Replace(testPolicy, `"Effect":"Allow"`, `"Effect":"Unknown"`, 1), strings.Repeat("x", policySizeLimit+1)} {
		if _, err := validatePolicyCandidate("policy", []byte(candidate)); err == nil {
			t.Fatal("invalid candidate accepted")
		}
	}
	evaluator, err := validatePolicyCandidate("policy", []byte(testPolicy))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		principal, key string
		effect         authorization.Effect
		sid            string
	}{
		{"reader", "public/a", authorization.Allow, "read"},
		{"reader", "private/a", authorization.ExplicitDeny, "deny"},
		{"unknown", "public/a", authorization.ImplicitDeny, ""},
	} {
		result, err := simulate(context.Background(), "policy", evaluator, simulationInput{Account: "account", Principal: tc.principal, AccessKey: "key", Operation: "GetObject", Bucket: "demo", Key: tc.key}, ownedBy("account"))
		_, title, _ := effectExplanation("policy", tc.effect)
		if err != nil || result.Title != title || result.Allowed != (tc.effect == authorization.Allow) {
			t.Fatalf("simulation = %+v, %v", result, err)
		}
		if tc.sid != "" && (len(result.References) == 0 || result.References[len(result.References)-1].Sid != tc.sid) {
			t.Fatalf("missing match: %+v", result)
		}
	}
}

func TestLuaPolicyValidationAndSimulation(t *testing.T) {
	for _, code := range []string{`not lua`, `function authorizeRequest(r) return "yes" end`, `while true do end`, `function authorizeRequest(r) while true do end end`, `function authorizeRequest(r) pcall(function() while true do end end) return true end`} {
		if _, err := validatePolicyCandidate("lua", []byte(code)); err == nil {
			t.Fatalf("accepted invalid/unbounded code: %s", code)
		}
	}
	evaluator, err := validatePolicyCandidate("lua", []byte(`function authorizeRequest(r) return r:principalIdEquals("reader") and r:isOperation("GetObject") and r:bucketEquals("demo") and r:keyHasPrefix("public/") end`))
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"public/a", "private/a"} {
		result, err := simulate(context.Background(), "lua", evaluator, simulationInput{Account: "account", Principal: "reader", AccessKey: "key", Operation: "GetObject", Bucket: "demo", Key: key}, ownedBy("account"))
		if err != nil || result.Allowed != (key == "public/a") {
			t.Fatalf("simulation = %+v, %v", result, err)
		}
	}
	// A branch not exercised by constructor dry-run must still be bounded.
	evaluator, err = validatePolicyCandidate("lua", []byte(`function authorizeRequest(r) if r:bucketEquals("loop") then while true do end end return true end`))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = simulate(context.Background(), "lua", evaluator, simulationInput{Operation: "GetObject", Bucket: "loop"}, ownedBy("account")); err == nil {
		t.Fatal("unbounded simulator")
	}
}

func TestPolicySaveBackupConflictAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	old := []byte(testPolicy)
	if err := os.WriteFile(path, old, 0644); err != nil {
		t.Fatal(err)
	}
	candidate := append(append([]byte{}, old...), '\n')
	calls := 0
	reload := func(data []byte) error {
		calls++
		actual, err := os.ReadFile(path)
		if err != nil || !bytes.Equal(actual, data) {
			t.Fatal("reload must see exact saved candidate")
		}
		return nil
	}
	backup, err := savePolicyCandidate(path, "policy", policyHash(old), candidate, reload)
	if err != nil {
		t.Fatal(err)
	}
	backupData, err := os.ReadFile(backup)
	if err != nil || !bytes.Equal(backupData, old) {
		t.Fatal("backup did not preserve old file")
	}
	// Windows has no POSIX permission bits; Go reports 0666 there.
	if runtime.GOOS != "windows" {
		for _, file := range []string{path, backup} {
			info, err := os.Stat(file)
			if err != nil || info.Mode().Perm() != 0600 {
				t.Fatalf("insecure permissions on %s", file)
			}
		}
	}
	if _, err = savePolicyCandidate(path, "policy", policyHash(old), old, reload); !errors.Is(err, errPolicyConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	if _, err = savePolicyCandidate(path, "policy", policyHash(candidate), []byte(`{}`), reload); err == nil {
		t.Fatal("saved invalid policy")
	}
	if calls != 1 {
		t.Fatalf("reload calls: %d", calls)
	}
	actual, _ := os.ReadFile(path)
	if !bytes.Equal(actual, candidate) {
		t.Fatal("failed save changed file")
	}
}

func TestPolicyReloadFailureRestoresFileAndRuntime(t *testing.T) {
	path := filepath.Join(t.TempDir(), "authorizer.lua")
	old := []byte(`function authorizeRequest(r) return true end`)
	candidate := []byte(`function authorizeRequest(r) return false end`)
	if err := os.WriteFile(path, old, 0600); err != nil {
		t.Fatal(err)
	}
	calls := 0
	backup, err := savePolicyCandidate(path, "lua", policyHash(old), candidate, func(data []byte) error {
		calls++
		if bytes.Equal(data, candidate) {
			return errors.New("reload unavailable")
		}
		if !bytes.Equal(data, old) {
			t.Fatal("wrong rollback candidate")
		}
		return nil
	})
	if err == nil || backup == "" || calls != 2 {
		t.Fatalf("backup=%s err=%v calls=%d", backup, err, calls)
	}
	actual, _ := os.ReadFile(path)
	if !bytes.Equal(actual, old) {
		t.Fatal("failed reload left new file on disk")
	}
}

func TestPolicyConcurrentSave(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	old := []byte(testPolicy)
	if err := os.WriteFile(path, old, 0600); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := savePolicyCandidate(path, "policy", policyHash(old), append(append([]byte{}, old...), '\n'), func([]byte) error { return nil })
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	successes, conflicts := 0, 0
	for err := range results {
		if err == nil {
			successes++
		} else if errors.Is(err, errPolicyConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("successes=%d conflicts=%d", successes, conflicts)
	}
}

func TestPoliciesTemplateEscapesCandidateAndCSRF(t *testing.T) {
	page, err := template.ParseFiles("templates/policies.html")
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = page.ExecuteTemplate(&out, "policies", policiesPage{Candidate: `</textarea><script>alert(1)</script>`, CSRF: `\"><script>`, Notice: &policyNotice{"bad", `<script>`}, Simulation: simulationInput{Account: `<script>`}, Operations: []string{`<script>`}, Identities: []simulatorIdentity{{AccessKey: `<script>`, Account: `<script>`, Principal: `<script>`}}, Result: &simulationResult{Title: `<script>`, Explanation: `<script>`, Context: []keyValue{{`<script>`, `<script>`}}, References: []authorization.StatementReference{{Policy: `<script>`, Sid: `<script>`}}}}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "<script>") || !strings.Contains(out.String(), `name="csrf_token"`) {
		t.Fatal("unsafe template or missing CSRF")
	}
}

func TestPolicyRoutes(t *testing.T) {
	for _, kind := range []string{"policy", "lua"} {
		t.Run(kind, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "policy")
			old, candidate := testPolicy, strings.Replace(testPolicy, `"Effect":"Allow"`, `"Effect":"Deny"`, 1)
			var active authorization.RequestAuthorizer
			var err error
			if kind == "lua" {
				old = `function authorizeRequest(r) return true end`
				candidate = `function authorizeRequest(r) return false end`
			}
			if err = os.WriteFile(path, []byte(old), 0600); err != nil {
				t.Fatal(err)
			}
			if kind == "lua" {
				active, err = policylua.NewLuaAuthorizer(old)
			} else {
				active, err = policy.NewAuthorizer(path, 0)
			}
			if err != nil {
				t.Fatal(err)
			}
			configured, err := settings.LoadSettings([]string{"-authorizerType=" + kind, "-policyPath=" + path, "-authorizerPath=" + path})
			if err != nil {
				t.Fatal(err)
			}
			pages, err := template.New("layout").Parse(`{{define "layout"}}{{.Content}}{{end}}`)
			if err != nil {
				t.Fatal(err)
			}
			pages, err = pages.ParseFiles("templates/policies.html")
			if err != nil {
				t.Fatal(err)
			}
			app := &App{options: Options{Settings: configured, Authorizer: active}, pages: pages}
			mux := http.NewServeMux()
			app.registerPolicies(mux)
			get := httptest.NewRecorder()
			mux.ServeHTTP(get, httptest.NewRequest("GET", "/policies", nil))
			if get.Code != 200 || !strings.Contains(get.Body.String(), policyHash([]byte(old))) {
				t.Fatalf("GET: %d %s", get.Code, get.Body.String())
			}
			submit := func(endpoint, hash, code string) *httptest.ResponseRecorder {
				form := url.Values{"candidate": {code}, "hash": {hash}, "operation": {"GetObject"}, "bucket": {"demo"}, "key": {"public/a"}, "account": {"account"}, "principal": {"reader"}, "accessKey": {"key"}}
				request := httptest.NewRequest("POST", endpoint, strings.NewReader(form.Encode()))
				request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
				recorder := httptest.NewRecorder()
				mux.ServeHTTP(recorder, request)
				return recorder
			}
			for _, endpoint := range []string{"/policies/validate", "/policies/simulate"} {
				result := submit(endpoint, policyHash([]byte(old)), candidate)
				if result.Code != 200 {
					t.Fatalf("%s: %d %s", endpoint, result.Code, result.Body.String())
				}
			}
			result := submit("/policies/save", policyHash([]byte(old)), candidate)
			if result.Code != 200 || !strings.Contains(result.Body.String(), "saved and reloaded") {
				t.Fatalf("save: %d %s", result.Code, result.Body.String())
			}
			bucket, key, account, principal := "demo", "public/a", "account", "reader"
			decision, err := active.AuthorizeRequest(context.Background(), &authorization.Request{Operation: "GetObject", Bucket: &bucket, Key: &key, Authorization: authorization.Authorization{AccountId: &account, PrincipalId: &principal}})
			expected := authorization.ExplicitDeny
			if err != nil || decision.Effect != expected {
				t.Fatalf("reload: %+v %v", decision, err)
			}
			result = submit("/policies/save", policyHash([]byte(old)), old)
			if result.Code != http.StatusConflict {
				t.Fatalf("conflict: %d", result.Code)
			}
		})
	}
}

func TestMaximumPolicyCandidatePassesWrappedRouter(t *testing.T) {
	h, _, _ := newSessionTestHandler(t)
	_, cookie := testLogin(t, h, "admin", "secret")
	if cookie == nil {
		t.Fatal("no session")
	}
	csrf := sessionCSRF(t, h, cookie)
	code := "function authorizeRequest(r) return true end\n"
	// Tabs are Lua whitespace and expand threefold when URL-encoded.
	post := func(candidate string) *httptest.ResponseRecorder {
		body := url.Values{"csrf_token": {csrf}, "candidate": {candidate}, "account": {"account"}, "operation": {"GetObject"}, "bucket": {"bucket"}, "key": {"key"}}.Encode()
		req := httptest.NewRequest(http.MethodPost, "/policies/validate", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(cookie)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	maximum := code + strings.Repeat("\t", policySizeLimit-len(code))
	if encoded := len(url.QueryEscape(maximum)); encoded <= 2*policySizeLimit {
		t.Fatalf("candidate does not exercise encoding overhead: %d bytes", encoded)
	}
	w := post(maximum)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "The policy is valid.") {
		t.Fatalf("maximum candidate rejected: %d %.300s", w.Code, w.Body.String())
	}
	w = post(maximum + "\t")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "policy exceeds 1 MiB") {
		t.Fatalf("oversized candidate accepted: %d %.300s", w.Code, w.Body.String())
	}
	w = post(maximum + strings.Repeat("\t", policyFormLimit/3))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unbounded form accepted: %d", w.Code)
	}
	if limit := csrfBodyLimit("/credentials/delete"); limit != 1024*1024 {
		t.Fatalf("ordinary form limit changed: %d", limit)
	}
}

// ownedBy resolves every bucket to owner, except "missing".
func ownedBy(owner string) bucketOwnerFunc {
	return func(_ context.Context, bucket string) (string, bool, error) {
		return owner, bucket != "missing", nil
	}
}

func TestSimulatorAppliesAccountBoundaryAndContext(t *testing.T) {
	evaluator, err := validatePolicyCandidate("policy", []byte(`{"schemaVersion":1,"policies":{"all":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:*","Resource":["arn:aws:s3:::*","arn:aws:s3:::*/*"],"Condition":{"StringEquals":{"aws:ResourceAccount":"owner","s3:authType":"REST-HEADER"}}}}},"bindings":[{"policy":"all","subjects":[{"type":"principal","accountId":"owner","principalId":"p"},{"type":"principal","accountId":"other","principalId":"p"}]}]}`))
	require.NoError(t, err)
	ctx := context.Background()

	result, err := simulate(ctx, "policy", evaluator, simulationInput{Account: "owner", Principal: "p", AccessKey: "key", Operation: "GetObject", Bucket: "bucket", Key: "k"}, ownedBy("owner"))
	require.NoError(t, err)
	require.True(t, result.Allowed, "the resolved owner and auth type satisfy the conditions: %+v", result)
	require.Equal(t, "arn:aws:s3:::bucket/k", result.Resource)

	result, err = simulate(ctx, "policy", evaluator, simulationInput{Account: "other", Principal: "p", AccessKey: "key", Operation: "GetObject", Bucket: "bucket", Key: "k"}, ownedBy("owner"))
	require.NoError(t, err)
	require.False(t, result.Allowed)
	require.Equal(t, "Denied by the account boundary", result.Title)
	require.NotEmpty(t, result.PolicyEffect)

	result, err = simulate(ctx, "policy", evaluator, simulationInput{Operation: "GetObject", Bucket: "bucket", Key: "k"}, ownedBy("owner"))
	require.NoError(t, err)
	require.False(t, result.Allowed, "anonymous callers have no binding")
	require.Equal(t, "Denied implicitly", result.Title)

	result, err = simulate(ctx, "policy", evaluator, simulationInput{Account: "owner", Principal: "p", AccessKey: "key", Operation: "CreateBucket", Bucket: "missing"}, ownedBy("owner"))
	require.NoError(t, err)
	require.True(t, result.Allowed, "a new bucket is owned by the caller: %+v", result)

	_, err = simulate(ctx, "policy", evaluator, simulationInput{Account: "owner", Principal: "p", AccessKey: "key", Operation: "GetObject", Bucket: "missing"}, ownedBy("owner"))
	require.ErrorContains(t, err, "NoSuchBucket")

	result, err = simulate(ctx, "policy", evaluator, simulationInput{Account: "owner", Principal: "p", AccessKey: "key", Operation: "CopyObject", Bucket: "bucket", Key: "k", SourceBucket: "source", SourceKey: "s"}, ownedBy("owner"))
	require.NoError(t, err)
	require.True(t, result.Allowed, "%+v", result)

	for _, in := range []simulationInput{
		{Operation: "s3:GetObject"},
		{Operation: "GetObject", Account: "owner"},
		{Operation: "GetObject", Account: "owner", Principal: "p"},
		{Operation: "GetObject", AccessKey: "key"},
		{Operation: "GetObject", Bucket: "Bad_Bucket"},
		{Operation: "GetObject", Key: "orphan"},
		{Operation: "CopyObject", Bucket: "bucket", Key: "k"},
	} {
		_, err := simulate(ctx, "policy", evaluator, in, ownedBy("owner"))
		require.Error(t, err, "%+v", in)
	}
}
