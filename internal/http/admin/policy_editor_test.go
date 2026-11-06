package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"html/template"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/settings"
	"github.com/stretchr/testify/require"
)

type policyFormsHarness struct {
	t      *testing.T
	path   string
	mux    *http.ServeMux
	active *policy.Authorizer
}

func newPolicyFormsHarness(t *testing.T, kind, content string) *policyFormsHarness {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policies.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0600))
	configured, err := settings.LoadSettings([]string{"-authorizerType=" + kind, "-policyPath=" + path, "-authorizerPath=" + path})
	require.NoError(t, err)
	pages, err := template.New("layout").Parse(`{{define "layout"}}{{.Content}}{{end}}{{define "error"}}{{.Message}}{{end}}`)
	require.NoError(t, err)
	pages, err = pages.ParseFiles("templates/policies.html", "templates/policy_forms.html")
	require.NoError(t, err)
	h := &policyFormsHarness{t: t, path: path, mux: http.NewServeMux()}
	app := &App{options: Options{Settings: configured}, pages: pages}
	if kind == "policy" {
		h.active, err = policy.NewAuthorizer(path, 0)
		require.NoError(t, err)
		app.options.Authorizer = h.active
	}
	app.registerPolicies(h.mux)
	return h
}

func (h *policyFormsHarness) get(target string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, target, nil))
	return w
}

func (h *policyFormsHarness) file() []byte {
	data, err := os.ReadFile(h.path)
	require.NoError(h.t, err)
	return data
}

func (h *policyFormsHarness) parsed() *editablePolicies {
	policies, err := parsePolicies(h.file())
	require.NoError(h.t, err)
	return policies
}

// post submits form with the hash of the current file unless it sets one.
func (h *policyFormsHarness) post(form url.Values) *httptest.ResponseRecorder {
	if !form.Has("hash") {
		form.Set("hash", policyHash(h.file()))
	}
	request := httptest.NewRequest(http.MethodPost, "/policies/edit", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.mux.ServeHTTP(w, request)
	return w
}

func (h *policyFormsHarness) effect(account, principal, operation, bucket, key string) authorization.Effect {
	request := &authorization.Request{Operation: operation, Bucket: &bucket, Key: &key}
	if principal != "" {
		request.Authorization = authorization.Authorization{AccountId: &account, PrincipalId: &principal}
	}
	decision, err := h.active.AuthorizeRequest(context.Background(), request)
	require.NoError(h.t, err)
	return decision.Effect
}

func statementTestForm(action string, values url.Values) url.Values {
	form := url.Values{"policy_action": {action}, "effect": {"Allow"}, "action": {"s3:GetObject", " "}, "resource": {"arn:aws:s3:::demo/*"}}
	maps.Copy(form, values)
	return form
}

func TestPolicyFileRoundTripKeepsMeaning(t *testing.T) {
	source := `{"schemaVersion":1,"policies":{"b":{"Version":"2012-10-17","Id":"keep","Statement":{"Effect":"Allow","Action":["s3:Get*"],"Resource":"arn:aws:s3:::a&b/<x>*","Condition":{"ForAnyValue:StringLikeIfExists":{"s3:RequestObjectTagKeys":["a*","b"]},"Bool":{"aws:SecureTransport":"true"}}}},"a":{"Version":"2012-10-17","Statement":[{"Sid":"s","Effect":"Deny","Action":"s3:*","Resource":"*"}]}},"bindings":[{"policy":"b","subjects":[{"type":"anonymous"}]}]}`
	policies, err := parsePolicies([]byte(source))
	require.NoError(t, err)
	out, err := policies.marshal()
	require.NoError(t, err)
	_, err = policy.Compile(out)
	require.NoError(t, err)
	require.Contains(t, string(out), `"arn:aws:s3:::a&b/<x>*"`, "keep ARNs readable")
	require.Contains(t, string(out), `"Id": "keep"`)
	require.Less(t, strings.Index(string(out), `"a": {`), strings.Index(string(out), `"b": {`), "policies are sorted")
	again, err := parsePolicies(out)
	require.NoError(t, err)
	require.Equal(t, policies, again)

	view := statementFormFrom(policies.Policies["b"].Statements[0])
	require.Equal(t, []conditionRow{
		{Operator: "Bool", Modifier: "", Key: "aws:SecureTransport", Value: "true"},
		{Operator: "StringLike", Modifier: "ForAnyValue:IfExists", Key: "s3:RequestObjectTagKeys", Value: "a*"},
		{Operator: "StringLike", Modifier: "ForAnyValue:IfExists", Key: "s3:RequestObjectTagKeys", Value: "b"},
	}, view.Conditions)

	// Rendering the rows as a form and submitting them restores the statement.
	form := url.Values{"effect": {view.Effect}, "action": view.Actions, "resource": view.Resources}
	for _, row := range view.Conditions {
		form.Add("condition_operator", row.Operator)
		form.Add("condition_modifier", row.Modifier)
		form.Add("condition_key", row.Key)
		form.Add("condition_value", row.Value)
	}
	statement, err := statementFromForm(form)
	require.NoError(t, err)
	require.Equal(t, policies.Policies["b"].Statements[0], statement)
}

func TestStatementFormRejectsIncompleteInput(t *testing.T) {
	conditions := func(operator, modifier, key, value string) url.Values {
		return url.Values{"condition_operator": {operator}, "condition_modifier": {modifier}, "condition_key": {key}, "condition_value": {value}}
	}
	for name, tc := range map[string]struct {
		values url.Values
		err    string
	}{
		"effect":          {url.Values{"effect": {"Maybe"}}, "Allow or Deny"},
		"actions":         {url.Values{"action": {" ", ""}}, "at least one action"},
		"resources":       {url.Values{"resource": {""}}, "at least one resource"},
		"missing key":     {conditions("StringEquals", "", " ", "x"), "needs a key"},
		"operator":        {conditions("Regex", "", "aws:UserAgent", "x"), "unknown operator"},
		"modifier":        {conditions("StringEquals", "Sometimes", "aws:UserAgent", "x"), "unknown operator"},
		"incomplete rows": {url.Values{"condition_operator": {"StringEquals"}, "condition_key": {"aws:UserAgent"}}, "incomplete"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := statementFromForm(statementTestForm("save_statement", tc.values))
			require.ErrorContains(t, err, tc.err)
		})
	}
	statement, err := statementFromForm(statementTestForm("save_statement", conditions("StringEquals", "", "", "")))
	require.NoError(t, err)
	require.Nil(t, statement.Condition, "blank condition rows are ignored")
}

func TestPolicyFormsEditTheActiveFile(t *testing.T) {
	h := newPolicyFormsHarness(t, "policy", testPolicy)

	w := h.get("/policies")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `href="/policies?policy=read"`)
	require.Contains(t, w.Body.String(), "reader (account account)")

	w = h.post(statementTestForm("create", url.Values{"name": {" website "}, "sid": {"Public"}}))
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, "/policies?policy=website&saved=1", w.Header().Get("Location"))
	require.Equal(t, []policy.Statement{{Sid: "Public", Effect: "Allow", Action: []string{"s3:GetObject"}, Resource: []string{"arn:aws:s3:::demo/*"}}}, h.parsed().Policies["website"].Statements)
	backups, err := filepath.Glob(filepath.Join(filepath.Dir(h.path), ".policies.json.bak-*"))
	require.NoError(t, err)
	require.Len(t, backups, 1)
	require.Contains(t, h.get(w.Header().Get("Location")).Body.String(), "Policy saved and reloaded")

	require.Equal(t, authorization.ImplicitDeny, h.effect("", "", "GetObject", "demo", "a"))
	w = h.post(url.Values{"policy_action": {"add_subject"}, "policy": {"website"}, "subject_type": {"anonymous"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, authorization.Allow, h.effect("", "", "GetObject", "demo", "a"), "saving reloads the authorizer")
	w = h.post(url.Values{"policy_action": {"add_subject"}, "policy": {"website"}, "subject_type": {"anonymous"}})
	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	require.Contains(t, w.Body.String(), "already assigned")

	w = h.post(url.Values{"policy_action": {"add_subject"}, "policy": {"read"}, "subject_type": {"principal"}, "account": {"account"}, "principal": {"writer"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	bindings := h.parsed().Bindings
	require.Len(t, bindings, 2, "a new subject joins the existing binding")
	require.Equal(t, []policy.Subject{{Type: "principal", AccountID: "account", PrincipalID: "reader"}, {Type: "principal", AccountID: "account", PrincipalID: "writer"}}, bindings[0].Subjects)

	form := statementTestForm("save_statement", url.Values{"policy": {"read"}, "statement": {"0"}, "action": {"s3:GetObject", "s3:PutObject"},
		"condition_operator": {"IpAddress", "IpAddress"}, "condition_modifier": {"", ""}, "condition_key": {"aws:SourceIp", "aws:SourceIp"}, "condition_value": {"10.0.0.0/8", "192.0.2.1"}})
	w = h.post(form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	statements := h.parsed().Policies["read"].Statements
	require.Len(t, statements, 2)
	require.Equal(t, json.RawMessage(`["10.0.0.0/8","192.0.2.1"]`), statements[0].Condition["IpAddress"]["aws:SourceIp"])
	require.Equal(t, "deny", statements[1].Sid, "other statements are unchanged")

	w = h.get("/policies?policy=read&statement=0")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `value="192.0.2.1"`)
	require.Contains(t, h.get("/policies?policy=read").Body.String(), "IpAddress aws:SourceIp = 10.0.0.0/8")

	w = h.post(url.Values{"policy_action": {"rename"}, "policy": {"read"}, "name": {"readers"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	parsed := h.parsed()
	require.Contains(t, parsed.Policies, "readers")
	require.NotContains(t, parsed.Policies, "read")
	require.Equal(t, "readers", parsed.Bindings[0].Policy)

	w = h.post(url.Values{"policy_action": {"remove_subject"}, "policy": {"readers"}, "binding": {"0"}, "subject": {"0"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Len(t, h.parsed().Bindings[0].Subjects, 1)

	w = h.post(url.Values{"policy_action": {"delete_statement"}, "policy": {"website"}, "statement": {"0"}})
	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	require.Contains(t, w.Body.String(), "delete the policy instead")
	w = h.post(url.Values{"policy_action": {"delete_statement"}, "policy": {"readers"}, "statement": {"1"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Len(t, h.parsed().Policies["readers"].Statements, 1)

	w = h.post(url.Values{"policy_action": {"delete"}, "policy": {"website"}})
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	parsed = h.parsed()
	require.NotContains(t, parsed.Policies, "website")
	for _, binding := range parsed.Bindings {
		require.NotEqual(t, "website", binding.Policy, "deleting a policy removes its bindings")
	}
	w = h.post(url.Values{"policy_action": {"delete"}, "policy": {"readers"}})
	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	require.Contains(t, w.Body.String(), "at least one policy")
}

func TestPolicyFormsRejectInvalidAndStaleSubmissions(t *testing.T) {
	h := newPolicyFormsHarness(t, "policy", testPolicy)
	for name, form := range map[string]url.Values{
		"compiler":       statementTestForm("save_statement", url.Values{"policy": {"read"}, "statement": {"0"}, "condition_operator": {"DateLessThan"}, "condition_modifier": {""}, "condition_key": {"aws:CurrentTime"}, "condition_value": {"tomorrow"}}),
		"action":         statementTestForm("save_statement", url.Values{"policy": {"read"}, "action": {"s3:FlyToMoon"}}),
		"duplicate name": statementTestForm("create", url.Values{"name": {"read"}}),
		"empty name":     statementTestForm("create", url.Values{"name": {" "}}),
		"rename":         {"policy_action": {"rename"}, "policy": {"read"}, "name": {"bad\nname"}},
		"policy":         {"policy_action": {"delete"}, "policy": {"missing"}},
		"statement":      {"policy_action": {"delete_statement"}, "policy": {"read"}, "statement": {"7"}},
		"subject":        {"policy_action": {"remove_subject"}, "policy": {"read"}, "binding": {"0"}, "subject": {"3"}},
		"principal":      {"policy_action": {"add_subject"}, "policy": {"read"}, "subject_type": {"principal"}, "account": {"account"}},
		"action name":    {"policy_action": {"explode"}},
	} {
		t.Run(name, func(t *testing.T) {
			before := h.file()
			w := h.post(form)
			require.Equal(t, http.StatusUnprocessableEntity, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), "No changes were saved")
			require.Equal(t, before, h.file())
		})
	}
	w := h.post(statementTestForm("save_statement", url.Values{"policy": {"read"}, "sid": {"kept-sid"}, "condition_operator": {"DateLessThan"}, "condition_modifier": {""}, "condition_key": {"aws:CurrentTime"}, "condition_value": {"tomorrow"}}))
	require.Contains(t, w.Body.String(), `value="kept-sid"`, "a rejected statement stays in the editor")
	require.Contains(t, w.Body.String(), `value="tomorrow"`)

	w = h.post(url.Values{"policy_action": {"delete"}, "policy": {"read"}, "hash": {"stale"}})
	require.Equal(t, http.StatusConflict, w.Code)

	require.Equal(t, http.StatusNotFound, h.get("/policies?policy=missing").Code)
	require.Equal(t, http.StatusNotFound, h.get("/policies?policy=read&statement=9").Code)
}

func TestPolicyFormsNeedAValidJSONFile(t *testing.T) {
	h := newPolicyFormsHarness(t, "policy", testPolicy)
	require.NoError(t, os.WriteFile(h.path, []byte(`{"schemaVersion":2}`), 0600))
	w := h.get("/policies")
	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), "cannot be edited through forms")
	w = h.post(url.Values{"policy_action": {"delete"}, "policy": {"read"}})
	require.Equal(t, http.StatusConflict, w.Code)

	lua := newPolicyFormsHarness(t, "lua", `function authorizeRequest(r) return true end`)
	require.Contains(t, lua.get("/policies").Body.String(), "Lua script", "Lua keeps the code editor")
	require.Equal(t, http.StatusNotFound, lua.post(url.Values{"policy_action": {"delete"}}).Code)
}

func TestPolicyFormsTemplateEscapesInput(t *testing.T) {
	pages, err := template.ParseFiles("templates/policies.html", "templates/policy_forms.html")
	require.NoError(t, err)
	evil := `"><script>alert(1)</script>`
	for _, page := range []policyFormsPage{
		{CSRF: evil, Hash: evil, Invalid: evil, Notice: &policyNotice{"bad", evil}},
		{Policies: []policyListItem{{Name: evil, Subjects: []string{evil}}}},
		{Policy: evil, ID: evil, Detail: []statementSummary{{Sid: evil, Effect: evil, Actions: []string{evil}, Resources: []string{evil}, Conditions: []string{evil}}}, Subjects: []subjectView{{Type: evil, Account: evil, Principal: evil, AccessKey: evil}}, Identities: []simulatorIdentity{{evil, evil, evil}}},
		{Policy: evil, Actions: []string{evil}, ConditionKeys: []string{evil}, Statement: &statementForm{Create: true, Name: evil, Index: evil, Sid: evil, Effect: evil, Actions: []string{evil}, Resources: []string{evil}, Conditions: []conditionRow{{evil, evil, evil, evil}}}},
	} {
		var out bytes.Buffer
		require.NoError(t, pages.ExecuteTemplate(&out, "policy_forms", page))
		require.NotContains(t, out.String(), "<script>")
	}
}
