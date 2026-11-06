package admin

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
)

const policyNameLimit = 128

// conditionModifiers are the prefixes and suffix a condition operator may
// carry, in the order the editor offers them.
var conditionModifiers = []string{"", "IfExists", "ForAnyValue:", "ForAnyValue:IfExists", "ForAllValues:", "ForAllValues:IfExists"}

// editablePolicies is a JSON policy file decoded far enough for forms to
// change single statements, policies and subjects and write it again.
type editablePolicies struct {
	Policies map[string]*editablePolicy
	Bindings []policy.Binding
}

type editablePolicy struct {
	ID         string
	Statements []policy.Statement
}

// parsePolicies accepts only files the authorizer would load, so a form never
// starts from a document the strict compiler rejects.
func parsePolicies(data []byte) (*editablePolicies, error) {
	if _, err := policy.Compile(data); err != nil {
		return nil, err
	}
	var file policy.File
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, err
	}
	policies := &editablePolicies{Policies: map[string]*editablePolicy{}, Bindings: file.Bindings}
	for name, document := range file.Policies {
		var statements []policy.Statement
		if raw := bytes.TrimSpace(document.Statement); len(raw) > 0 && raw[0] == '{' {
			var statement policy.Statement
			if err := json.Unmarshal(raw, &statement); err != nil {
				return nil, err
			}
			statements = []policy.Statement{statement}
		} else if err := json.Unmarshal(raw, &statements); err != nil {
			return nil, err
		}
		// Raw condition values keep the indentation of the file; compact them
		// so a file reads the same no matter how it was formatted.
		for _, statement := range statements {
			for _, entries := range statement.Condition {
				for key, raw := range entries {
					var compact bytes.Buffer
					if err := json.Compact(&compact, raw); err != nil {
						return nil, err
					}
					entries[key] = compact.Bytes()
				}
			}
		}
		policies.Policies[name] = &editablePolicy{ID: document.ID, Statements: statements}
	}
	return policies, nil
}

// marshal writes the file as indented JSON. Policies are sorted by name and
// every Statement is written as an array.
func (p *editablePolicies) marshal() ([]byte, error) {
	file := policy.File{SchemaVersion: 1, Policies: map[string]policy.Document{}, Bindings: p.Bindings}
	if file.Bindings == nil {
		file.Bindings = []policy.Binding{}
	}
	for name, editable := range p.Policies {
		statements, err := marshalUnescaped(editable.Statements, "")
		if err != nil {
			return nil, err
		}
		file.Policies[name] = policy.Document{Version: "2012-10-17", ID: editable.ID, Statement: statements}
	}
	return marshalUnescaped(file, "  ")
}

// marshalUnescaped keeps characters such as < and & readable in ARNs and
// condition values.
func marshalUnescaped(value any, indent string) ([]byte, error) {
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", indent)
	if err := encoder.Encode(value); err != nil {
		return nil, err
	}
	if indent == "" {
		return bytes.TrimSuffix(out.Bytes(), []byte("\n")), nil
	}
	return out.Bytes(), nil
}

func (p *editablePolicies) names() []string {
	names := make([]string, 0, len(p.Policies))
	for name := range p.Policies {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (p *editablePolicies) lookup(name string) (*editablePolicy, error) {
	editable, ok := p.Policies[name]
	if !ok {
		return nil, errors.New("policy not found; reload the page")
	}
	return editable, nil
}

func (p *editablePolicies) statementIndex(editable *editablePolicy, text string) (int, error) {
	index, err := strconv.Atoi(text)
	if err != nil || index < 0 || index >= len(editable.Statements) {
		return 0, errors.New("statement not found; reload the page")
	}
	return index, nil
}

// apply performs one form action and returns the page to show afterwards.
func (p *editablePolicies) apply(form url.Values) (string, error) {
	name := form.Get("policy")
	switch form.Get("policy_action") {
	case "create":
		newName, err := p.newPolicyName(form.Get("name"))
		if err != nil {
			return "", err
		}
		statement, err := statementFromForm(form)
		if err != nil {
			return "", err
		}
		p.Policies[newName] = &editablePolicy{Statements: []policy.Statement{statement}}
		return policyDetailLocation(newName), nil
	case "rename":
		editable, err := p.lookup(name)
		if err != nil {
			return "", err
		}
		newName := strings.TrimSpace(form.Get("name"))
		if newName == name {
			return policyDetailLocation(name), nil
		}
		if newName, err = p.newPolicyName(newName); err != nil {
			return "", err
		}
		delete(p.Policies, name)
		p.Policies[newName] = editable
		for i := range p.Bindings {
			if p.Bindings[i].Policy == name {
				p.Bindings[i].Policy = newName
			}
		}
		return policyDetailLocation(newName), nil
	case "delete":
		if _, err := p.lookup(name); err != nil {
			return "", err
		}
		if len(p.Policies) == 1 {
			return "", errors.New("the policy file needs at least one policy; edit this policy or use the JSON editor instead")
		}
		delete(p.Policies, name)
		p.Bindings = slices.DeleteFunc(p.Bindings, func(binding policy.Binding) bool { return binding.Policy == name })
		return "/policies", nil
	case "save_statement":
		editable, err := p.lookup(name)
		if err != nil {
			return "", err
		}
		statement, err := statementFromForm(form)
		if err != nil {
			return "", err
		}
		if form.Get("statement") == "" {
			editable.Statements = append(editable.Statements, statement)
		} else {
			index, err := p.statementIndex(editable, form.Get("statement"))
			if err != nil {
				return "", err
			}
			editable.Statements[index] = statement
		}
		return policyDetailLocation(name), nil
	case "delete_statement":
		editable, err := p.lookup(name)
		if err != nil {
			return "", err
		}
		index, err := p.statementIndex(editable, form.Get("statement"))
		if err != nil {
			return "", err
		}
		if len(editable.Statements) == 1 {
			return "", errors.New("a policy needs at least one statement; delete the policy instead")
		}
		editable.Statements = slices.Delete(editable.Statements, index, index+1)
		return policyDetailLocation(name), nil
	case "add_subject":
		if _, err := p.lookup(name); err != nil {
			return "", err
		}
		subject := policy.Subject{Type: form.Get("subject_type")}
		switch subject.Type {
		case "anonymous":
		case "principal":
			subject.AccountID, subject.PrincipalID = strings.TrimSpace(form.Get("account")), strings.TrimSpace(form.Get("principal"))
			if subject.AccountID == "" || subject.PrincipalID == "" {
				return "", errors.New("a principal needs an account ID and a principal ID")
			}
		default:
			return "", errors.New("unknown subject type")
		}
		for _, binding := range p.Bindings {
			if binding.Policy == name && slices.Contains(binding.Subjects, subject) {
				return "", errors.New("the policy is already assigned to this subject")
			}
		}
		for i := range p.Bindings {
			if p.Bindings[i].Policy == name {
				p.Bindings[i].Subjects = append(p.Bindings[i].Subjects, subject)
				return policyDetailLocation(name), nil
			}
		}
		p.Bindings = append(p.Bindings, policy.Binding{Policy: name, Subjects: []policy.Subject{subject}})
		return policyDetailLocation(name), nil
	case "remove_subject":
		binding, errBinding := strconv.Atoi(form.Get("binding"))
		subject, errSubject := strconv.Atoi(form.Get("subject"))
		if errBinding != nil || errSubject != nil || binding < 0 || binding >= len(p.Bindings) || p.Bindings[binding].Policy != name || subject < 0 || subject >= len(p.Bindings[binding].Subjects) {
			return "", errors.New("subject not found; reload the page")
		}
		p.Bindings[binding].Subjects = slices.Delete(p.Bindings[binding].Subjects, subject, subject+1)
		if len(p.Bindings[binding].Subjects) == 0 {
			p.Bindings = slices.Delete(p.Bindings, binding, binding+1)
		}
		return policyDetailLocation(name), nil
	}
	return "", errors.New("unknown policy action")
}

func (p *editablePolicies) newPolicyName(name string) (string, error) {
	name = strings.TrimSpace(name)
	switch {
	case name == "":
		return "", errors.New("enter a policy name")
	case len(name) > policyNameLimit:
		return "", fmt.Errorf("the policy name must be at most %d bytes", policyNameLimit)
	case strings.ContainsFunc(name, unicode.IsControl):
		return "", errors.New("the policy name must not contain control characters")
	}
	if _, ok := p.Policies[name]; ok {
		return "", fmt.Errorf("a policy named %q already exists", name)
	}
	return name, nil
}

func policyDetailLocation(name string) string {
	return "/policies?policy=" + url.QueryEscape(name)
}

func formLines(values []string) []string {
	var lines []string
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			lines = append(lines, value)
		}
	}
	return lines
}

// statementFromForm builds a statement from the editor rows. Condition rows
// with the same operator and key form one condition with several values.
func statementFromForm(form url.Values) (policy.Statement, error) {
	statement := policy.Statement{Sid: strings.TrimSpace(form.Get("sid")), Effect: form.Get("effect"), Action: formLines(form["action"]), Resource: formLines(form["resource"])}
	if statement.Effect != "Allow" && statement.Effect != "Deny" {
		return statement, errors.New("choose Allow or Deny as the effect")
	}
	if len(statement.Action) == 0 {
		return statement, errors.New("add at least one action")
	}
	if len(statement.Resource) == 0 {
		return statement, errors.New("add at least one resource")
	}
	operators, modifiers, keys, values := form["condition_operator"], form["condition_modifier"], form["condition_key"], form["condition_value"]
	if len(modifiers) != len(operators) || len(keys) != len(operators) || len(values) != len(operators) {
		return statement, errors.New("the condition rows are incomplete; reload the page")
	}
	conditions := map[string]map[string][]string{}
	for i, operator := range operators {
		key := strings.TrimSpace(keys[i])
		if key == "" && values[i] == "" {
			continue
		}
		if key == "" {
			return statement, fmt.Errorf("condition %d needs a key", i+1)
		}
		if !slices.Contains(policy.ConditionOperators(), operator) || !slices.Contains(conditionModifiers, modifiers[i]) {
			return statement, fmt.Errorf("condition %d has an unknown operator", i+1)
		}
		full := fullConditionOperator(operator, modifiers[i])
		if conditions[full] == nil {
			conditions[full] = map[string][]string{}
		}
		conditions[full][key] = append(conditions[full][key], values[i])
	}
	if len(conditions) > 0 {
		statement.Condition = map[string]map[string]json.RawMessage{}
		for operator, entries := range conditions {
			statement.Condition[operator] = map[string]json.RawMessage{}
			for key, list := range entries {
				var value any = list
				if len(list) == 1 {
					value = list[0]
				}
				raw, err := marshalUnescaped(value, "")
				if err != nil {
					return statement, err
				}
				statement.Condition[operator][key] = raw
			}
		}
	}
	return statement, nil
}

type policyFormsPage struct {
	CSRF, Hash string
	Notice     *policyNotice
	// Invalid explains why the active file cannot be edited through forms.
	Invalid   string
	Policies  []policyListItem
	Policy    string
	ID        string
	Detail    []statementSummary
	Subjects  []subjectView
	Statement *statementForm
	// Anonymous is true when the policy already applies to anonymous requests.
	Anonymous     bool
	Identities    []simulatorIdentity
	Actions       []string
	ConditionKeys []string
}

type policyListItem struct {
	Name       string
	Statements int
	Subjects   []string
}

type statementSummary struct {
	Index              int
	Sid, Effect        string
	Actions, Resources []string
	Conditions         []string
}

type subjectView struct {
	Binding, Index                      int
	Type, Account, Principal, AccessKey string
}

type statementForm struct {
	// Create is true when the statement is the first one of a new policy.
	Create             bool
	Name, Index        string
	Sid, Effect        string
	Actions, Resources []string
	Conditions         []conditionRow
}

type conditionRow struct{ Operator, Modifier, Key, Value string }

// valueRow is one repeatable action or resource input.
type valueRow struct{ Label, Name, Value, List, Placeholder string }

func (f *statementForm) ActionRows() []valueRow { return f.rows(f.Actions, f.BlankAction()) }
func (f *statementForm) ResourceRows() []valueRow {
	return f.rows(f.Resources, f.BlankResource())
}
func (f *statementForm) BlankAction() valueRow {
	return valueRow{"Action", "action", "", "policy-actions", "s3:GetObject"}
}
func (f *statementForm) BlankResource() valueRow {
	return valueRow{"Resource", "resource", "", "", "arn:aws:s3:::bucket/*"}
}
func (f *statementForm) BlankCondition() conditionRow { return conditionRow{} }

func (f *statementForm) rows(values []string, blank valueRow) []valueRow {
	rows := make([]valueRow, len(values))
	for i, value := range values {
		rows[i] = blank
		rows[i].Value = value
	}
	return rows
}

func (conditionRow) Operators() []string { return policy.ConditionOperators() }
func (conditionRow) Modifiers() []string { return conditionModifiers }

// fullConditionOperator joins a base operator with one of conditionModifiers.
func fullConditionOperator(base, modifier string) string {
	set, ifExists := strings.CutSuffix(modifier, "IfExists")
	if ifExists {
		return set + base + "IfExists"
	}
	return set + base
}

func statementFormFrom(statement policy.Statement) *statementForm {
	view := &statementForm{Sid: statement.Sid, Effect: statement.Effect, Actions: statement.Action, Resources: statement.Resource}
	for _, operator := range sortedKeys(statement.Condition) {
		set, rest, ok := strings.Cut(operator, ":")
		if ok {
			set += ":"
		} else {
			set, rest = "", operator
		}
		base, ifExists := strings.CutSuffix(rest, "IfExists")
		modifier := set
		if ifExists {
			modifier += "IfExists"
		}
		for _, key := range sortedKeys(statement.Condition[operator]) {
			for _, value := range conditionValues(statement.Condition[operator][key]) {
				view.Conditions = append(view.Conditions, conditionRow{Operator: base, Modifier: modifier, Key: key, Value: value})
			}
		}
	}
	return view
}

// statementFormFromInput keeps a rejected submission on screen.
func statementFormFromInput(form url.Values) *statementForm {
	view := &statementForm{Create: form.Get("policy_action") == "create", Name: form.Get("name"), Index: form.Get("statement"), Sid: form.Get("sid"), Effect: form.Get("effect"), Actions: form["action"], Resources: form["resource"]}
	operators, modifiers, keys, values := form["condition_operator"], form["condition_modifier"], form["condition_key"], form["condition_value"]
	for i := range operators {
		if i < len(modifiers) && i < len(keys) && i < len(values) {
			view.Conditions = append(view.Conditions, conditionRow{operators[i], modifiers[i], keys[i], values[i]})
		}
	}
	return view
}

func conditionValues(raw json.RawMessage) []string {
	var one string
	if json.Unmarshal(raw, &one) == nil {
		return []string{one}
	}
	var many []string
	_ = json.Unmarshal(raw, &many)
	return many
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func subjectLabel(subject policy.Subject) string {
	if subject.Type == "anonymous" {
		return "Anonymous requests"
	}
	return subject.PrincipalID + " (account " + subject.AccountID + ")"
}

// policyForms renders the policy list, one policy, or a statement editor.
func (a *App) policyForms(w http.ResponseWriter, r *http.Request, path string) {
	data, err := readPolicyFile(path)
	if err != nil {
		a.fail(w, r, 500, "Unable to read active policy file")
		return
	}
	page := a.newPolicyFormsPage(r, data)
	query := r.URL.Query()
	if query.Get("saved") != "" {
		page.Notice = &policyNotice{"ok", "Policy saved and reloaded. A backup of the previous file was written next to it."}
	}
	policies, err := parsePolicies(data)
	if err != nil {
		page.Invalid = err.Error()
		a.render(w, r, "Policies", "policy_forms", page)
		return
	}
	status, title := http.StatusOK, "Policies"
	if !a.fillPolicyFormsPage(&page, policies, query) {
		status, page.Notice = http.StatusNotFound, &policyNotice{"bad", "The policy or statement was not found."}
		a.fillPolicyFormsPage(&page, policies, url.Values{})
	}
	if page.Policy != "" || page.Statement != nil {
		title = "Policy: " + page.Policy
	}
	a.renderStatus(w, r, status, title, "policy_forms", page)
}

func (a *App) newPolicyFormsPage(r *http.Request, data []byte) policyFormsPage {
	actions := append([]string{"s3:*"}, policy.SupportedActions()...)
	sort.Strings(actions[1:])
	return policyFormsPage{CSRF: csrfToken(r), Hash: policyHash(data), Identities: a.simulatorIdentities(r.Context()), Actions: actions, ConditionKeys: append(policy.ConditionKeys(), "s3:ExistingObjectTag/", "s3:RequestObjectTag/")}
}

// fillPolicyFormsPage selects the view from the query. It returns false when
// the requested policy or statement does not exist.
func (a *App) fillPolicyFormsPage(page *policyFormsPage, policies *editablePolicies, query url.Values) bool {
	if query.Get("editor") == "create" {
		page.Statement = &statementForm{Create: true, Effect: "Allow", Actions: []string{""}, Resources: []string{""}}
		return true
	}
	name := query.Get("policy")
	if name == "" {
		for _, name := range policies.names() {
			item := policyListItem{Name: name, Statements: len(policies.Policies[name].Statements)}
			for _, binding := range policies.Bindings {
				if binding.Policy == name {
					for _, subject := range binding.Subjects {
						item.Subjects = append(item.Subjects, subjectLabel(subject))
					}
				}
			}
			page.Policies = append(page.Policies, item)
		}
		return true
	}
	editable, ok := policies.Policies[name]
	if !ok {
		return false
	}
	page.Policy, page.ID = name, editable.ID
	if text := query.Get("statement"); text != "" {
		if text == "new" {
			page.Statement = &statementForm{Name: name, Effect: "Allow", Actions: []string{""}, Resources: []string{""}}
			return true
		}
		index, err := policies.statementIndex(editable, text)
		if err != nil {
			return false
		}
		page.Statement = statementFormFrom(editable.Statements[index])
		page.Statement.Name, page.Statement.Index = name, text
		return true
	}
	for i, statement := range editable.Statements {
		summary := statementSummary{Index: i, Sid: statement.Sid, Effect: statement.Effect, Actions: statement.Action, Resources: statement.Resource}
		for _, row := range statementFormFrom(statement).Conditions {
			summary.Conditions = append(summary.Conditions, fullConditionOperator(row.Operator, row.Modifier)+" "+row.Key+" = "+row.Value)
		}
		page.Detail = append(page.Detail, summary)
	}
	accessKeys := map[[2]string][]string{}
	for _, identity := range page.Identities {
		id := [2]string{identity.Account, identity.Principal}
		accessKeys[id] = append(accessKeys[id], identity.AccessKey)
	}
	for i, binding := range policies.Bindings {
		if binding.Policy != name {
			continue
		}
		for j, subject := range binding.Subjects {
			page.Anonymous = page.Anonymous || subject.Type == "anonymous"
			page.Subjects = append(page.Subjects, subjectView{Binding: i, Index: j, Type: subject.Type, Account: subject.AccountID, Principal: subject.PrincipalID, AccessKey: strings.Join(accessKeys[[2]string{subject.AccountID, subject.PrincipalID}], ", ")})
		}
	}
	return true
}

// editPolicies applies one form action to the active JSON policy file and
// saves it through the same backup and reload path as the JSON editor.
func (a *App) editPolicies(w http.ResponseWriter, r *http.Request) {
	kind, path := a.policyLocation()
	if kind != "policy" {
		a.fail(w, r, http.StatusNotFound, "Policy forms are only available for JSON policies")
		return
	}
	if err := r.ParseForm(); err != nil {
		a.fail(w, r, http.StatusBadRequest, "Malformed policy form")
		return
	}
	data, err := readPolicyFile(path)
	if err != nil {
		a.fail(w, r, 500, "Unable to read active policy file")
		return
	}
	if r.PostForm.Get("hash") != policyHash(data) {
		a.audit(r, "AdminSavePolicyConflict")
		a.fail(w, r, http.StatusConflict, errPolicyConflict.Error())
		return
	}
	policies, err := parsePolicies(data)
	if err != nil {
		a.fail(w, r, http.StatusConflict, "The active policy file is invalid; fix it in the JSON editor first")
		return
	}
	location, err := policies.apply(r.PostForm)
	var candidate []byte
	if err == nil {
		candidate, err = policies.marshal()
	}
	if err == nil {
		_, err = validatePolicyCandidate("policy", candidate)
	}
	if err != nil {
		a.audit(r, "AdminPolicyValidationFailed")
		a.rejectPolicyForm(w, r, data, r.PostForm, err)
		return
	}
	if _, ok := a.savePolicy(w, r, kind, path, r.PostForm.Get("hash"), candidate); !ok {
		return
	}
	separator := "?"
	if strings.Contains(location, "?") {
		separator = "&"
	}
	http.Redirect(w, r, location+separator+"saved=1", http.StatusSeeOther)
}

// rejectPolicyForm shows the unchanged file again with the error, keeping a
// submitted statement in its editor.
func (a *App) rejectPolicyForm(w http.ResponseWriter, r *http.Request, data []byte, form url.Values, cause error) {
	page := a.newPolicyFormsPage(r, data)
	policies, err := parsePolicies(data)
	if err != nil {
		a.fail(w, r, http.StatusConflict, "The active policy file is invalid; fix it in the JSON editor first")
		return
	}
	query := url.Values{}
	if _, ok := policies.Policies[form.Get("policy")]; ok {
		query.Set("policy", form.Get("policy"))
	}
	a.fillPolicyFormsPage(&page, policies, query)
	switch form.Get("policy_action") {
	case "create", "save_statement":
		page.Statement = statementFormFromInput(form)
	}
	page.Notice = &policyNotice{"bad", "No changes were saved: " + cause.Error()}
	title := "Policies"
	if page.Policy != "" {
		title = "Policy: " + page.Policy
	}
	a.renderStatus(w, r, http.StatusUnprocessableEntity, title, "policy_forms", page)
}
