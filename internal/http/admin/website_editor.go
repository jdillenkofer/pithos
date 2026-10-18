package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/storage"
)

type websiteRoutingFormView struct {
	Index, Position, MaxPosition                              string
	Prefix, ErrorCode, Host, Protocol, Replacement, Key, Code string
	PrefixSet, ErrorSet, HostSet                              bool
}

type websiteRoutingView struct {
	Index, Position, Condition, Redirect, Code string
	CanMoveUp, CanMoveDown                     bool
}

func websiteRoutingRuleForm(rule storage.WebsiteRoutingRule, index string, count int) websiteRoutingFormView {
	view := websiteRoutingFormView{Index: index, Position: strconv.Itoa(count + 1), MaxPosition: strconv.Itoa(count + 1), Replacement: "unchanged", Code: "301"}
	if index != "" {
		n, _ := strconv.Atoi(index)
		view.Position, view.MaxPosition = strconv.Itoa(n+1), strconv.Itoa(count)
	}
	if condition := rule.Condition; condition != nil {
		if condition.KeyPrefixEquals != nil {
			view.PrefixSet, view.Prefix = true, *condition.KeyPrefixEquals
		}
		if condition.HttpErrorCodeReturnedEquals != nil {
			view.ErrorSet, view.ErrorCode = true, *condition.HttpErrorCodeReturnedEquals
		}
	}
	redirect := rule.Redirect
	if redirect.HostName != nil {
		view.HostSet, view.Host = true, *redirect.HostName
	}
	if redirect.Protocol != nil {
		view.Protocol = *redirect.Protocol
		if view.Protocol == "" {
			view.Protocol = "request"
		}
	}
	if redirect.ReplaceKeyWith != nil {
		view.Replacement, view.Key = "key", *redirect.ReplaceKeyWith
	} else if redirect.ReplaceKeyPrefixWith != nil {
		view.Replacement, view.Key = "prefix", *redirect.ReplaceKeyPrefixWith
	}
	if redirect.HttpRedirectCode != nil && *redirect.HttpRedirectCode != "" {
		view.Code = *redirect.HttpRedirectCode
	}
	return view
}

func describeWebsiteRouting(config *storage.WebsiteConfiguration) []websiteRoutingView {
	if config == nil {
		return nil
	}
	var views []websiteRoutingView
	for i, rule := range config.RoutingRules {
		form := websiteRoutingRuleForm(rule, strconv.Itoa(i), len(config.RoutingRules))
		var conditions, redirects []string
		if form.PrefixSet {
			conditions = append(conditions, "Key starts with "+strconv.Quote(form.Prefix))
		}
		if form.ErrorSet {
			conditions = append(conditions, "HTTP error "+form.ErrorCode)
		}
		if len(conditions) == 0 {
			conditions = append(conditions, "All requests")
		}
		if form.HostSet {
			host := form.Host
			if host == "" {
				host = "request host (absolute URL)"
			}
			redirects = append(redirects, "Host: "+host)
		}
		if form.Protocol != "" {
			redirects = append(redirects, "Protocol: "+form.Protocol)
		}
		switch form.Replacement {
		case "key":
			redirects = append(redirects, "Replace key with "+strconv.Quote(form.Key))
		case "prefix":
			redirects = append(redirects, "Replace prefix with "+strconv.Quote(form.Key))
		default:
			redirects = append(redirects, "Keep object key")
		}
		views = append(views, websiteRoutingView{
			Index: form.Index, Position: form.Position, Condition: strings.Join(conditions, " and "),
			Redirect: strings.Join(redirects, "; "), Code: form.Code,
			CanMoveUp: i > 0, CanMoveDown: i < len(config.RoutingRules)-1,
		})
	}
	return views
}

func websiteRoutingRuleFromFields(form url.Values) (storage.WebsiteRoutingRule, error) {
	rule := storage.WebsiteRoutingRule{}
	condition := &storage.WebsiteRoutingRuleCondition{}
	if form.Get("match_prefix") == "enabled" {
		prefix := form.Get("key_prefix")
		condition.KeyPrefixEquals = &prefix
	}
	if form.Get("match_error") == "enabled" {
		code := form.Get("error_code")
		n, err := strconv.Atoi(code)
		if err != nil || len(code) != 3 || n < 400 || n > 599 {
			return rule, errors.New("enter an HTTP error status between 400 and 599")
		}
		condition.HttpErrorCodeReturnedEquals = &code
	}
	if condition.KeyPrefixEquals != nil || condition.HttpErrorCodeReturnedEquals != nil {
		rule.Condition = condition
	}
	if form.Get("change_host") == "enabled" {
		host := strings.TrimSpace(form.Get("redirect_host"))
		rule.Redirect.HostName = &host
	}
	switch protocol := form.Get("redirect_protocol"); protocol {
	case "":
	case "request":
		empty := ""
		rule.Redirect.Protocol = &empty
	case "http", "https":
		rule.Redirect.Protocol = &protocol
	default:
		return rule, errors.New("choose HTTP, HTTPS or an unchanged protocol")
	}
	key := form.Get("replacement_key")
	switch form.Get("replacement") {
	case "unchanged":
	case "prefix":
		rule.Redirect.ReplaceKeyPrefixWith = &key
	case "key":
		rule.Redirect.ReplaceKeyWith = &key
	default:
		return rule, errors.New("choose how the object key is redirected")
	}
	if rule.Redirect.HostName == nil && rule.Redirect.Protocol == nil && rule.Redirect.ReplaceKeyWith == nil && rule.Redirect.ReplaceKeyPrefixWith == nil {
		return rule, errors.New("change the host, protocol or object key for the redirect")
	}
	code := form.Get("redirect_code")
	switch code {
	case "301", "302", "303", "307", "308":
		rule.Redirect.HttpRedirectCode = &code
	default:
		return rule, errors.New("choose a redirect status of 301, 302, 303, 307 or 308")
	}
	return rule, nil
}

// websiteFromForm keeps hosting settings and ordered routing rules in the same
// configuration. A failed read never turns a partial form into a replacement.
func websiteFromForm(ctx context.Context, st storage.Storage, name storage.BucketName, form url.Values) (string, string, error) {
	current, err := st.GetBucketWebsiteConfiguration(ctx, name)
	if err != nil && !errors.Is(err, storage.ErrNoSuchWebsiteConfiguration) {
		return "", "", err
	}
	if current == nil {
		current = &storage.WebsiteConfiguration{}
	}
	// The rule editor always requires a snapshot; the legacy hosting submission
	// format remains accepted without one, but the UI includes it on every form.
	if form.Has("website_hash") || form.Has("website_rule_action") {
		if form.Get("website_hash") != policyHash([]byte(configurationText(current))) {
			return "", "", errors.New("website configuration changed; reload the page before saving")
		}
	}
	if form.Has("website_rule_action") {
		return websiteRoutingFromForm(current, form)
	}
	if form.Get("website") != "enabled" {
		return "", "delete", nil
	}
	if form.Get("hosting") == "redirect" {
		if len(current.RoutingRules) > 0 && form.Get("discard_routing_rules") != "yes" {
			return "", "", errors.New("confirm removal of existing redirection rules before redirecting all requests")
		}
		redirect := &storage.WebsiteRedirectAllRequestsTo{HostName: strings.TrimSpace(form.Get("redirect_host"))}
		if protocol := form.Get("redirect_protocol"); protocol != "" {
			redirect.Protocol = &protocol
		}
		return mustMarshal(storage.WebsiteConfiguration{RedirectAllRequestsTo: redirect})
	}
	config := storage.WebsiteConfiguration{IndexDocumentSuffix: strings.TrimSpace(form.Get("index_document")), RoutingRules: current.RoutingRules}
	if errorDocument := strings.TrimSpace(form.Get("error_document")); errorDocument != "" {
		config.ErrorDocumentKey = &errorDocument
	}
	return mustMarshal(config)
}

// websiteRoutingSaveConfirmation reviews the resulting rule at its new position.
func websiteRoutingSaveConfirmation(bucket, text string, form url.Values) confirmation {
	var config storage.WebsiteConfiguration
	_ = json.Unmarshal([]byte(text), &config)
	position, _ := strconv.Atoi(form.Get("position"))
	view := describeWebsiteRouting(&config)[position-1]
	c := confirmation{Title: "Review website redirection rule", Action: "Create rule", Review: true,
		Details: []keyValue{{"Bucket", bucket}, {"Position", view.Position}, {"Condition", view.Condition}, {"Redirect", view.Redirect}, {"HTTP status", view.Code}},
	}
	if form.Get("rule_index") != "" {
		c.Action = "Save rule"
	}
	var warnings []string
	if websiteRuleMatchesAll(config.RoutingRules[position-1]) {
		warnings = append(warnings, "This rule matches every request. Rules are evaluated in order; later rules will not be reached.")
	}
	for _, earlier := range config.RoutingRules[:position-1] {
		if websiteRuleMatchesAll(earlier) {
			warnings = append(warnings, "An earlier rule matches every request, so this rule will not be reached. Move this rule before it if needed.")
			break
		}
	}
	c.Warning = strings.Join(warnings, " ")
	return c
}

func websiteRuleMatchesAll(rule storage.WebsiteRoutingRule) bool {
	return rule.Condition == nil || (rule.Condition.HttpErrorCodeReturnedEquals == nil && (rule.Condition.KeyPrefixEquals == nil || *rule.Condition.KeyPrefixEquals == ""))
}

func websiteRoutingFromForm(current *storage.WebsiteConfiguration, form url.Values) (string, string, error) {
	if current.RedirectAllRequestsTo != nil || current.IndexDocumentSuffix == "" {
		return "", "", errors.New("enable static website hosting before editing redirection rules")
	}
	config := *current
	config.RoutingRules = append([]storage.WebsiteRoutingRule(nil), current.RoutingRules...)
	action := form.Get("website_rule_action")
	index := -1
	if text := form.Get("rule_index"); text != "" {
		var err error
		index, err = strconv.Atoi(text)
		if err != nil || index < 0 || index >= len(config.RoutingRules) {
			return "", "", errors.New("redirection rule not found; reload the page")
		}
	}
	switch action {
	case "save":
		rule, err := websiteRoutingRuleFromFields(form)
		if err != nil {
			return "", "", err
		}
		if index >= 0 {
			config.RoutingRules = append(config.RoutingRules[:index], config.RoutingRules[index+1:]...)
		}
		position, err := strconv.Atoi(form.Get("position"))
		if err != nil || position < 1 || position > len(config.RoutingRules)+1 {
			return "", "", fmt.Errorf("rule position must be between 1 and %d", len(config.RoutingRules)+1)
		}
		config.RoutingRules = append(config.RoutingRules, storage.WebsiteRoutingRule{})
		copy(config.RoutingRules[position:], config.RoutingRules[position-1:])
		config.RoutingRules[position-1] = rule
	case "delete":
		if index < 0 {
			return "", "", errors.New("choose a redirection rule to delete")
		}
		config.RoutingRules = append(config.RoutingRules[:index], config.RoutingRules[index+1:]...)
	case "up", "down":
		target := index - 1
		if action == "down" {
			target = index + 1
		}
		if index < 0 || target < 0 || target >= len(config.RoutingRules) {
			return "", "", errors.New("redirection rule cannot be moved further in that direction")
		}
		config.RoutingRules[index], config.RoutingRules[target] = config.RoutingRules[target], config.RoutingRules[index]
	case "delete_all":
		config.RoutingRules = nil
	default:
		return "", "", errors.New("unknown website redirection rule action")
	}
	return mustMarshal(config)
}
