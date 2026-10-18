package admin

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/http/middleware"
	"github.com/jdillenkofer/pithos/internal/storage"
)

var corsMethods = []string{"GET", "PUT", "POST", "DELETE", "HEAD", "PATCH", "OPTIONS"}

type corsFormView struct {
	Index, ID, Origins, Headers, Expose, MaxAge string
	Methods                                     map[string]bool
}

func corsRuleForm(rule storage.CORSRule, index string) corsFormView {
	view := corsFormView{Index: index, Origins: strings.Join(rule.AllowedOrigins, "\n"), Headers: strings.Join(rule.AllowedHeaders, "\n"), Expose: strings.Join(rule.ExposeHeaders, "\n"), Methods: map[string]bool{}}
	if rule.ID != nil {
		view.ID = *rule.ID
	}
	if rule.MaxAgeSeconds != nil {
		view.MaxAge = strconv.Itoa(*rule.MaxAgeSeconds)
	}
	for _, method := range rule.AllowedMethods {
		view.Methods[strings.ToUpper(method)] = true
	}
	return view
}

func corsLines(text string) []string {
	var values []string
	for _, line := range strings.Split(text, "\n") {
		if value := strings.TrimSpace(line); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func corsRuleFromFields(form url.Values) (storage.CORSRule, error) {
	rule := storage.CORSRule{
		AllowedOrigins: corsLines(form.Get("origins")),
		AllowedMethods: form["methods"],
		AllowedHeaders: corsLines(form.Get("headers")),
		ExposeHeaders:  corsLines(form.Get("expose")),
	}
	if id := form.Get("id"); id != "" {
		rule.ID = &id
	}
	if text := form.Get("max_age"); text != "" {
		n, err := strconv.Atoi(text)
		if err != nil || n < 0 {
			return rule, errors.New("maximum age must be a nonnegative integer in seconds")
		}
		rule.MaxAgeSeconds = &n
	}
	return rule, nil
}

// corsFromForm edits a single rule while preserving the order and content of
// other rules. A snapshot hash prevents stale pages from targeting a new index.
func corsFromForm(ctx context.Context, st storage.Storage, name storage.BucketName, form url.Values) (string, string, error) {
	config, err := st.GetBucketCORSConfiguration(ctx, name)
	if err != nil && !errors.Is(err, storage.ErrNoSuchCORSConfiguration) {
		return "", "", err
	}
	if config == nil {
		config = &storage.BucketCORSConfiguration{}
	}
	if form.Get("cors_hash") != policyHash([]byte(configurationText(config))) {
		return "", "", errors.New("CORS rules changed; reload the page before saving")
	}
	if form.Get("action") == "delete" {
		return "", "delete", nil
	}
	// Copy the slice: a storage implementation may return a shared snapshot.
	rules := append([]storage.CORSRule(nil), config.Rules...)
	index := len(rules)
	if text := form.Get("rule_index"); text != "" {
		index, err = strconv.Atoi(text)
		if err != nil || index < 0 || index >= len(rules) {
			return "", "", errors.New("CORS rule not found; reload the page")
		}
	}
	switch form.Get("cors_action") {
	case "delete":
		if index == len(rules) {
			return "", "", errors.New("choose a CORS rule to delete")
		}
		rules = append(rules[:index], rules[index+1:]...)
		if len(rules) == 0 {
			return "", "delete", nil
		}
	case "save":
		rule, err := corsRuleFromFields(form)
		if err != nil {
			return "", "", err
		}
		if index == len(rules) {
			rules = append(rules, rule)
		} else {
			rules[index] = rule
		}
	default:
		return "", "", fmt.Errorf("unknown CORS rule action")
	}
	normalized, err := middleware.NormalizeAndValidateCORSRules(rules)
	if err != nil {
		return "", "", err
	}
	return mustMarshal(storage.BucketCORSConfiguration{Rules: normalized})
}
