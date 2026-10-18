package admin

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/jdillenkofer/pithos/internal/storage"
)

// Structured editors post editor=form. Their fields are translated into typed
// configuration documents, so every change goes through
// bucketConfigurationMutation and its S3 validation.

// parseTagRows reads the tag editor's repeated tag_key/tag_value fields.
// Rows with an empty key and value are ignored, so blank rows can be left.
func parseTagRows(form url.Values) (map[string]string, error) {
	keys, values := form["tag_key"], form["tag_value"]
	if len(keys) != len(values) {
		return nil, errors.New("every tag needs a key and a value field")
	}
	tags := make(map[string]string, len(keys))
	for i, key := range keys {
		key, value := strings.TrimSpace(key), values[i]
		if key == "" && value == "" {
			continue
		}
		if key == "" {
			return nil, errors.New("tag keys must not be empty")
		}
		if _, duplicate := tags[key]; duplicate {
			return nil, fmt.Errorf("tag key %q is used more than once", key)
		}
		tags[key] = value
	}
	return tags, nil
}

func sortedTags(tags map[string]string) []keyValue {
	rows := make([]keyValue, 0, len(tags))
	for key, value := range tags {
		rows = append(rows, keyValue{key, value})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Label < rows[j].Label })
	return rows
}

func positiveInt32(form url.Values, field, label string) (*int32, error) {
	text := strings.TrimSpace(form.Get(field))
	if text == "" {
		return nil, nil
	}
	value, err := strconv.ParseInt(text, 10, 32)
	if err != nil || value < 1 {
		return nil, fmt.Errorf("%s must be a whole number of at least 1", label)
	}
	v := int32(value)
	return &v, nil
}

func marshalConfiguration(config any) (string, error) {
	data, err := json.Marshal(config)
	return string(data), err
}

// structuredBucketConfiguration converts a structured editor submission into
// the configuration text and action for bucketConfigurationMutation.
func structuredBucketConfiguration(ctx context.Context, st storage.Storage, name storage.BucketName, tab string, form url.Values) (string, string, error) {
	switch tab {
	case "versioning":
		status := storage.BucketVersioningStatus(form.Get("status"))
		return mustMarshal(storage.BucketVersioningConfiguration{Status: &status})
	case "tags":
		tags, err := parseTagRows(form)
		if err != nil {
			return "", "", err
		}
		if len(tags) == 0 {
			return "", "delete", nil
		}
		return mustMarshal(tags)
	case "object-lock":
		config := storage.ObjectLockConfiguration{ObjectLockEnabled: "Enabled"}
		if form.Get("default_retention") == "enabled" {
			period, err := positiveInt32(form, "period", "Default retention period")
			if err != nil {
				return "", "", err
			}
			if period == nil {
				return "", "", errors.New("enter a default retention period")
			}
			retention := &storage.DefaultRetention{Mode: storage.RetentionMode(form.Get("mode"))}
			if form.Get("unit") == "Years" {
				retention.Years = period
			} else {
				retention.Days = period
			}
			config.DefaultRetention = retention
		}
		return mustMarshal(config)
	case "website":
		if form.Get("website") != "enabled" {
			return "", "delete", nil
		}
		if form.Get("hosting") == "redirect" {
			redirect := &storage.WebsiteRedirectAllRequestsTo{HostName: strings.TrimSpace(form.Get("redirect_host"))}
			if protocol := form.Get("redirect_protocol"); protocol != "" {
				redirect.Protocol = &protocol
			}
			return mustMarshal(storage.WebsiteConfiguration{RedirectAllRequestsTo: redirect})
		}
		config := storage.WebsiteConfiguration{IndexDocumentSuffix: strings.TrimSpace(form.Get("index_document"))}
		if errorDocument := strings.TrimSpace(form.Get("error_document")); errorDocument != "" {
			config.ErrorDocumentKey = &errorDocument
		}
		// Routing rules have no form fields; keep any configured ones.
		if current, err := st.GetBucketWebsiteConfiguration(ctx, name); err == nil && current != nil {
			config.RoutingRules = current.RoutingRules
		}
		return mustMarshal(config)
	case "notifications":
		return notificationFromForm(ctx, st, name, form)
	case "cors":
		return corsFromForm(ctx, st, name, form)
	case "lifecycle":
		return lifecycleFromForm(ctx, st, name, form)
	}
	return "", "", errors.New("this configuration has no form editor")
}

func mustMarshal(config any) (string, string, error) {
	text, err := marshalConfiguration(config)
	return text, "save", err
}

// lifecycleFromForm adds, replaces or deletes one rule of the current
// lifecycle configuration.
func lifecycleFromForm(ctx context.Context, st storage.Storage, name storage.BucketName, form url.Values) (string, string, error) {
	var rules []storage.LifecycleRule
	current, err := st.GetBucketLifecycleConfiguration(ctx, name)
	if err == nil && current != nil {
		rules = current.Rules
	} else if err != nil && !errors.Is(err, storage.ErrNoSuchLifecycleConfiguration) {
		return "", "", err
	}
	original := form.Get("original_id")
	kept := rules[:0:0]
	for i, rule := range rules {
		match := original != "" && rule.ID != nil && *rule.ID == original
		if index := form.Get("original_index"); index != "" {
			match = index == strconv.Itoa(i)
		}
		if !match {
			kept = append(kept, rule)
		}
	}
	if form.Get("rule_action") == "delete" {
		if len(kept) == len(rules) {
			return "", "", errors.New("lifecycle rule not found")
		}
		if len(kept) == 0 {
			return "", "delete", nil
		}
		return mustMarshal(storage.BucketLifecycleConfiguration{Rules: kept})
	}
	if (original != "" || form.Get("original_index") != "") && len(kept) == len(rules) {
		return "", "", errors.New("lifecycle rule not found")
	}

	id := strings.TrimSpace(form.Get("rule_id"))
	if id == "" && form.Get("original_index") == "" {
		return "", "", errors.New("enter a lifecycle rule name")
	}
	for _, rule := range kept {
		if rule.ID != nil && *rule.ID == id {
			return "", "", fmt.Errorf("a lifecycle rule named %q already exists", id)
		}
	}
	status := storage.LifecycleRuleStatusEnabled
	if value := form.Get("status"); value != "" && value != storage.LifecycleRuleStatusEnabled && value != storage.LifecycleRuleStatusDisabled {
		return "", "", errors.New("choose Enabled or Disabled for the lifecycle rule status")
	}
	if form.Get("status") == storage.LifecycleRuleStatusDisabled {
		status = storage.LifecycleRuleStatusDisabled
	}
	rule, err := lifecycleRuleFromFields(form)
	if err != nil {
		return "", "", err
	}
	rule.ID, rule.Status = &id, status
	if id == "" {
		rule.ID = nil
	}
	if days, err := positiveInt32(form, "expire_days", "Expire current versions after"); err != nil {
		return "", "", err
	} else if days != nil {
		rule.Expiration = &storage.LifecycleExpiration{Days: days}
	}
	if form.Get("expired_delete_markers") == "true" {
		if rule.Expiration != nil && rule.Expiration.ExpiredObjectDeleteMarker == nil {
			return "", "", errors.New("expired object delete markers cannot be removed in a rule that also expires current versions")
		}
		enabled := true
		rule.Expiration = &storage.LifecycleExpiration{ExpiredObjectDeleteMarker: &enabled}
	}
	if days, err := positiveInt32(form, "noncurrent_days", "Delete noncurrent versions after"); err != nil {
		return "", "", err
	} else if days != nil {
		rule.NoncurrentVersionExpiration = &storage.LifecycleNoncurrentVersionExpiration{NoncurrentDays: days}
		if newer, err := positiveInt32(form, "newer_noncurrent", "Number of newer versions to retain"); err != nil {
			return "", "", err
		} else {
			rule.NoncurrentVersionExpiration.NewerNoncurrentVersions = newer
		}
	}
	if days, err := positiveInt32(form, "abort_days", "Delete incomplete multipart uploads after"); err != nil {
		return "", "", err
	} else if days != nil {
		rule.AbortIncompleteMultipartUpload = &storage.LifecycleAbortIncompleteMultipartUpload{DaysAfterInitiation: days}
	}
	if rule.Expiration == nil && rule.NoncurrentVersionExpiration == nil && rule.AbortIncompleteMultipartUpload == nil && len(rule.Transitions) == 0 && len(rule.NoncurrentVersionTransitions) == 0 {
		return "", "", errors.New("choose at least one lifecycle rule action")
	}
	return mustMarshal(storage.BucketLifecycleConfiguration{Rules: append(kept, rule)})
}

// View models for the S3-console-style bucket tabs.

type objectLockView struct {
	Enabled, HasDefault bool
	Mode, Period, Unit  string
}

type websiteView struct {
	Enabled, Redirect            bool
	Index, Error, Host, Protocol string
	RoutingRules                 int
}

type notificationView struct {
	ID, Type, Destination, Events, Prefix, Suffix string
	Kind, Index                                   string
}

type corsView struct {
	ID, Origins, Methods, Headers, Expose, MaxAge string
	Index                                         string
}

type lifecycleView struct {
	ID, Status, Scope, Current, Noncurrent, Other string
	EditURL                                       string
	Index                                         string
}

type lifecycleFormView struct {
	OriginalID, OriginalIndex, ID, Status, Prefix                  string
	ExpireDays, ExpireDate, NoncurrentDays, NewerNoncurrent, Abort string
	SizeGreater, SizeLess, MarkerMode                              string
	Tags                                                           []keyValue
	Transitions, NoncurrentTransitions                             []lifecycleTransitionForm
	BlankTransition                                                lifecycleTransitionForm
	BlankTag                                                       keyValue
}

func int32Text(value *int32) string {
	if value == nil {
		return ""
	}
	return strconv.Itoa(int(*value))
}

func joinOrDash(values []string) string {
	if len(values) == 0 {
		return "-"
	}
	return strings.Join(values, ", ")
}

func describeObjectLock(config *storage.ObjectLockConfiguration) objectLockView {
	view := objectLockView{Unit: "Days", Mode: "GOVERNANCE"}
	if config == nil || config.ObjectLockEnabled != "Enabled" {
		return view
	}
	view.Enabled = true
	if retention := config.DefaultRetention; retention != nil {
		view.HasDefault, view.Mode = true, string(retention.Mode)
		if retention.Years != nil {
			view.Period, view.Unit = int32Text(retention.Years), "Years"
		} else {
			view.Period = int32Text(retention.Days)
		}
	}
	return view
}

func describeWebsite(config *storage.WebsiteConfiguration) websiteView {
	view := websiteView{Index: "index.html", Protocol: ""}
	if config == nil {
		return view
	}
	view.Enabled = true
	view.RoutingRules = len(config.RoutingRules)
	if redirect := config.RedirectAllRequestsTo; redirect != nil {
		view.Redirect, view.Host = true, redirect.HostName
		if redirect.Protocol != nil {
			view.Protocol = *redirect.Protocol
		}
		return view
	}
	view.Index = config.IndexDocumentSuffix
	if config.ErrorDocumentKey != nil {
		view.Error = *config.ErrorDocumentKey
	}
	return view
}

func describeNotifications(config *storage.BucketNotificationConfiguration) []notificationView {
	if config == nil {
		return nil
	}
	var views []notificationView
	for _, group := range []struct {
		label string
		kind  storage.NotificationDestinationType
		rules []storage.NotificationConfigurationRule
	}{{"SNS topic", storage.NotificationDestinationTopic, config.TopicConfigurations}, {"SQS queue", storage.NotificationDestinationQueue, config.QueueConfigurations}, {"Lambda function", storage.NotificationDestinationCloudFunction, config.CloudFunctionConfigurations}} {
		for i, rule := range group.rules {
			view := notificationView{ID: "-", Type: group.label, Kind: string(group.kind), Index: strconv.Itoa(i), Destination: rule.DestinationARN, Events: joinOrDash(rule.Events)}
			for _, filter := range rule.FilterRules {
				if filter.Name == "prefix" {
					view.Prefix = filter.Value
				} else if filter.Name == "suffix" {
					view.Suffix = filter.Value
				}
			}
			if rule.ID != nil && *rule.ID != "" {
				view.ID = *rule.ID
			}
			views = append(views, view)
		}
	}
	return views
}

func describeCORS(config *storage.BucketCORSConfiguration) []corsView {
	if config == nil {
		return nil
	}
	views := make([]corsView, 0, len(config.Rules))
	for i, rule := range config.Rules {
		view := corsView{ID: "-", Index: strconv.Itoa(i), Origins: joinOrDash(rule.AllowedOrigins), Methods: joinOrDash(rule.AllowedMethods), Headers: joinOrDash(rule.AllowedHeaders), Expose: joinOrDash(rule.ExposeHeaders), MaxAge: "-"}
		if rule.ID != nil && *rule.ID != "" {
			view.ID = *rule.ID
		}
		if rule.MaxAgeSeconds != nil {
			view.MaxAge = strconv.Itoa(*rule.MaxAgeSeconds) + " s"
		}
		views = append(views, view)
	}
	return views
}

func describeLifecycle(bucket string, config *storage.BucketLifecycleConfiguration) []lifecycleView {
	if config == nil {
		return nil
	}
	var views []lifecycleView
	for i, rule := range config.Rules {
		view := lifecycleRuleSummary(rule)
		query := url.Values{"bucket": {bucket}, "tab": {"lifecycle"}}
		if rule.ID != nil && *rule.ID != "" {
			query.Set("rule", *rule.ID)
		} else {
			query.Set("rule_index", strconv.Itoa(i))
			view.Index = strconv.Itoa(i)
		}
		view.EditURL = "/buckets/detail?" + query.Encode()
		views = append(views, view)
	}
	return views
}

func lifecycleRuleForm(config *storage.BucketLifecycleConfiguration, id string) lifecycleFormView {
	if config != nil && id != "" {
		for _, rule := range config.Rules {
			if rule.ID != nil && *rule.ID == id {
				return lifecycleFormForRule(rule)
			}
		}
	}
	return lifecycleFormForRule(storage.LifecycleRule{Status: storage.LifecycleRuleStatusEnabled})
}

// lifecycleSaveConfirmation summarizes the validated rule before any mutation.
func lifecycleSaveConfirmation(bucket, text string, form url.Values) confirmation {
	var config storage.BucketLifecycleConfiguration
	_ = json.Unmarshal([]byte(text), &config)
	c := confirmation{Title: "Review lifecycle rule", Action: "Create rule", Review: true}
	if form.Get("original_id") != "" || form.Get("original_index") != "" {
		c.Action = "Save rule"
	}
	for _, view := range describeLifecycle(bucket, &storage.BucketLifecycleConfiguration{Rules: config.Rules[len(config.Rules)-1:]}) {
		c.Details = []keyValue{{"Bucket", bucket}, {"Rule", view.ID}, {"Status", view.Status}, {"Scope", view.Scope}}
		if view.Current != "-" {
			c.Details = append(c.Details, keyValue{"Current versions", view.Current})
		}
		if view.Noncurrent != "-" {
			c.Details = append(c.Details, keyValue{"Noncurrent versions", view.Noncurrent})
		}
		if view.Other != "-" {
			c.Details = append(c.Details, keyValue{"Other actions", view.Other})
		}
		if view.Scope == "Entire bucket" {
			c.Warning = "This rule applies to the entire bucket, including existing objects. Expiration and deletion actions can permanently remove data when the rule is enabled."
		}
		if view.Status == storage.LifecycleRuleStatusDisabled {
			c.Message = "This rule will not run until enabled."
		}

		break
	}
	return c
}
