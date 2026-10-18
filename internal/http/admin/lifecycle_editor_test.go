package admin

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestAdvancedLifecycleFormAndPrefill(t *testing.T) {
	s := &bucketTestStorage{}
	a := bucketTestApp(t, s)
	fields := url.Values{
		"rule_id": {"archive"}, "status": {"Disabled"}, "prefix": {"logs/"},
		"filter_tag_key": {"team", "env", ""}, "filter_tag_value": {"platform", "", ""},
		"size_greater": {"0"}, "size_less": {"1048576"}, "expire_date": {"2030-01-01"},
		"transition_class": {"STANDARD_IA", "GLACIER", ""}, "transition_days": {"0", "", ""}, "transition_date": {"", "2029-01-01", ""},
		"noncurrent_days": {"90"}, "newer_noncurrent": {"3"},
		"noncurrent_transition_class": {"STANDARD_IA", "GLACIER", ""}, "noncurrent_transition_days": {"7", "30", ""}, "noncurrent_transition_newer": {"2", "4", ""},
	}
	w := structuredBucketRequest(a, "lifecycle", fields)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	config := s.last.(*storage.BucketLifecycleConfiguration)
	rule := config.Rules[0]
	require.Nil(t, storage.ValidateBucketLifecycleConfiguration(config))
	require.Len(t, rule.Filter.And.Tags, 2)
	require.Equal(t, int64(0), *rule.Filter.And.ObjectSizeGreaterThan)
	require.Equal(t, "2030-01-01", rule.Expiration.Date.Format("2006-01-02"))
	require.Len(t, rule.Transitions, 2)
	require.Equal(t, int32(0), *rule.Transitions[0].Days)
	require.Len(t, rule.NoncurrentVersionTransitions, 2)
	prefill := lifecycleFormForRule(rule)
	require.Equal(t, "logs/", prefill.Prefix)
	require.Equal(t, "0", prefill.SizeGreater)
	require.Equal(t, "1048576", prefill.SizeLess)
	require.Equal(t, "2030-01-01", prefill.ExpireDate)
	require.Equal(t, "", prefill.Tags[1].Value)
	require.Equal(t, "2029-01-01", prefill.Transitions[1].Date)
	require.Equal(t, "4", prefill.NoncurrentTransitions[1].Newer)
	summary := lifecycleRuleSummary(rule)
	for _, want := range []string{"team = platform", "env = ", "Size > 0 bytes", "Size < 1048576 bytes"} {
		require.Contains(t, summary.Scope, want)
	}
	require.Contains(t, summary.Current, "Move to GLACIER on 2029-01-01")
	require.Contains(t, summary.Noncurrent, "keep 4 newer versions")
	require.NotContains(t, summary.Scope, "JSON")

	// Editing one rule must keep every advanced field and unrelated rules.
	otherID := "other"
	other := rule
	other.ID = &otherID
	s.lifecycle = &storage.BucketLifecycleConfiguration{Rules: []storage.LifecycleRule{other, rule}}
	fields.Set("original_id", "archive")
	w = structuredBucketRequest(a, "lifecycle", fields)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	updated := s.last.(*storage.BucketLifecycleConfiguration)
	require.Equal(t, other, updated.Rules[0])
	require.Equal(t, rule, updated.Rules[1])
}

func TestLifecycleAdvancedInvalidCombinations(t *testing.T) {
	cases := []url.Values{
		{"expire_days": {"30"}, "expire_date": {"2030-01-01"}},
		{"expire_date": {"invalid"}},
		{"expire_days": {"1"}, "filter_tag_key": {"a", "a"}, "filter_tag_value": {"1", "2"}},
		{"abort_days": {"1"}, "size_greater": {"0"}},
		{"abort_days": {"1"}, "filter_tag_key": {"a"}, "filter_tag_value": {"1"}},
		{"expired_delete_markers": {"true"}, "filter_tag_key": {"a"}, "filter_tag_value": {"1"}},
		{"expire_days": {"1"}, "size_greater": {"100"}, "size_less": {"100"}},
		{"expire_days": {"1"}, "size_less": {"0"}},
		{"expire_days": {"1"}, "size_greater": {"9223372036854775808"}},
		{"transition_class": {"STANDARD_IA"}, "transition_days": {"1"}, "transition_date": {"2030-01-01"}},
		{"transition_class": {"STANDARD"}, "transition_days": {"1"}, "transition_date": {""}},
		{"transition_class": {"GLACIER", "GLACIER"}, "transition_days": {"1", "2"}, "transition_date": {"", ""}},
		{"transition_class": {"GLACIER"}, "transition_days": {"2147483648"}, "transition_date": {""}},
		{"transition_class": {"GLACIER"}, "transition_days": {"30"}, "transition_date": {""}, "expire_days": {"30"}},
		{"noncurrent_transition_class": {"GLACIER"}, "noncurrent_transition_days": {"0"}, "noncurrent_transition_newer": {""}},
		{"noncurrent_transition_class": {"GLACIER"}, "noncurrent_transition_days": {"1"}, "noncurrent_transition_newer": {"101"}},
		{"noncurrent_transition_class": {"GLACIER"}, "noncurrent_transition_days": {"10"}, "noncurrent_transition_newer": {""}, "noncurrent_days": {"5"}},
		{"newer_noncurrent": {"3"}, "expire_days": {"30"}},
		{"transition_class": {"GLACIER"}, "transition_days": {}, "transition_date": {""}},
	}
	for i, fields := range cases {
		fields.Set("rule_id", "invalid")
		s := &bucketTestStorage{}
		w := structuredBucketRequest(bucketTestApp(t, s), "lifecycle", fields)
		require.Equalf(t, http.StatusBadRequest, w.Code, "case %d: %s", i, w.Body.String())
		require.Zero(t, s.writes)
	}
}

func TestUnnamedLifecycleRuleCanBeEditedAndDeleted(t *testing.T) {
	prefix := "legacy/"
	days := int32(30)
	rule := storage.LifecycleRule{Status: "Disabled", Prefix: &prefix, Expiration: &storage.LifecycleExpiration{Days: &days}}
	s := &bucketTestStorage{buckets: []storage.Bucket{{Name: storage.MustNewBucketName("valid-bucket")}}, lifecycle: &storage.BucketLifecycleConfiguration{Rules: []storage.LifecycleRule{rule}}}
	a := bucketTestApp(t, s)
	page := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=lifecycle", nil)
	require.Contains(t, page.Body.String(), "Unnamed rule")
	require.Contains(t, page.Body.String(), "rule_index=0")
	page = bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=lifecycle&rule_index=0", nil)
	require.Equal(t, http.StatusOK, page.Code, page.Body.String())
	require.Contains(t, page.Body.String(), `name="original_index" value="0"`)
	require.Contains(t, page.Body.String(), `name="prefix" value="legacy/"`)
	fields := url.Values{"original_index": {"0"}, "status": {"Disabled"}, "prefix": {"legacy/"}, "expire_days": {"60"}}
	page = structuredBucketRequest(a, "lifecycle", fields)
	require.Equal(t, http.StatusSeeOther, page.Code, page.Body.String())
	s.lifecycle = s.last.(*storage.BucketLifecycleConfiguration)
	require.Len(t, s.lifecycle.Rules, 1)
	require.Nil(t, s.lifecycle.Rules[0].ID)
	require.Equal(t, int32(60), *s.lifecycle.Rules[0].Expiration.Days)
	page = structuredBucketRequest(a, "lifecycle", url.Values{"original_index": {"0"}, "rule_action": {"delete"}})
	require.Equal(t, http.StatusSeeOther, page.Code, page.Body.String())
	require.Equal(t, "lifecycle", s.deleted)
}

func TestLifecycleFormHasNoJSONEditor(t *testing.T) {
	s := &bucketTestStorage{buckets: []storage.Bucket{{Name: storage.MustNewBucketName("valid-bucket")}}}
	a := bucketTestApp(t, s)
	for _, suffix := range []string{"", "&editor=create"} {
		page := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=lifecycle"+suffix, nil)
		require.Equal(t, http.StatusOK, page.Code, page.Body.String())
		require.NotContains(t, page.Body.String(), `name="configuration"`)
		if suffix != "" {
			for _, field := range []string{"filter_tag_key", "size_greater", "expire_date", "transition_class", "noncurrent_transition_class"} {
				require.Contains(t, page.Body.String(), `name="`+field+`"`)
			}
			require.False(t, strings.Contains(page.Body.String(), "<textarea"))
		}
	}
}

func TestLifecycleTransitionOnlyAndCleanupForms(t *testing.T) {
	for _, fields := range []url.Values{
		{"transition_class": {"GLACIER"}, "transition_days": {"0"}, "transition_date": {""}},
		{"noncurrent_transition_class": {"GLACIER"}, "noncurrent_transition_days": {"7"}, "noncurrent_transition_newer": {"2"}},
		{"expired_delete_markers": {"false"}},
		{"expired_delete_markers": {"true"}, "abort_days": {"7"}},
	} {
		fields.Set("rule_id", "action-only")
		s := &bucketTestStorage{}
		w := structuredBucketRequest(bucketTestApp(t, s), "lifecycle", fields)
		require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
		require.Nil(t, storage.ValidateBucketLifecycleConfiguration(s.last.(*storage.BucketLifecycleConfiguration)))
	}
}
