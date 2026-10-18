package admin

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"

	s3 "github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

func websiteTestConfig() *storage.WebsiteConfiguration {
	return &storage.WebsiteConfiguration{
		IndexDocumentSuffix: "index.html", ErrorDocumentKey: ptrutils.ToPtr("error.html"),
		RoutingRules: []storage.WebsiteRoutingRule{
			{Condition: &storage.WebsiteRoutingRuleCondition{KeyPrefixEquals: ptrutils.ToPtr("old/")}, Redirect: storage.WebsiteRedirect{ReplaceKeyPrefixWith: ptrutils.ToPtr("new/"), HttpRedirectCode: ptrutils.ToPtr("301")}},
			{Condition: &storage.WebsiteRoutingRuleCondition{HttpErrorCodeReturnedEquals: ptrutils.ToPtr("404")}, Redirect: storage.WebsiteRedirect{HostName: ptrutils.ToPtr("docs.example.com"), Protocol: ptrutils.ToPtr("https"), ReplaceKeyWith: ptrutils.ToPtr("missing.html"), HttpRedirectCode: ptrutils.ToPtr("302")}},
			{Redirect: storage.WebsiteRedirect{Protocol: ptrutils.ToPtr("https"), HttpRedirectCode: ptrutils.ToPtr("308")}},
		},
	}
}

func websiteTestForm(config *storage.WebsiteConfiguration) url.Values {
	if config == nil {
		config = &storage.WebsiteConfiguration{}
	}
	return url.Values{
		"website_hash": {policyHash([]byte(configurationText(config)))}, "website_rule_action": {"save"},
		"position":     {strconv.Itoa(len(config.RoutingRules) + 1)},
		"match_prefix": {"enabled"}, "key_prefix": {" old/ "},
		"match_error": {"enabled"}, "error_code": {"404"},
		"change_host": {"enabled"}, "redirect_host": {"new.example.com"},
		"redirect_protocol": {"https"}, "redirect_code": {"307"},
		"replacement": {"prefix"}, "replacement_key": {" new/ "},
	}
}

func TestWebsiteRoutingCreateAtPosition(t *testing.T) {
	config := websiteTestConfig()
	s := &bucketTestStorage{website: config}
	form := websiteTestForm(config)
	form.Set("position", "2")
	w := structuredBucketRequest(bucketTestApp(t, s), "website", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	actual := s.last.(*storage.WebsiteConfiguration)
	require.Equal(t, config.IndexDocumentSuffix, actual.IndexDocumentSuffix)
	require.Equal(t, config.ErrorDocumentKey, actual.ErrorDocumentKey)
	require.Len(t, actual.RoutingRules, 4)
	require.Equal(t, config.RoutingRules[0], actual.RoutingRules[0])
	require.Equal(t, config.RoutingRules[1:], actual.RoutingRules[2:])
	rule := actual.RoutingRules[1]
	require.Equal(t, " old/ ", *rule.Condition.KeyPrefixEquals)
	require.Equal(t, "404", *rule.Condition.HttpErrorCodeReturnedEquals)
	require.Equal(t, " new/ ", *rule.Redirect.ReplaceKeyPrefixWith)
	require.Nil(t, rule.Redirect.ReplaceKeyWith)
	require.Equal(t, "new.example.com", *rule.Redirect.HostName)
	require.Equal(t, "https", *rule.Redirect.Protocol)
	require.Equal(t, "307", *rule.Redirect.HttpRedirectCode)
	require.Equal(t, websiteTestConfig(), config, "do not mutate a shared storage snapshot")
}

func TestWebsiteRoutingEditAndReorder(t *testing.T) {
	config := websiteTestConfig()
	s := &bucketTestStorage{website: config}
	a := bucketTestApp(t, s)
	form := websiteTestForm(config)
	form.Set("rule_index", "0")
	form.Set("position", "3")
	form.Set("replacement", "key")
	form.Set("replacement_key", "landing.html")
	w := structuredBucketRequest(a, "website", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	actual := s.last.(*storage.WebsiteConfiguration)
	require.Len(t, actual.RoutingRules, 3)
	require.Equal(t, config.RoutingRules[1:], actual.RoutingRules[:2])
	require.Equal(t, "landing.html", *actual.RoutingRules[2].Redirect.ReplaceKeyWith)
	require.Nil(t, actual.RoutingRules[2].Redirect.ReplaceKeyPrefixWith)

	for _, tc := range []struct {
		action, index string
		order         []int
	}{
		{"up", "1", []int{1, 0, 2}}, {"down", "1", []int{0, 2, 1}},
	} {
		form = websiteTestForm(config)
		form.Set("website_rule_action", tc.action)
		form.Set("rule_index", tc.index)
		w = structuredBucketRequest(a, "website", form)
		require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
		actual = s.last.(*storage.WebsiteConfiguration)
		for i, original := range tc.order {
			require.Equal(t, config.RoutingRules[original], actual.RoutingRules[i])
		}
	}
	require.Equal(t, websiteTestConfig(), config)
}

func TestWebsiteRoutingDeletePreservesHosting(t *testing.T) {
	for _, action := range []string{"delete", "delete_all"} {
		config := websiteTestConfig()
		s := &bucketTestStorage{website: config}
		form := websiteTestForm(config)
		form.Set("website_rule_action", action)
		form.Set("rule_index", "1")
		w := structuredBucketRequest(bucketTestApp(t, s), "website", form)
		require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
		actual := s.last.(*storage.WebsiteConfiguration)
		require.Equal(t, "index.html", actual.IndexDocumentSuffix)
		require.Equal(t, "error.html", *actual.ErrorDocumentKey)
		if action == "delete" {
			require.Equal(t, []storage.WebsiteRoutingRule{config.RoutingRules[0], config.RoutingRules[2]}, actual.RoutingRules)
		} else {
			require.Empty(t, actual.RoutingRules)
		}
		require.Equal(t, websiteTestConfig(), config)
	}
}

func TestWebsiteRoutingRoundTrip(t *testing.T) {
	rules := append(websiteTestConfig().RoutingRules,
		storage.WebsiteRoutingRule{Condition: &storage.WebsiteRoutingRuleCondition{KeyPrefixEquals: ptrutils.ToPtr("")}, Redirect: storage.WebsiteRedirect{ReplaceKeyPrefixWith: ptrutils.ToPtr("")}},
		storage.WebsiteRoutingRule{Redirect: storage.WebsiteRedirect{ReplaceKeyWith: ptrutils.ToPtr("")}},
		storage.WebsiteRoutingRule{Redirect: storage.WebsiteRedirect{HostName: ptrutils.ToPtr(""), Protocol: ptrutils.ToPtr("")}},
	)
	for i, rule := range rules {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			config := &storage.WebsiteConfiguration{IndexDocumentSuffix: "index.html", RoutingRules: []storage.WebsiteRoutingRule{rule}}
			view := websiteRoutingRuleForm(rule, "0", 1)
			form := websiteTestForm(config)
			form.Set("rule_index", view.Index)
			form.Set("position", view.Position)
			form.Del("match_prefix")
			form.Del("match_error")
			form.Del("change_host")
			if view.PrefixSet {
				form.Set("match_prefix", "enabled")
			}
			if view.ErrorSet {
				form.Set("match_error", "enabled")
			}
			if view.HostSet {
				form.Set("change_host", "enabled")
			}
			form.Set("key_prefix", view.Prefix)
			form.Set("error_code", view.ErrorCode)
			form.Set("redirect_host", view.Host)
			form.Set("redirect_protocol", view.Protocol)
			form.Set("redirect_code", view.Code)
			form.Set("replacement", view.Replacement)
			form.Set("replacement_key", view.Key)
			s := &bucketTestStorage{website: config}
			w := structuredBucketRequest(bucketTestApp(t, s), "website", form)
			require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
			expected, err := s3.ValidateWebsiteConfiguration(config)
			require.NoError(t, err)
			require.Equal(t, expected, s.last, "preserve explicit empty prefixes and replacement values")
		})
	}
}

func TestWebsiteRoutingInvalidSubmissionsNeverWrite(t *testing.T) {
	for _, tc := range []struct{ field, value string }{
		{"website_hash", ""}, {"website_hash", "stale"}, {"website_rule_action", "bad"},
		{"rule_index", "-1"}, {"rule_index", "3"}, {"rule_index", "bad"},
		{"position", "0"}, {"position", "5"}, {"position", "bad"},
		{"redirect_protocol", "ftp"}, {"redirect_code", "200"}, {"redirect_code", ""},
		{"replacement", "bad"}, {"error_code", "399"}, {"error_code", "600"}, {"error_code", "bad"}, {"error_code", ""},
	} {
		t.Run(tc.field+"="+tc.value, func(t *testing.T) {
			s := &bucketTestStorage{website: websiteTestConfig()}
			form := websiteTestForm(s.website)
			form.Set(tc.field, tc.value)
			w := structuredBucketRequest(bucketTestApp(t, s), "website", form)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.Zero(t, s.writes)
		})
	}
	for _, tc := range []struct{ action, index string }{{"delete", ""}, {"up", ""}, {"down", ""}, {"up", "0"}, {"down", "2"}} {
		s := &bucketTestStorage{website: websiteTestConfig()}
		form := websiteTestForm(s.website)
		form.Set("website_rule_action", tc.action)
		form.Set("rule_index", tc.index)
		w := structuredBucketRequest(bucketTestApp(t, s), "website", form)
		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		require.Zero(t, s.writes)
	}
	s := &bucketTestStorage{website: websiteTestConfig()}
	form := websiteTestForm(s.website)
	form.Del("change_host")
	form.Set("redirect_protocol", "")
	form.Set("replacement", "unchanged")
	w := structuredBucketRequest(bucketTestApp(t, s), "website", form)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "change the host, protocol or object key")
	require.Zero(t, s.writes)
}

func TestWebsiteRoutingReviewsAndStaleConfirmation(t *testing.T) {
	for _, action := range []string{"save", "delete", "delete_all"} {
		s := &bucketTestStorage{website: websiteTestConfig()}
		a := bucketTestApp(t, s)
		form := websiteTestForm(s.website)
		form.Set("bucket", "valid-bucket")
		form.Set("tab", "website")
		form.Set("editor", "form")
		form.Set("website_rule_action", action)
		if action == "delete" {
			form.Set("rule_index", "0")
		}
		form.Del("match_prefix")
		form.Del("match_error")
		w := bucketRequest(a, "POST", "/buckets/configure", form)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), `name="website_hash"`)
		require.Zero(t, s.writes)
		if action == "save" {
			require.Contains(t, w.Body.String(), "Review website redirection rule")
			require.Contains(t, w.Body.String(), "matches every request")
			require.Contains(t, w.Body.String(), "An earlier rule matches every request")
			require.Contains(t, w.Body.String(), "new.example.com")
		} else {
			require.Contains(t, w.Body.String(), "Delete")
		}
		s.website.IndexDocumentSuffix = "changed.html"
		form.Set("confirm", "yes")
		w = bucketRequest(a, "POST", "/buckets/configure", form)
		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		require.Contains(t, w.Body.String(), "changed")
		require.Zero(t, s.writes)
	}
}

func TestWebsiteRoutingCatchAllDetection(t *testing.T) {
	for _, tc := range []struct {
		condition *storage.WebsiteRoutingRuleCondition
		matches   bool
	}{
		{nil, true},
		{&storage.WebsiteRoutingRuleCondition{}, true},
		{&storage.WebsiteRoutingRuleCondition{KeyPrefixEquals: ptrutils.ToPtr("")}, true},
		{&storage.WebsiteRoutingRuleCondition{KeyPrefixEquals: ptrutils.ToPtr("old/")}, false},
		{&storage.WebsiteRoutingRuleCondition{HttpErrorCodeReturnedEquals: ptrutils.ToPtr("404")}, false},
		{&storage.WebsiteRoutingRuleCondition{KeyPrefixEquals: ptrutils.ToPtr(""), HttpErrorCodeReturnedEquals: ptrutils.ToPtr("404")}, false},
	} {
		require.Equal(t, tc.matches, websiteRuleMatchesAll(storage.WebsiteRoutingRule{Condition: tc.condition}))
	}
}

func TestWebsiteHostingPreservesRulesAndRequiresDiscardConsent(t *testing.T) {
	config := websiteTestConfig()
	s := &bucketTestStorage{website: config}
	a := bucketTestApp(t, s)
	form := url.Values{"website_hash": {policyHash([]byte(configurationText(config)))}, "website": {"enabled"}, "hosting": {"website"}, "index_document": {"new-index.html"}, "error_document": {"new-error.html"}}
	w := structuredBucketRequest(a, "website", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	actual := s.last.(*storage.WebsiteConfiguration)
	require.Equal(t, config.RoutingRules, actual.RoutingRules)
	require.Equal(t, "new-index.html", actual.IndexDocumentSuffix)
	form.Set("hosting", "redirect")
	form.Set("redirect_host", "example.com")
	w = structuredBucketRequest(a, "website", form)
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	require.Equal(t, 1, s.writes)
	form.Set("discard_routing_rules", "yes")
	w = structuredBucketRequest(a, "website", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Empty(t, s.last.(*storage.WebsiteConfiguration).RoutingRules)
	form.Set("website_hash", "stale")
	w = structuredBucketRequest(a, "website", form)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Equal(t, 2, s.writes)
}

func TestWebsiteLoadFailuresAndHostingPrerequisites(t *testing.T) {
	failure := errors.New("website read failed")
	s := &bucketTestStorage{websiteFailure: failure}
	form := websiteTestForm(nil)
	form.Del("website_rule_action")
	form.Set("website", "enabled")
	form.Set("index_document", "index.html")
	_, _, err := websiteFromForm(context.Background(), s, storage.MustNewBucketName("valid-bucket"), form)
	require.ErrorIs(t, err, failure)
	require.Zero(t, s.writes)
	for _, config := range []*storage.WebsiteConfiguration{nil, {RedirectAllRequestsTo: &storage.WebsiteRedirectAllRequestsTo{HostName: "example.com"}}} {
		s := &bucketTestStorage{website: config, buckets: []storage.Bucket{{Name: storage.MustNewBucketName("valid-bucket")}}}
		a := bucketTestApp(t, s)
		w := structuredBucketRequest(a, "website", websiteTestForm(config))
		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		require.Zero(t, s.writes)
		w = bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=website&editor=create", nil)
		require.Equal(t, http.StatusBadRequest, w.Code)
		w = bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=website", nil)
		require.Equal(t, http.StatusOK, w.Code)
		require.NotContains(t, w.Body.String(), ">Create redirection rule</a>")
	}
}

func TestWebsiteRoutingPagesAndEscaping(t *testing.T) {
	s := &bucketTestStorage{website: websiteTestConfig(), buckets: []storage.Bucket{{Name: storage.MustNewBucketName("valid-bucket")}}}
	a := bucketTestApp(t, s)
	for _, suffix := range []string{"", "&editor=create", "&rule_index=0", "&rule_index=1", "&rule_index=2"} {
		w := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=website"+suffix, nil)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		body := w.Body.String()
		require.NotContains(t, body, `name="configuration"`)
		require.Equal(t, strings.Count(body, "<form"), strings.Count(body, `name="csrf_token"`))
		if suffix == "" {
			require.Contains(t, body, "first matching rule")
			require.Contains(t, body, "Move up")
			require.Contains(t, body, "Move down")
		} else {
			require.Contains(t, body, `class="container-body form-stack"`)
			require.NotContains(t, body, "Delete bucket")
			for _, field := range []string{"match_prefix", "key_prefix", "match_error", "error_code", "change_host", "redirect_host", "redirect_protocol", "redirect_code", "replacement", "replacement_key", "position", "website_hash"} {
				require.Contains(t, body, `name="`+field+`"`)
			}
			if strings.Contains(suffix, "rule_index") {
				require.Contains(t, body, "Edit website redirection rule")
			}
		}
	}
	for _, index := range []string{"-1", "3", "bad", ""} {
		w := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=website&rule_index="+index, nil)
		require.Equal(t, http.StatusNotFound, w.Code)
	}
	unsafe := `"><script>alert(1)</script>`
	s.website.RoutingRules[0].Condition.KeyPrefixEquals = &unsafe
	s.website.RoutingRules[0].Redirect.ReplaceKeyPrefixWith = &unsafe
	for _, suffix := range []string{"", "&rule_index=0"} {
		w := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=website"+suffix, nil)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotContains(t, w.Body.String(), "<script>")
		require.Contains(t, w.Body.String(), "&lt;script&gt;")
	}
}
