package admin

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/require"
)

func corsTestForm(config *storage.BucketCORSConfiguration) url.Values {
	return url.Values{
		"cors_hash":   {policyHash([]byte(configurationText(config)))},
		"cors_action": {"save"}, "origins": {" https://example.com\r\nhttps://*.example.org\n\n"},
		"methods": {"GET", "PUT"}, "headers": {"Content-Type\r\nx-amz-*"},
		"expose": {"ETag\nx-amz-version-id"}, "max_age": {"0"},
	}
}

func TestCORSStructuredCreateEditAndDelete(t *testing.T) {
	original := storage.CORSRule{ID: ptrutils.ToPtr("existing"), AllowedOrigins: []string{"https://keep.example"}, AllowedMethods: []string{"HEAD"}, MaxAgeSeconds: ptrutils.ToPtr(30)}
	s := &bucketTestStorage{cors: &storage.BucketCORSConfiguration{Rules: []storage.CORSRule{original}}}
	a := bucketTestApp(t, s)
	form := corsTestForm(s.cors)
	form.Set("id", "website")
	w := structuredBucketRequest(a, "cors", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	created := s.last.(*storage.BucketCORSConfiguration)
	require.Len(t, created.Rules, 2)
	require.Equal(t, original, created.Rules[0])
	require.Len(t, s.cors.Rules, 1, "do not mutate a shared storage snapshot")
	rule := created.Rules[1]
	require.Equal(t, "website", *rule.ID)
	require.Equal(t, []string{"https://example.com", "https://*.example.org"}, rule.AllowedOrigins)
	require.Equal(t, []string{"GET", "PUT"}, rule.AllowedMethods)
	require.Equal(t, []string{"Content-Type", "x-amz-*"}, rule.AllowedHeaders)
	require.Equal(t, []string{"ETag", "x-amz-version-id"}, rule.ExposeHeaders)
	require.Equal(t, 0, *rule.MaxAgeSeconds, "zero is distinct from an omitted maximum age")

	s.cors = created
	form = corsTestForm(s.cors)
	form.Set("rule_index", "1")
	form.Set("max_age", "")
	w = structuredBucketRequest(a, "cors", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	edited := s.last.(*storage.BucketCORSConfiguration)
	require.Len(t, edited.Rules, 2)
	require.Equal(t, original, edited.Rules[0])
	require.Nil(t, edited.Rules[1].MaxAgeSeconds)
	require.Nil(t, edited.Rules[1].ID)

	form = corsTestForm(s.cors)
	form.Set("rule_index", "1")
	form.Set("cors_action", "delete")
	w = structuredBucketRequest(a, "cors", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, []storage.CORSRule{original}, s.last.(*storage.BucketCORSConfiguration).Rules)

	s.cors = &storage.BucketCORSConfiguration{Rules: []storage.CORSRule{original}}
	form = corsTestForm(s.cors)
	form.Set("rule_index", "0")
	form.Set("cors_action", "delete")
	w = structuredBucketRequest(a, "cors", form)
	require.Equal(t, http.StatusSeeOther, w.Code, w.Body.String())
	require.Equal(t, "cors", s.deleted)
}

func TestCORSFormRejectsInvalidAndStaleSubmissions(t *testing.T) {
	for _, tc := range []struct{ field, value string }{
		{"origins", ""}, {"origins", "https://**.example.com"}, {"methods", ""}, {"methods", "BAD"},
		{"headers", "**"}, {"max_age", "-1"}, {"max_age", "1.5"}, {"max_age", "999999999999999999999999"},
		{"rule_index", "-1"}, {"rule_index", "0"}, {"rule_index", "bad"}, {"cors_hash", "stale"}, {"cors_action", "bad"},
	} {
		t.Run(tc.field+"="+tc.value, func(t *testing.T) {
			s := &bucketTestStorage{cors: &storage.BucketCORSConfiguration{}}
			form := corsTestForm(s.cors)
			form.Set(tc.field, tc.value)
			w := structuredBucketRequest(bucketTestApp(t, s), "cors", form)
			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			require.Zero(t, s.writes)
		})
	}
	config := &storage.BucketCORSConfiguration{Rules: []storage.CORSRule{{ID: ptrutils.ToPtr("duplicate"), AllowedOrigins: []string{"*"}, AllowedMethods: []string{"GET"}}}}
	form := corsTestForm(config)
	form.Set("id", "duplicate")
	_, _, err := corsFromForm(context.Background(), &bucketTestStorage{cors: config}, storage.MustNewBucketName("valid-bucket"), form)
	require.ErrorContains(t, err, "duplicate ID")
}

func TestCORSPagesAndDeleteConfirmation(t *testing.T) {
	s := &bucketTestStorage{
		buckets: []storage.Bucket{{Name: storage.MustNewBucketName("valid-bucket")}},
		cors:    &storage.BucketCORSConfiguration{Rules: []storage.CORSRule{{AllowedOrigins: []string{"https://example.com"}, AllowedMethods: []string{"GET"}, MaxAgeSeconds: ptrutils.ToPtr(0)}}},
	}
	a := bucketTestApp(t, s)
	for _, path := range []string{"", "&editor=create", "&rule_index=0"} {
		w := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=cors"+path, nil)
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		require.NotContains(t, w.Body.String(), `name="configuration"`)
		require.Contains(t, w.Body.String(), `name="csrf_token"`)
		if path != "" {
			require.Contains(t, w.Body.String(), `class="container cors-editor"`)
			require.Contains(t, w.Body.String(), `class="container-body form-stack"`)
			require.Contains(t, w.Body.String(), `name="origins"`)
			require.Contains(t, w.Body.String(), `name="methods"`)
		}
		if path == "&rule_index=0" {
			require.Contains(t, w.Body.String(), "Edit CORS rule")
			require.Contains(t, w.Body.String(), `value="GET" checked`)
			require.Contains(t, w.Body.String(), `name="max_age" min="0" step="1" value="0"`)
		}
	}
	w := bucketRequest(a, "GET", "/buckets/detail?bucket=valid-bucket&tab=cors&rule_index=1", nil)
	require.Equal(t, http.StatusNotFound, w.Code)
	form := corsTestForm(s.cors)
	form.Set("bucket", "valid-bucket")
	form.Set("tab", "cors")
	form.Set("editor", "form")
	form.Set("cors_action", "delete")
	form.Set("rule_index", "0")
	w = bucketRequest(a, "POST", "/buckets/configure", form)
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Contains(t, w.Body.String(), "Delete CORS rule?")
	require.Zero(t, s.writes)
	form.Set("confirm", "yes")
	form.Set("cors_hash", "stale")
	w = bucketRequest(a, "POST", "/buckets/configure", form)
	require.Equal(t, http.StatusBadRequest, w.Code)
	require.Zero(t, s.writes, "confirmation must recheck the snapshot")
}
