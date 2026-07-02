package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestParseObjectMetadataHeadersReturnsNilWithoutMetadata(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r := httptest.NewRequest("PUT", "/bucket/key", nil)
	r.Header.Set(contentTypeHeader, "text/plain")
	r.Header.Set(metadataDirectiveHeader, "REPLACE")

	metadata, err := parseObjectMetadataHeaders(r.Header)
	require.NoError(t, err)
	require.Nil(t, metadata)
}

func TestParseObjectMetadataHeadersParsesSystemHeaders(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r := httptest.NewRequest("PUT", "/bucket/key", nil)
	r.Header.Set(cacheControlHeader, "max-age=3600")
	r.Header.Set(contentDispositionHeader, `attachment; filename="report.pdf"`)
	r.Header.Set(contentEncodingHeader, "gzip")
	r.Header.Set(contentLanguageHeader, "de-DE")
	r.Header.Set(expiresHeader, "Wed, 21 Oct 2026 07:28:00 GMT")
	r.Header.Set(websiteRedirectLocationHeader, "/new-location")

	metadata, err := parseObjectMetadataHeaders(r.Header)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	require.Equal(t, "max-age=3600", *metadata.CacheControl)
	require.Equal(t, `attachment; filename="report.pdf"`, *metadata.ContentDisposition)
	require.Equal(t, "gzip", *metadata.ContentEncoding)
	require.Equal(t, "de-DE", *metadata.ContentLanguage)
	require.Equal(t, "Wed, 21 Oct 2026 07:28:00 GMT", *metadata.Expires)
	require.Equal(t, "/new-location", *metadata.WebsiteRedirectLocation)
	require.Nil(t, metadata.UserMetadata)
}

func TestParseObjectMetadataHeadersLowercasesUserMetadataKeys(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r := httptest.NewRequest("PUT", "/bucket/key", nil)
	r.Header.Set("x-amz-meta-Purpose", "testing")
	r.Header.Set("X-Amz-Meta-OWNER", "storage-team")

	metadata, err := parseObjectMetadataHeaders(r.Header)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	require.Equal(t, map[string]string{"purpose": "testing", "owner": "storage-team"}, metadata.UserMetadata)
}

func TestParseObjectMetadataHeadersCombinesRepeatedHeaders(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r := httptest.NewRequest("PUT", "/bucket/key", nil)
	r.Header.Add("x-amz-meta-tags", "a")
	r.Header.Add("x-amz-meta-tags", "b")

	metadata, err := parseObjectMetadataHeaders(r.Header)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	require.Equal(t, map[string]string{"tags": "a,b"}, metadata.UserMetadata)
}

func TestParseObjectMetadataHeadersIgnoresNonMetadataAmzHeaders(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r := httptest.NewRequest("PUT", "/bucket/key", nil)
	r.Header.Set(metadataDirectiveHeader, "REPLACE")
	r.Header.Set(taggingHeader, "a=1")
	r.Header.Set("x-amz-meta-key", "value")

	metadata, err := parseObjectMetadataHeaders(r.Header)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	require.Equal(t, map[string]string{"key": "value"}, metadata.UserMetadata)
}

func TestParseObjectMetadataHeadersRejectsOversizedUserMetadata(t *testing.T) {
	testutils.SkipIfIntegration(t)

	r := httptest.NewRequest("PUT", "/bucket/key", nil)
	r.Header.Set("x-amz-meta-big", strings.Repeat("v", storage.MaxUserMetadataSize))

	_, err := parseObjectMetadataHeaders(r.Header)
	require.ErrorIs(t, err, storage.ErrMetadataTooLarge)
}

func TestSetMetadataHeadersFromObjectRoundTrips(t *testing.T) {
	testutils.SkipIfIntegration(t)

	object := &storage.Object{
		Metadata: storage.ObjectMetadata{
			CacheControl:            ptrutils.ToPtr("no-cache"),
			ContentDisposition:      ptrutils.ToPtr("inline"),
			ContentEncoding:         ptrutils.ToPtr("gzip"),
			ContentLanguage:         ptrutils.ToPtr("en"),
			Expires:                 ptrutils.ToPtr("Wed, 21 Oct 2026 07:28:00 GMT"),
			WebsiteRedirectLocation: ptrutils.ToPtr("https://example.com/"),
			UserMetadata:            map[string]string{"purpose": "testing"},
		},
	}

	headers := http.Header{}
	setMetadataHeadersFromObject(headers, object)

	require.Equal(t, "no-cache", headers.Get(cacheControlHeader))
	require.Equal(t, "inline", headers.Get(contentDispositionHeader))
	require.Equal(t, "gzip", headers.Get(contentEncodingHeader))
	require.Equal(t, "en", headers.Get(contentLanguageHeader))
	require.Equal(t, "Wed, 21 Oct 2026 07:28:00 GMT", headers.Get(expiresHeader))
	require.Equal(t, "https://example.com/", headers.Get(websiteRedirectLocationHeader))
	require.Equal(t, "testing", headers.Get("x-amz-meta-purpose"))
}
