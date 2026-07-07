package server

import (
	"context"
	"encoding/xml"
	"net/http/httptest"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/delegator"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

type objectTaggingResolverStorage struct {
	delegator.DelegatingStorage
	seenOpts *storage.ObjectTaggingOptions
}

func (s *objectTaggingResolverStorage) GetObjectTagging(ctx context.Context, bucketName storage.BucketName, key storage.ObjectKey, opts *storage.ObjectTaggingOptions) (map[string]string, error) {
	s.seenOpts = opts
	return map[string]string{"state": "archived"}, nil
}

func TestTaggingHeaderAppliesOnlyToTagStoringOperations(t *testing.T) {
	testutils.SkipIfIntegration(t)

	plainRequest := httptest.NewRequest("PUT", "/bucket/key", nil)
	require.True(t, taggingHeaderApplies(authorization.OperationPutObject, plainRequest))
	require.True(t, taggingHeaderApplies(authorization.OperationCreateMultipartUpload, plainRequest))
	require.False(t, taggingHeaderApplies(authorization.OperationGetObject, plainRequest))
	require.False(t, taggingHeaderApplies(authorization.OperationPutObjectTagging, plainRequest))

	// CopyObject only stores the header's tags with the REPLACE directive; the
	// default (COPY) carries over the source tags and ignores the header.
	require.False(t, taggingHeaderApplies(authorization.OperationCopyObject, plainRequest))
	replaceRequest := httptest.NewRequest("PUT", "/bucket/key", nil)
	replaceRequest.Header.Set(taggingDirectiveHeader, "replace")
	require.True(t, taggingHeaderApplies(authorization.OperationCopyObject, replaceRequest))
	copyRequest := httptest.NewRequest("PUT", "/bucket/key", nil)
	copyRequest.Header.Set(taggingDirectiveHeader, "COPY")
	require.False(t, taggingHeaderApplies(authorization.OperationCopyObject, copyRequest))
}

func TestExistingObjectTagsResolverUsesVersionID(t *testing.T) {
	testutils.SkipIfIntegration(t)

	st := &objectTaggingResolverStorage{}
	server := &Server{storage: st}
	bucket := "bucket"
	key := "key"
	versionID := "version-1"

	resolver := server.makeExistingObjectTagsResolver(&bucket, &key, &versionID)
	require.NotNil(t, resolver)
	tags, err := resolver(context.Background())

	require.NoError(t, err)
	require.Equal(t, map[string]string{"state": "archived"}, tags)
	require.NotNil(t, st.seenOpts)
	require.NotNil(t, st.seenOpts.VersionID)
	require.Equal(t, versionID, *st.seenOpts.VersionID)
}

func TestUnmarshalTaggingParsesTagSet(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := `<Tagging><TagSet><Tag><Key>k1</Key><Value>v1</Value></Tag><Tag><Key>k2</Key><Value>v2</Value></Tag></TagSet></Tagging>`

	var tagging Tagging
	err := xml.Unmarshal([]byte(body), &tagging)

	require.NoError(t, err)
	require.Len(t, tagging.TagSet, 2)

	tags, err := tagSetToMap(&tagging)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"k1": "v1", "k2": "v2"}, tags)
}

func TestUnmarshalTaggingEmptyTagSet(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := `<Tagging><TagSet></TagSet></Tagging>`

	var tagging Tagging
	err := xml.Unmarshal([]byte(body), &tagging)

	require.NoError(t, err)
	require.Len(t, tagging.TagSet, 0)

	tags, err := tagSetToMap(&tagging)
	require.NoError(t, err)
	require.Len(t, tags, 0)
}

func TestTagSetToMapRejectsDuplicateKeys(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tagging := &Tagging{TagSet: []Tag{{Key: "k", Value: "v1"}, {Key: "k", Value: "v2"}}}

	_, err := tagSetToMap(tagging)

	require.ErrorIs(t, err, storage.ErrInvalidTag)
}

func TestMarshalTaggingRoundTrips(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tagging := Tagging{
		Xmlns:  "http://s3.amazonaws.com/doc/2006-03-01/",
		TagSet: []Tag{{Key: "env", Value: "prod"}},
	}

	out, err := xmlMarshalWithDocType(tagging)
	require.NoError(t, err)

	var parsed Tagging
	require.NoError(t, xml.Unmarshal(out, &parsed))
	require.Equal(t, tagging.TagSet, parsed.TagSet)
}
