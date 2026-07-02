package storage

import (
	"strings"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestValidateTagsAcceptsValidTagSet(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tags := map[string]string{"env": "prod", "team": "storage"}
	require.NoError(t, ValidateTags(tags))
}

func TestValidateTagsAcceptsEmptyAndNil(t *testing.T) {
	testutils.SkipIfIntegration(t)

	require.NoError(t, ValidateTags(nil))
	require.NoError(t, ValidateTags(map[string]string{}))
}

func TestValidateTagsRejectsTooManyTags(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tags := map[string]string{}
	for i := 0; i < MaxObjectTags+1; i++ {
		tags[string(rune('a'+i))] = "v"
	}
	require.ErrorIs(t, ValidateTags(tags), ErrInvalidTag)
}

func TestValidateTagsRejectsEmptyKey(t *testing.T) {
	testutils.SkipIfIntegration(t)

	require.ErrorIs(t, ValidateTags(map[string]string{"": "v"}), ErrInvalidTag)
}

func TestValidateTagsRejectsOverlongKey(t *testing.T) {
	testutils.SkipIfIntegration(t)

	key := strings.Repeat("k", MaxTagKeyLength+1)
	require.ErrorIs(t, ValidateTags(map[string]string{key: "v"}), ErrInvalidTag)
}

func TestValidateTagsRejectsOverlongValue(t *testing.T) {
	testutils.SkipIfIntegration(t)

	value := strings.Repeat("v", MaxTagValueLength+1)
	require.ErrorIs(t, ValidateTags(map[string]string{"k": value}), ErrInvalidTag)
}

func TestValidateTagsAcceptsBoundaryLengths(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tags := map[string]string{
		strings.Repeat("k", MaxTagKeyLength): strings.Repeat("v", MaxTagValueLength),
	}
	require.NoError(t, ValidateTags(tags))
}

func TestParseTaggingHeaderParsesPairs(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tags, err := ParseTaggingHeader("k1=v1&k2=v2")

	require.NoError(t, err)
	require.Equal(t, map[string]string{"k1": "v1", "k2": "v2"}, tags)
}

func TestParseTaggingHeaderEmptyYieldsEmptyMap(t *testing.T) {
	testutils.SkipIfIntegration(t)

	tags, err := ParseTaggingHeader("")

	require.NoError(t, err)
	require.NotNil(t, tags)
	require.Len(t, tags, 0)
}

func TestParseTaggingHeaderRejectsDuplicateKeys(t *testing.T) {
	testutils.SkipIfIntegration(t)

	_, err := ParseTaggingHeader("k=v1&k=v2")

	require.ErrorIs(t, err, ErrInvalidTag)
}

func TestParseTaggingHeaderRejectsOverlongValue(t *testing.T) {
	testutils.SkipIfIntegration(t)

	_, err := ParseTaggingHeader("k=" + strings.Repeat("v", MaxTagValueLength+1))

	require.ErrorIs(t, err, ErrInvalidTag)
}
