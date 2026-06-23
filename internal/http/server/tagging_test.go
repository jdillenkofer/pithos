package server

import (
	"encoding/xml"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

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
