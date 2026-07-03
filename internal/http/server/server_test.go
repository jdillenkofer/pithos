package server

import (
	"net/http/httptest"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestParseCopySourceRangeRejectsMalformedRange(t *testing.T) {
	testutils.SkipIfIntegration(t)

	req := httptest.NewRequest("PUT", "/bucket/key", nil)
	req.Header.Set(copySourceRangeHeader, "bytes=7-bad")

	_, err := parseCopySourceRange(req)

	require.ErrorIs(t, err, ErrInvalidRequest)
}

func TestParseCopySourceRangeParsesSingleRange(t *testing.T) {
	testutils.SkipIfIntegration(t)

	req := httptest.NewRequest("PUT", "/bucket/key", nil)
	req.Header.Set(copySourceRangeHeader, "bytes=7-12")

	byteRange, err := parseCopySourceRange(req)

	require.NoError(t, err)
	require.NotNil(t, byteRange)
	require.NotNil(t, byteRange.Start)
	require.NotNil(t, byteRange.End)
	require.Equal(t, int64(7), *byteRange.Start)
	require.Equal(t, int64(13), *byteRange.End)
}
