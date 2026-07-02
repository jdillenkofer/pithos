package storage

import (
	"strings"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestValidateUserMetadataAcceptsNilAndSmallSets(t *testing.T) {
	testutils.SkipIfIntegration(t)

	require.NoError(t, ValidateUserMetadata(nil))
	require.NoError(t, ValidateUserMetadata(map[string]string{}))
	require.NoError(t, ValidateUserMetadata(map[string]string{"purpose": "testing", "owner": "storage-team"}))
}

func TestValidateUserMetadataEnforcesSizeLimit(t *testing.T) {
	testutils.SkipIfIntegration(t)

	// Exactly at the limit: key (3 bytes) + value (2045 bytes) = 2048 bytes.
	atLimit := map[string]string{"key": strings.Repeat("v", MaxUserMetadataSize-3)}
	require.NoError(t, ValidateUserMetadata(atLimit))

	// One byte over the limit.
	overLimit := map[string]string{"key": strings.Repeat("v", MaxUserMetadataSize-2)}
	require.ErrorIs(t, ValidateUserMetadata(overLimit), ErrMetadataTooLarge)

	// The limit applies to the sum over all entries.
	half := strings.Repeat("v", MaxUserMetadataSize/2)
	overLimitCombined := map[string]string{"a": half, "b": half}
	require.ErrorIs(t, ValidateUserMetadata(overLimitCombined), ErrMetadataTooLarge)
}
