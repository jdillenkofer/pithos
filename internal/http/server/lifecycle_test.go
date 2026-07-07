package server

import (
	"encoding/xml"
	"testing"

	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestLifecycleNoncurrentVersionExpirationRoundTrips(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := `<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter><Prefix>logs/</Prefix></Filter><NoncurrentVersionExpiration><NoncurrentDays>30</NoncurrentDays><NewerNoncurrentVersions>2</NewerNoncurrentVersions></NoncurrentVersionExpiration></Rule></LifecycleConfiguration>`

	var request LifecycleConfiguration
	require.NoError(t, xml.Unmarshal([]byte(body), &request))

	config, validationErr := convertLifecycleConfigurationFromXML(&request)
	require.Nil(t, validationErr)
	require.Nil(t, storage.ValidateBucketLifecycleConfiguration(config))
	require.Len(t, config.Rules, 1)
	require.NotNil(t, config.Rules[0].NoncurrentVersionExpiration)
	require.Equal(t, int32(30), *config.Rules[0].NoncurrentVersionExpiration.NoncurrentDays)
	require.Equal(t, int32(2), *config.Rules[0].NoncurrentVersionExpiration.NewerNoncurrentVersions)

	response := convertLifecycleConfigurationToXML(config)
	require.Len(t, response.Rules, 1)
	require.NotNil(t, response.Rules[0].NoncurrentVersionExpiration)
	require.Equal(t, int32(30), *response.Rules[0].NoncurrentVersionExpiration.NoncurrentDays)
	require.Equal(t, int32(2), *response.Rules[0].NoncurrentVersionExpiration.NewerNoncurrentVersions)
}

func TestLifecycleTransitionElementsAreRecognized(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := `<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition><NoncurrentVersionTransition><NoncurrentDays>2</NoncurrentDays><NewerNoncurrentVersions>3</NewerNoncurrentVersions><StorageClass>DEEP_ARCHIVE</StorageClass></NoncurrentVersionTransition></Rule></LifecycleConfiguration>`

	var request LifecycleConfiguration
	require.NoError(t, xml.Unmarshal([]byte(body), &request))
	require.Len(t, request.Rules, 1)
	require.Len(t, request.Rules[0].Transitions, 1)
	require.Len(t, request.Rules[0].NoncurrentVersionTransitions, 1)

	config, validationErr := convertLifecycleConfigurationFromXML(&request)
	require.Nil(t, validationErr)
	require.Nil(t, storage.ValidateBucketLifecycleConfiguration(config))
	require.Len(t, config.Rules, 1)
	require.Len(t, config.Rules[0].Transitions, 1)
	require.Equal(t, int32(1), *config.Rules[0].Transitions[0].Days)
	require.Equal(t, "GLACIER", config.Rules[0].Transitions[0].StorageClass)
	require.Len(t, config.Rules[0].NoncurrentVersionTransitions, 1)
	require.Equal(t, int32(2), *config.Rules[0].NoncurrentVersionTransitions[0].NoncurrentDays)
	require.Equal(t, int32(3), *config.Rules[0].NoncurrentVersionTransitions[0].NewerNoncurrentVersions)
	require.Equal(t, "DEEP_ARCHIVE", config.Rules[0].NoncurrentVersionTransitions[0].StorageClass)

	response := convertLifecycleConfigurationToXML(config)
	require.Len(t, response.Rules[0].NoncurrentVersionTransitions, 1)
	require.Equal(t, int32(2), *response.Rules[0].NoncurrentVersionTransitions[0].NoncurrentDays)
	require.Equal(t, int32(3), *response.Rules[0].NoncurrentVersionTransitions[0].NewerNoncurrentVersions)
	require.Equal(t, "DEEP_ARCHIVE", response.Rules[0].NoncurrentVersionTransitions[0].StorageClass)
}

func TestLifecycleNoncurrentVersionExpirationToXML(t *testing.T) {
	testutils.SkipIfIntegration(t)

	config := &storage.BucketLifecycleConfiguration{Rules: []storage.LifecycleRule{{
		Status: storage.LifecycleRuleStatusEnabled,
		Filter: &storage.LifecycleFilter{},
		NoncurrentVersionExpiration: &storage.LifecycleNoncurrentVersionExpiration{
			NoncurrentDays:          ptrutils.ToPtr(int32(7)),
			NewerNoncurrentVersions: ptrutils.ToPtr(int32(1)),
		},
	}}}

	response := convertLifecycleConfigurationToXML(config)
	require.NotNil(t, response.Rules[0].NoncurrentVersionExpiration)
	require.Equal(t, int32(7), *response.Rules[0].NoncurrentVersionExpiration.NoncurrentDays)
	require.Equal(t, int32(1), *response.Rules[0].NoncurrentVersionExpiration.NewerNoncurrentVersions)
}
