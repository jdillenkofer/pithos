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

func TestLifecycleTransitionElementsAreRecognizedForRejection(t *testing.T) {
	testutils.SkipIfIntegration(t)

	body := `<LifecycleConfiguration><Rule><Status>Enabled</Status><Filter></Filter><Transition><Days>1</Days><StorageClass>GLACIER</StorageClass></Transition><NoncurrentVersionTransition><NoncurrentDays>1</NoncurrentDays><StorageClass>GLACIER</StorageClass></NoncurrentVersionTransition></Rule></LifecycleConfiguration>`

	var request LifecycleConfiguration
	require.NoError(t, xml.Unmarshal([]byte(body), &request))
	require.Len(t, request.Rules, 1)
	require.Len(t, request.Rules[0].Transitions, 1)
	require.Len(t, request.Rules[0].NoncurrentVersionTransitions, 1)
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
