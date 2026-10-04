package server

import (
	"encoding/xml"
	"testing"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListPartsResultSerializesOwnerAndInitiator(t *testing.T) {
	result := ListPartsResult{
		Bucket:    "bucket",
		Key:       "key",
		UploadId:  "upload",
		Parts:     []*PartResult{},
		Owner:     identityResult(&storage.ObjectIdentity{AccountID: "account-a"}),
		Initiator: identityResult(&storage.ObjectIdentity{AccountID: "account-a", PrincipalID: "writer"}),
	}

	data, err := xml.Marshal(result)
	require.NoError(t, err)
	assert.Contains(t, string(data), "<Owner><ID>account-a</ID></Owner>")
	assert.Contains(t, string(data), "<Initiator><ID>arn:pithos:iam::account-a:principal/writer</ID></Initiator>")
	assert.NotContains(t, string(data), "DisplayName")
}

func TestListPartsResultOmitsAbsentIdentities(t *testing.T) {
	result := ListPartsResult{
		Bucket:   "bucket",
		Key:      "key",
		UploadId: "upload",
		Parts:    []*PartResult{},
	}

	data, err := xml.Marshal(result)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "<Owner>")
	assert.NotContains(t, string(data), "<Initiator>")
}

func TestIdentityResultMapsAccountAndPrincipal(t *testing.T) {
	assert.Nil(t, identityResult(nil))
	assert.Nil(t, identityResult(&storage.ObjectIdentity{}))
	assert.Equal(t, "account-a", identityResult(&storage.ObjectIdentity{AccountID: "account-a"}).Id)
	assert.Equal(t, "arn:pithos:iam::account-a:principal/writer", identityResult(&storage.ObjectIdentity{AccountID: "account-a", PrincipalID: "writer"}).Id)
}
