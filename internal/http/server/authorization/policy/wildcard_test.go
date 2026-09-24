package policy

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestResourceWildcardsMatchAllValidKeyCharacters(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		pattern, key string
	}{
		{"*", "private/secret\nfile"},
		{"arn:aws:s3:::bucket/private/*", "private/secret\nfile"},
		{"arn:aws:s3:::bucket/private/?", "private/\n"},
		{"arn:aws:s3:::bucket/private/?", "private/ä"},
	} {
		t.Run(tc.pattern+"/"+tc.key, func(t *testing.T) {
			_, err := storage.NewObjectKey(tc.key)
			require.NoError(t, err)
			resource, err := json.Marshal(tc.pattern)
			require.NoError(t, err)
			s := compileTestPolicy(t, `{"Effect":"Allow","Action":"s3:GetObject","Resource":`+string(resource)+`}`)
			d, err := s.AuthorizeRequest(context.Background(), request(authorization.OperationGetObject, "bucket", tc.key))
			require.NoError(t, err)
			require.Equal(t, authorization.Allow, d.Effect)
		})
	}
}

func TestWildcardDenyOverridesExactAllowForNewlineKey(t *testing.T) {
	testutils.SkipIfIntegration(t)
	s := compileTestPolicy(t, `[
		{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/private/secret\nfile"},
		{"Effect":"Deny","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/private/*"}
	]`)
	d, err := s.AuthorizeRequest(context.Background(), request(authorization.OperationGetObject, "bucket", "private/secret\nfile"))
	require.NoError(t, err)
	require.Equal(t, authorization.ExplicitDeny, d.Effect)
}

func TestLikeConditionsMatchNewlines(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		operator string
		want     authorization.Effect
	}{
		{"StringLike", authorization.Allow},
		{"StringNotLike", authorization.ImplicitDeny},
		{"ArnLike", authorization.Allow},
		{"ArnNotLike", authorization.ImplicitDeny},
	} {
		t.Run(tc.operator, func(t *testing.T) {
			s := compileTestPolicy(t, `{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*","Condition":{"`+tc.operator+`":{"s3:prefix":"private/*"}}}`)
			r := request(authorization.OperationListObjects, "bucket", "")
			r.Key = nil
			r.HttpRequest.QueryParams = map[string][]string{"prefix": {"private/secret\nfile"}}
			d, err := s.AuthorizeRequest(context.Background(), r)
			require.NoError(t, err)
			require.Equal(t, tc.want, d.Effect)
		})
	}
}
