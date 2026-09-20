package policy

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func compileTestPolicy(t *testing.T, statements string) *Snapshot {
	t.Helper()
	data := []byte(`{"schemaVersion":1,"policies":{"test":{"Version":"2012-10-17","Statement":` + statements + `}},"bindings":[{"policy":"test","subjects":[{"type":"principal","accountId":"a","principalId":"p"}]}]}`)
	s, err := Compile(data)
	require.NoError(t, err)
	return s
}

func TestReloadRetainsLastValidSnapshot(t *testing.T) {
	testutils.SkipIfIntegration(t)

	path := filepath.Join(t.TempDir(), "policies.json")
	valid := `{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}}},"bindings":[{"policy":"p","subjects":[{"type":"principal","accountId":"a","principalId":"p"}]}]}`
	require.NoError(t, os.WriteFile(path, []byte(valid), 0600))
	a, err := NewAuthorizer(path, 0)
	require.NoError(t, err)
	defer a.Close()
	require.NoError(t, os.WriteFile(path, []byte(`{invalid`), 0600))
	require.Error(t, a.Reload())
	d, err := a.AuthorizeRequest(context.Background(), request(authorization.OperationGetObject, "bucket", "key"))
	require.NoError(t, err)
	require.Equal(t, authorization.Allow, d.Effect)
}

func request(operation, bucket, key string) *authorization.Request {
	return &authorization.Request{Operation: operation, Bucket: &bucket, Key: &key, Authorization: authorization.Authorization{AccountId: stringPointer("a"), PrincipalId: stringPointer("p")}}
}

func stringPointer(v string) *string { return &v }

func TestDecisionMatrix(t *testing.T) {
	testutils.SkipIfIntegration(t)

	allow := `{"Sid":"allow","Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}`
	deny := `{"Sid":"deny","Effect":"Deny","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/private/*"}`
	for _, tc := range []struct {
		name, statements, key string
		effect                authorization.Effect
	}{
		{"implicit deny", allow, "other", authorization.ImplicitDeny},
		{"allow", allow, "public/a", authorization.Allow},
		{"explicit deny wins", `[` + allow + `,` + deny + `]`, "private/a", authorization.ExplicitDeny},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := authorization.OperationGetObject
			if tc.name == "implicit deny" {
				op = authorization.OperationPutObject
			}
			d, err := compileTestPolicy(t, tc.statements).AuthorizeRequest(context.Background(), request(op, "bucket", tc.key))
			require.NoError(t, err)
			require.Equal(t, tc.effect, d.Effect)
		})
	}
}

func TestCopyRequiresSourceAndDestination(t *testing.T) {
	testutils.SkipIfIntegration(t)

	s := compileTestPolicy(t, `[{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::source/*"},{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::destination/*"}]`)
	r := request(authorization.OperationCopyObject, "destination", "copy")
	r.SourceBucket = stringPointer("source")
	r.SourceKey = stringPointer("original")
	d, err := s.AuthorizeRequest(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, authorization.Allow, d.Effect)
	r.SourceKey = stringPointer("missing")
	s = compileTestPolicy(t, `{"Effect":"Allow","Action":"s3:PutObject","Resource":"arn:aws:s3:::destination/*"}`)
	d, err = s.AuthorizeRequest(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, authorization.ImplicitDeny, d.Effect)
}

func TestStrictJSONAndReferences(t *testing.T) {
	testutils.SkipIfIntegration(t)

	_, err := Compile([]byte(`{"schemaVersion":1,"schemaVersion":1,"policies":{},"bindings":[]}`))
	require.ErrorContains(t, err, "duplicate")
	_, err = Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Principal":"*"}}},"bindings":[]}`))
	require.Error(t, err)
	_, err = Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}}},"bindings":[{"policy":"missing","subjects":[{"type":"anonymous"}]}]}`))
	require.ErrorContains(t, err, "unknown policy")
}

func TestConditionIfExistsAndExplicitDeny(t *testing.T) {
	testutils.SkipIfIntegration(t)

	s := compileTestPolicy(t, `[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"},{"Effect":"Deny","Action":"s3:GetObject","Resource":"*","Condition":{"StringNotEqualsIfExists":{"s3:ExistingObjectTag/team":"storage"}}}]`)
	r := request(authorization.OperationGetObject, "bucket", "key")
	r.ResolveExistingObjectTags = func(context.Context) (map[string]string, error) { return map[string]string{"team": "finance"}, nil }
	d, err := s.AuthorizeRequest(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, authorization.ExplicitDeny, d.Effect)
}

func TestActionMatchingIsCaseInsensitive(t *testing.T) {
	testutils.SkipIfIntegration(t)

	s := compileTestPolicy(t, `{"Effect":"Allow","Action":"S3:gEtObJeCt","Resource":"*"}`)
	d, err := s.AuthorizeRequest(context.Background(), request(authorization.OperationGetObject, "bucket", "key"))
	require.NoError(t, err)
	require.Equal(t, authorization.Allow, d.Effect)
}

func TestResourceMatchingIsCaseSensitive(t *testing.T) {
	testutils.SkipIfIntegration(t)

	s := compileTestPolicy(t, `{"Effect":"Allow","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/private/*"}`)
	d, err := s.AuthorizeRequest(context.Background(), request(authorization.OperationGetObject, "bucket", "Private/key"))
	require.NoError(t, err)
	require.Equal(t, authorization.ImplicitDeny, d.Effect)
}

func TestStringLikeMatchingIsCaseSensitive(t *testing.T) {
	testutils.SkipIfIntegration(t)

	s := compileTestPolicy(t, `{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"StringLike":{"aws:UserAgent":"Pithos/*"}}}`)
	r := request(authorization.OperationGetObject, "bucket", "key")
	r.HttpRequest.Headers = map[string][]string{"User-Agent": {"pithos/client"}}
	d, err := s.AuthorizeRequest(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, authorization.ImplicitDeny, d.Effect)
}

func TestNegatedConditionMatchesMissingContextKey(t *testing.T) {
	testutils.SkipIfIntegration(t)

	s := compileTestPolicy(t, `[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"},{"Effect":"Deny","Action":"s3:GetObject","Resource":"*","Condition":{"StringNotEquals":{"s3:ExistingObjectTag/team":"storage"}}}]`)
	d, err := s.AuthorizeRequest(context.Background(), request(authorization.OperationGetObject, "bucket", "key"))
	require.NoError(t, err)
	require.Equal(t, authorization.ExplicitDeny, d.Effect)
}

func TestPositiveConditionDoesNotMatchMissingContextKey(t *testing.T) {
	testutils.SkipIfIntegration(t)

	s := compileTestPolicy(t, `{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"StringEquals":{"s3:ExistingObjectTag/team":"storage"}}}`)
	d, err := s.AuthorizeRequest(context.Background(), request(authorization.OperationGetObject, "bucket", "key"))
	require.NoError(t, err)
	require.Equal(t, authorization.ImplicitDeny, d.Effect)
}
