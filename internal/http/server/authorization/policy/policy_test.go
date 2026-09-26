package policy

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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

func TestRemainingRetentionDaysEnforcesObjectLimits(t *testing.T) {
	testutils.SkipIfIntegration(t)
	s := compileTestPolicy(t, `[
		{"Effect":"Allow","Action":["s3:PutObject","s3:PutObjectRetention"],"Resource":"*"},
		{"Effect":"Deny","Action":"s3:PutObjectRetention","Resource":"*","Condition":{"NumericGreaterThan":{"s3:object-lock-remaining-retention-days":"30"}}}
	]`)
	for _, operation := range []string{authorization.OperationPutObjectRetention, authorization.OperationPutObject, authorization.OperationCreateMultipartUpload} {
		for _, tc := range []struct {
			name      string
			remaining time.Duration
			want      authorization.Effect
		}{
			{"within limit", 29 * 24 * time.Hour, authorization.Allow},
			{"at limit", 30 * 24 * time.Hour, authorization.Allow},
			{"partial day over limit", 30*24*time.Hour + time.Hour, authorization.ExplicitDeny},
			{"over limit", 90 * 24 * time.Hour, authorization.ExplicitDeny},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				r := request(operation, "bucket", "key")
				r.ObjectLockMode = stringPointer("GOVERNANCE")
				r.ObjectLockRetainUntilDate = stringPointer(time.Now().Add(tc.remaining).UTC().Format(time.RFC3339Nano))
				d, err := s.AuthorizeRequest(context.Background(), r)
				require.NoError(t, err)
				require.Equal(t, tc.want, d.Effect)
			})
		}
	}
}

func TestRemainingRetentionDaysFailsClosedOnInvalidDate(t *testing.T) {
	testutils.SkipIfIntegration(t)
	s := compileTestPolicy(t, `{"Effect":"Allow","Action":"s3:PutObjectRetention","Resource":"*","Condition":{"NumericLessThanEquals":{"s3:object-lock-remaining-retention-days":"30"}}}`)
	r := request(authorization.OperationPutObjectRetention, "bucket", "key")
	r.ObjectLockRetainUntilDate = stringPointer("invalid")
	d, err := s.AuthorizeRequest(context.Background(), r)
	require.Error(t, err)
	require.NotEqual(t, authorization.Allow, d.Effect)
}

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

func TestCompileRejectsInvalidConditionValues(t *testing.T) {
	testutils.SkipIfIntegration(t)

	for _, tc := range []struct {
		name, operator, key, value string
	}{
		{"numeric", "NumericLessThan", "s3:max-keys", `"many"`},
		{"null string", "StringEquals", "s3:RequestObjectTag/team", `null`},
		{"null array element", "StringEquals", "s3:RequestObjectTag/team", `["storage",null]`},
		{"non-finite numeric", "NumericLessThan", "s3:max-keys", `"NaN"`},
		{"date", "DateGreaterThan", "aws:CurrentTime", `"tomorrow"`},
		{"boolean", "Bool", "aws:SecureTransport", `"yes"`},
		{"IP address", "IpAddress", "aws:SourceIp", `"localhost"`},
		{"Null value count", "Null", "s3:VersionId", `["true","false"]`},
		{"Null IfExists", "NullIfExists", "s3:VersionId", `"true"`},
		{"Null set operator", "ForAnyValue:Null", "s3:VersionId", `"true"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := []byte(`{"schemaVersion":1,"policies":{"test":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"` + tc.operator + `":{"` + tc.key + `":` + tc.value + `}}}}},"bindings":[]}`)
			_, err := Compile(data)
			require.Error(t, err)
		})
	}
}

func TestReloadRejectsNullConditionValues(t *testing.T) {
	testutils.SkipIfIntegration(t)
	path := filepath.Join(t.TempDir(), "policies.json")
	valid := `{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:PutObject","Resource":"*","Condition":{"StringEquals":{"s3:RequestObjectTag/team":"storage"}}}}},"bindings":[{"policy":"p","subjects":[{"type":"principal","accountId":"a","principalId":"p"}]}]}`
	require.NoError(t, os.WriteFile(path, []byte(valid), 0600))
	a, err := NewAuthorizer(path, 0)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, a.Close()) })
	for _, value := range []string{`null`, `["storage",null]`} {
		invalid := strings.Replace(valid, `"s3:RequestObjectTag/team":"storage"`, `"s3:RequestObjectTag/team":`+value, 1)
		require.NoError(t, os.WriteFile(path, []byte(invalid), 0600))
		require.Error(t, a.Reload())
		r := request(authorization.OperationPutObject, "bucket", "key")
		r.RequestObjectTags = map[string]string{"team": ""}
		d, err := a.AuthorizeRequest(context.Background(), r)
		require.NoError(t, err)
		require.Equal(t, authorization.ImplicitDeny, d.Effect)
	}
}

func TestCompileAcceptsValidTypedConditionValues(t *testing.T) {
	testutils.SkipIfIntegration(t)

	data := []byte(`{"schemaVersion":1,"policies":{"test":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"NumericLessThan":{"s3:max-keys":"100"},"DateGreaterThan":{"aws:CurrentTime":"2026-01-01T00:00:00Z"},"Bool":{"aws:SecureTransport":"true"},"IpAddress":{"aws:SourceIp":["192.0.2.1","2001:db8::/32"]},"Null":{"s3:VersionId":"false"}}}}},"bindings":[]}`)
	_, err := Compile(data)
	require.NoError(t, err)
}

func TestAuthorizerAppliesTrustedForwardedHeaders(t *testing.T) {
	testutils.SkipIfIntegration(t)

	path := filepath.Join(t.TempDir(), "policies.json")
	data := `{"schemaVersion":1,"policies":{"test":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"IpAddress":{"aws:SourceIp":"198.51.100.0/24"},"Bool":{"aws:SecureTransport":"true"}}}}},"bindings":[{"policy":"test","subjects":[{"type":"principal","accountId":"a","principalId":"p"}]}]}`
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))
	a, err := NewAuthorizerWithOptions(path, 0, Options{TrustForwardedHeaders: true, TrustedProxyCIDRs: []string{"10.0.0.0/8"}})
	require.NoError(t, err)
	defer a.Close()

	r := request(authorization.OperationGetObject, "bucket", "key")
	r.HttpRequest = authorization.HTTPRequest{
		RemoteIP: stringPointer("10.1.2.3"),
		Scheme:   "http",
		Headers: map[string][]string{
			"X-Forwarded-For":   {"198.51.100.7"},
			"X-Forwarded-Proto": {"https"},
		},
	}
	d, err := a.AuthorizeRequest(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, authorization.Allow, d.Effect)
}

func TestAuthorizerIgnoresForwardedHeadersFromUntrustedProxy(t *testing.T) {
	testutils.SkipIfIntegration(t)

	path := filepath.Join(t.TempDir(), "policies.json")
	data := `{"schemaVersion":1,"policies":{"test":{"Version":"2012-10-17","Statement":{"Effect":"Allow","Action":"s3:GetObject","Resource":"*","Condition":{"IpAddress":{"aws:SourceIp":"198.51.100.0/24"}}}}},"bindings":[{"policy":"test","subjects":[{"type":"principal","accountId":"a","principalId":"p"}]}]}`
	require.NoError(t, os.WriteFile(path, []byte(data), 0o600))
	a, err := NewAuthorizerWithOptions(path, 0, Options{TrustForwardedHeaders: true, TrustedProxyCIDRs: []string{"10.0.0.0/8"}})
	require.NoError(t, err)
	defer a.Close()

	r := request(authorization.OperationGetObject, "bucket", "key")
	r.HttpRequest = authorization.HTTPRequest{
		RemoteIP: stringPointer("192.0.2.5"),
		Scheme:   "http",
		Headers:  map[string][]string{"X-Forwarded-For": {"198.51.100.7"}},
	}
	d, err := a.AuthorizeRequest(context.Background(), r)
	require.NoError(t, err)
	require.Equal(t, authorization.ImplicitDeny, d.Effect)
}
