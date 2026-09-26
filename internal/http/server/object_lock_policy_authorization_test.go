package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server/authentication"
	"github.com/jdillenkofer/pithos/internal/http/server/authorization/policy"
	"github.com/jdillenkofer/pithos/internal/storage"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

type policyLockReadStorage struct {
	accountStorage
	object      *storage.Object
	tagVersions []*string
}

func (s *policyLockReadStorage) HeadObject(context.Context, storage.BucketName, storage.ObjectKey, *storage.HeadObjectOptions) (*storage.Object, error) {
	return s.object, nil
}

func (s *policyLockReadStorage) GetObject(context.Context, storage.BucketName, storage.ObjectKey, []storage.ByteRange, *storage.GetObjectOptions) (*storage.Object, []io.ReadCloser, error) {
	return s.object, []io.ReadCloser{io.NopCloser(strings.NewReader("data"))}, nil
}

func (s *policyLockReadStorage) GetObjectTagging(_ context.Context, _ storage.BucketName, _ storage.ObjectKey, opts *storage.ObjectTaggingOptions) (map[string]string, error) {
	s.tagVersions = append(s.tagVersions, opts.VersionID)
	return map[string]string{"team": "storage"}, nil
}

func TestObjectLockHeadersRespectResourceAccountPolicy(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, method := range []string{http.MethodGet, http.MethodHead} {
		for _, subject := range []string{"principal", "anonymous"} {
			for _, tc := range []struct {
				name, statements string
				expose           bool
			}{
				{"account deny", `[
					{"Effect":"Allow","Action":"s3:GetObject*","Resource":"*"},
					{"Effect":"Deny","Action":["s3:GetObjectRetention","s3:GetObjectLegalHold"],"Resource":"*","Condition":{"StringEquals":{"aws:ResourceAccount":"owner"}}}
				]`, false},
				{"account allow", `[
					{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"},
					{"Effect":"Allow","Action":["s3:GetObjectRetention","s3:GetObjectLegalHold"],"Resource":"*","Condition":{"StringEquals":{"aws:ResourceAccount":"owner","s3:VersionId":"returned-version","s3:ExistingObjectTag/team":"storage"}}}
				]`, true},
				{"implicit deny", `{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}`, false},
			} {
				t.Run(method+"/"+subject+"/"+tc.name, func(t *testing.T) {
					binding := `{"type":"anonymous"}`
					if subject == "principal" {
						binding = `{"type":"principal","accountId":"owner","principalId":"reader"}`
					}
					snapshot, err := policy.Compile([]byte(`{"schemaVersion":1,"policies":{"p":{"Version":"2012-10-17","Statement":` + tc.statements + `}},"bindings":[{"policy":"p","subjects":[` + binding + `]}]}`))
					require.NoError(t, err)
					hold := storage.LegalHoldOn
					until := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
					backend := &policyLockReadStorage{
						accountStorage: accountStorage{owners: map[string]string{"bucket": "owner"}},
						object: &storage.Object{Size: 4, VersionID: stringPtr("returned-version"), ObjectLock: storage.ObjectLock{
							Retention: &storage.ObjectRetention{Mode: storage.RetentionModeCompliance, RetainUntilDate: until},
							LegalHold: &hold,
						}},
					}
					s := &Server{storage: backend, requestAuthorizer: snapshot, tracer: otel.Tracer("policy-lock-read-test")}
					r := httptest.NewRequest(method, "/bucket/key", nil)
					r.SetPathValue(bucketPath, "bucket")
					r.SetPathValue(keyPath, "key")
					if subject == "principal" {
						r = r.WithContext(authentication.WithRequestAuthentication(r.Context(), authentication.RequestAuthentication{
							Authenticated: true, Identity: &authentication.AuthenticatedIdentity{AccessKeyID: "key", AccountID: "owner", PrincipalID: "reader"},
						}))
					}
					w := httptest.NewRecorder()
					if method == http.MethodGet {
						s.getObjectHandler(w, r)
					} else {
						s.headObjectHandler(w, r)
					}
					require.Equal(t, http.StatusOK, w.Code)
					if method == http.MethodGet {
						require.Equal(t, "data", w.Body.String())
					}
					if tc.expose {
						require.Equal(t, "COMPLIANCE", w.Header().Get("x-amz-object-lock-mode"))
						require.Equal(t, until.Format(time.RFC3339Nano), w.Header().Get("x-amz-object-lock-retain-until-date"))
						require.Equal(t, "ON", w.Header().Get("x-amz-object-lock-legal-hold"))
						require.Len(t, backend.tagVersions, 2)
						for _, version := range backend.tagVersions {
							require.NotNil(t, version)
							require.Equal(t, "returned-version", *version)
						}
					} else {
						require.Empty(t, w.Header().Get("x-amz-object-lock-mode"))
						require.Empty(t, w.Header().Get("x-amz-object-lock-retain-until-date"))
						require.Empty(t, w.Header().Get("x-amz-object-lock-legal-hold"))
					}
				})
			}
		}
	}
}
