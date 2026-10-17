package lua

import (
	"context"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
)

const benchmarkPolicy = `
local admins = { "AKIAADMIN1", "AKIAADMIN2", "AKIAADMIN3" }
local readers = { "AKIAREAD1", "AKIAREAD2" }

local function contains(list, v)
  for _, x in ipairs(list) do
    if x == v then return true end
  end
  return false
end

function authorizeRequest(request)
  if request:isAnonymous() then
    return request:isReadOnly() and request:bucketEquals("public")
  end
  local key = request.authorization.accessKeyId
  if contains(admins, key) then
    return true
  end
  if contains(readers, key) then
    return request:isReadOnly()
  end
  if request.httpRequest:isScheme("https") and request:keyHasPrefix("uploads/") then
    return request:isOperationIn({ "PutObject", "CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload" })
  end
  return false
end
`

func BenchmarkLuaAuthorizeRequest(b *testing.B) {
	authorizer, err := NewLuaAuthorizer(benchmarkPolicy)
	if err != nil {
		b.Fatal(err)
	}
	req := authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAREAD2"),
			AuthType:    "REST-HEADER",
		},
		Bucket: ptrutils.ToPtr("bucket"),
		Key:    ptrutils.ToPtr("some/key.txt"),
		HttpRequest: authorization.HTTPRequest{
			Method: "GET", Path: "/bucket/some/key.txt", Host: "localhost",
			Headers:     map[string][]string{"Authorization": {"AWS4..."}, "X-Amz-Date": {"20260101T000000Z"}, "X-Amz-Content-Sha256": {"UNSIGNED"}, "User-Agent": {"aws-sdk"}},
			QueryParams: map[string][]string{},
		},
	}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := authorizer.AuthorizeRequest(ctx, &req)
		if err != nil || d.Effect != authorization.Allow {
			b.Fatal(err, d)
		}
	}
}
