package lua

import (
	"context"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func TestAuthorizationAlwaysDenied(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return false
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)
	request := authorization.Request{
		Operation: authorization.OperationPutObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.False(t, authorized)
	assert.Nil(t, err)
}

func TestAuthorizationAlwaysAllowed(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return true
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)
	request := authorization.Request{
		Operation: authorization.OperationPutObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestOperationCorrectlyPassedThrough(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.operation == "PutObject"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)
	allowedRequest := authorization.Request{
		Operation: authorization.OperationPutObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &allowedRequest)
	assert.True(t, authorized)
	assert.Nil(t, err)

	deniedRequest := authorization.Request{
		Operation: authorization.OperationCreateBucket,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err = authorizer.AuthorizeRequest(context.Background(), &deniedRequest)
	assert.False(t, authorized)
	assert.Nil(t, err)
}

func TestNestedStructWorks(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  authorization = request.authorization
	  return authorization.accessKeyId == "AKIAIOSFODNN7EXAMPLE" and request.operation == "PutObject"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)
	allowedRequest := authorization.Request{
		Operation: authorization.OperationPutObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &allowedRequest)
	assert.True(t, authorized)
	assert.Nil(t, err)

	deniedRequest := authorization.Request{
		Operation: authorization.OperationCreateBucket,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err = authorizer.AuthorizeRequest(context.Background(), &deniedRequest)
	assert.False(t, authorized)
	assert.Nil(t, err)
}

func TestIsAnonymousReturnsTrueWhenAccessKeyIdIsNil(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:isAnonymous()
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: nil,
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestIsAnonymousReturnsFalseWhenAccessKeyIdIsSet(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:isAnonymous()
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.False(t, authorized)
	assert.Nil(t, err)
}

func TestAccessKeyIdIsNilInLuaWhenAnonymous(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.authorization.accessKeyId == nil
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: nil,
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestAccessKeyIdIsSetInLuaWhenAuthenticated(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.authorization.accessKeyId == "AKIAIOSFODNN7EXAMPLE"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestIsReadOnlyReturnsTrueForReadOperations(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:isReadOnly()
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	readOperations := []string{
		authorization.OperationListBuckets,
		authorization.OperationHeadBucket,
		authorization.OperationHeadObject,
		authorization.OperationListMultipartUploads,
		authorization.OperationListObjects,
		authorization.OperationListParts,
		authorization.OperationGetObject,
		authorization.OperationGetBucketWebsite,
	}
	for _, op := range readOperations {
		request := authorization.Request{
			Operation: op,
			Authorization: authorization.Authorization{
				AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
			},
		}
		authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
		assert.True(t, authorized, "expected isReadOnly() == true for operation %s", op)
		assert.Nil(t, err)
	}
}

func TestIsReadOnlyReturnsFalseForWriteOperations(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:isReadOnly()
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	writeOperations := []string{
		authorization.OperationCreateBucket,
		authorization.OperationDeleteBucket,
		authorization.OperationCreateMultipartUpload,
		authorization.OperationCompleteMultipartUpload,
		authorization.OperationUploadPart,
		authorization.OperationPutObject,
		authorization.OperationAppendObject,
		authorization.OperationAbortMultipartUpload,
		authorization.OperationDeleteObject,
		authorization.OperationDeleteObjects,
		authorization.OperationPutBucketWebsite,
		authorization.OperationDeleteBucketWebsite,
	}
	for _, op := range writeOperations {
		request := authorization.Request{
			Operation: op,
			Authorization: authorization.Authorization{
				AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
			},
		}
		authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
		assert.False(t, authorized, "expected isReadOnly() == false for operation %s", op)
		assert.Nil(t, err)
	}
}

func TestBucketAndKeyPassedThroughToLua(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.bucket == "my-bucket" and request.key == "my-key"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: ptrutils.ToPtr("my-bucket"),
		Key:    ptrutils.ToPtr("my-key"),
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestBucketAndKeyAreNilInLuaWhenNotSet(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.bucket == nil and request.key == nil
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationListBuckets,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestAnonymousGetObjectAllowedByAuthorizer(t *testing.T) {
	testutils.SkipIfIntegration(t)

	// Simulates the example authorizer from the design: allow anonymous GetObject,
	// require a specific key for everything else.
	luaCode := `
	function authorizeRequest(request)
	  if request:isAnonymous() and request.operation == "GetObject" then
	    return true
	  end
	  return request.authorization.accessKeyId == "AKIAIOSFODNN7EXAMPLE"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	anonymousGet := authorization.Request{
		Operation:     authorization.OperationGetObject,
		Authorization: authorization.Authorization{AccessKeyId: nil},
		Bucket:        ptrutils.ToPtr("public-bucket"),
		Key:           ptrutils.ToPtr("public-object"),
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &anonymousGet)
	assert.True(t, authorized)
	assert.Nil(t, err)

	anonymousPut := authorization.Request{
		Operation:     authorization.OperationPutObject,
		Authorization: authorization.Authorization{AccessKeyId: nil},
		Bucket:        ptrutils.ToPtr("public-bucket"),
		Key:           ptrutils.ToPtr("some-object"),
	}
	authorized, err = authorizer.AuthorizeRequest(context.Background(), &anonymousPut)
	assert.False(t, authorized)
	assert.Nil(t, err)

	authenticatedGet := authorization.Request{
		Operation:     authorization.OperationGetObject,
		Authorization: authorization.Authorization{AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE")},
		Bucket:        ptrutils.ToPtr("private-bucket"),
		Key:           ptrutils.ToPtr("private-object"),
	}
	authorized, err = authorizer.AuthorizeRequest(context.Background(), &authenticatedGet)
	assert.True(t, authorized)
	assert.Nil(t, err)

	wrongKey := authorization.Request{
		Operation:     authorization.OperationGetObject,
		Authorization: authorization.Authorization{AccessKeyId: ptrutils.ToPtr("WRONGKEY")},
		Bucket:        ptrutils.ToPtr("private-bucket"),
		Key:           ptrutils.ToPtr("private-object"),
	}
	authorized, err = authorizer.AuthorizeRequest(context.Background(), &wrongKey)
	assert.False(t, authorized)
	assert.Nil(t, err)
}

func TestHTTPRequestFieldsPassedThroughToLua(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.httpRequest.method == "GET" and
	    request.httpRequest.path == "/my-bucket/my-key" and
	    request.httpRequest.query == "partNumber=2&uploadId=abc" and
	    request.httpRequest.queryParams["partNumber"][1] == "2" and
	    request.httpRequest.queryParams["uploadId"][1] == "abc" and
	    request.httpRequest.headers["X-Test"][1] == "first" and
	    request.httpRequest.headers["X-Test"][2] == "second" and
	    request.httpRequest.headers["Content-Type"][1] == "application/octet-stream" and
	    request.httpRequest.host == "s3.localhost:8080" and
	    request.httpRequest.proto == "HTTP/1.1" and
	    request.httpRequest.contentLength == 42 and
	    request.httpRequest.remoteAddr == "203.0.113.42:58888" and
	    request.httpRequest.remoteIP == "203.0.113.42" and
	    request.httpRequest.clientIP == "203.0.113.42" and
	    request.httpRequest.scheme == "http"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: ptrutils.ToPtr("my-bucket"),
		Key:    ptrutils.ToPtr("my-key"),
		HttpRequest: authorization.HTTPRequest{
			Method:        "GET",
			Path:          "/my-bucket/my-key",
			Query:         "partNumber=2&uploadId=abc",
			Host:          "s3.localhost:8080",
			Proto:         "HTTP/1.1",
			RemoteAddr:    "203.0.113.42:58888",
			RemoteIP:      ptrutils.ToPtr("203.0.113.42"),
			ClientIP:      ptrutils.ToPtr("203.0.113.42"),
			Scheme:        "http",
			ContentLength: ptrutils.ToPtr(42),
			QueryParams: map[string][]string{
				"partNumber": []string{"2"},
				"uploadId":   []string{"abc"},
			},
			Headers: map[string][]string{
				"X-Test":       []string{"first", "second"},
				"Content-Type": []string{"application/octet-stream"},
			},
		},
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestTrustedForwardedHeadersAppliedForClientIPAndScheme(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.httpRequest.clientIP == "198.51.100.7" and request.httpRequest.scheme == "https"
	end
	`
	authorizer, err := NewLuaAuthorizerWithOptions(luaCode, Options{
		TrustForwardedHeaders: true,
		TrustedProxyCIDRs:     []string{"10.0.0.0/8"},
	})
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		HttpRequest: authorization.HTTPRequest{
			RemoteIP: ptrutils.ToPtr("10.1.2.3"),
			Scheme:   "http",
			Headers: map[string][]string{
				"CF-Connecting-IP":  []string{"198.51.100.7"},
				"X-Forwarded-Proto": []string{"https"},
			},
		},
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestForwardedHeadersIgnoredForUntrustedProxy(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.httpRequest.clientIP == "192.0.2.5" and request.httpRequest.scheme == "http"
	end
	`
	authorizer, err := NewLuaAuthorizerWithOptions(luaCode, Options{
		TrustForwardedHeaders: true,
		TrustedProxyCIDRs:     []string{"10.0.0.0/8"},
	})
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		HttpRequest: authorization.HTTPRequest{
			RemoteIP: ptrutils.ToPtr("192.0.2.5"),
			Scheme:   "http",
			Headers: map[string][]string{
				"X-Forwarded-For":   []string{"198.51.100.7"},
				"X-Forwarded-Proto": []string{"https"},
			},
		},
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestHTTPRequestIsMethodAndHasHeader(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.httpRequest:isMethod("GET") and request.httpRequest:hasHeader("x-api-key")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		HttpRequest: authorization.HTTPRequest{
			Method: "GET",
			Headers: map[string][]string{
				"X-Api-Key": []string{"my-key"},
			},
		},
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestHTTPRequestHeaderEqualsAndQueryParamEquals(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.httpRequest:headerEquals("x-api-key", "my-key") and
	    request.httpRequest:queryParamEquals("uploadId", "upload-123")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		HttpRequest: authorization.HTTPRequest{
			Method: "GET",
			QueryParams: map[string][]string{
				"uploadId": []string{"upload-123", "upload-456"},
			},
			Headers: map[string][]string{
				"X-Api-Key": []string{"my-key"},
			},
		},
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestHTTPRequestHeaderEqualsAndQueryParamEqualsReturnFalseWhenMissingOrDifferent(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request.httpRequest:headerEquals("x-api-key", "my-key") and
	    request.httpRequest:queryParamEquals("uploadId", "upload-123")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		HttpRequest: authorization.HTTPRequest{
			Method: "GET",
			QueryParams: map[string][]string{
				"uploadId": []string{"wrong"},
			},
			Headers: map[string][]string{
				"X-Api-Key": []string{"wrong"},
			},
		},
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.False(t, authorized)
	assert.Nil(t, err)
}

func TestIsOperationReturnsTrueWhenOperationMatches(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:isOperation("GetObject")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestIsOperationReturnsFalseWhenOperationDoesNotMatch(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:isOperation("PutObject")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.False(t, authorized)
	assert.Nil(t, err)
}

func TestAuthorizeListBucketFallsBackToAllowWhenHookMissing(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return true
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{Operation: authorization.OperationListBuckets}
	authorized, err := authorizer.AuthorizeListBucket(context.Background(), &request, "my-bucket")
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestAuthorizeListObjectUsesOptionalHook(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return true
	end

	function authorizeListObject(request, key)
	  return key == "public/readme.txt"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{Operation: authorization.OperationListObjects}
	authorized, err := authorizer.AuthorizeListObject(context.Background(), &request, "public/readme.txt")
	assert.True(t, authorized)
	assert.Nil(t, err)

	authorized, err = authorizer.AuthorizeListObject(context.Background(), &request, "private/secret.txt")
	assert.False(t, authorized)
	assert.Nil(t, err)
}

func TestAuthorizeDeleteObjectEntryFallsBackToAllowWhenHookMissing(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return true
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{Operation: authorization.OperationDeleteObjects}
	authorized, err := authorizer.AuthorizeDeleteObjectEntry(context.Background(), &request, "secret.txt")
	assert.True(t, authorized)
	assert.Nil(t, err)
}

func TestAuthorizeListMultipartUploadUsesOptionalHook(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return true
	end

	function authorizeListMultipartUpload(request, key, uploadId)
	  return key == "allowed/file.txt" and uploadId == "upload-1"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{Operation: authorization.OperationListMultipartUploads}
	authorized, err := authorizer.AuthorizeListMultipartUpload(context.Background(), &request, "allowed/file.txt", "upload-1")
	assert.True(t, authorized)
	assert.Nil(t, err)

	authorized, err = authorizer.AuthorizeListMultipartUpload(context.Background(), &request, "allowed/file.txt", "upload-2")
	assert.False(t, authorized)
	assert.Nil(t, err)
}

func TestAuthorizeListPartUsesOptionalHook(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return true
	end

	function authorizeListPart(request, partNumber)
	  return partNumber == 1
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := authorization.Request{Operation: authorization.OperationListParts}
	authorized, err := authorizer.AuthorizeListPart(context.Background(), &request, 1)
	assert.True(t, authorized)
	assert.Nil(t, err)

	authorized, err = authorizer.AuthorizeListPart(context.Background(), &request, 2)
	assert.False(t, authorized)
	assert.Nil(t, err)
}

func TestComprehensiveRequestAndHTTPRequestHelpers(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  if request.operation ~= "GetObject" then
	    return true
	  end
	  assert(request:hasAccessKeyId(), "hasAccessKeyId")
	  assert(request:accessKeyIdEquals("AKIAIOSFODNN7EXAMPLE"), "accessKeyIdEquals")
	  assert(request:accessKeyIdIn({"AKIAIOSFODNN7EXAMPLE", "OTHER"}), "accessKeyIdIn")
	  assert(request:isOperation("GetObject"), "isOperation")
	  assert(request:isOperationIn({"PutObject", "GetObject"}), "isOperationIn")
	  assert(request:isReadOnly(), "isReadOnly")
	  assert(not request:isWriteOperation(), "isWriteOperation")
	  assert(not request:isAnonymous(), "isAnonymous")
	  assert(request:bucketEquals("my-bucket"), "bucketEquals")
	  assert(request:keyHasPrefix("public/"), "keyHasPrefix")
	  assert(request:keyHasSuffix(".txt"), "keyHasSuffix")
	  assert(request.httpRequest:isMethod("get"), "httpRequest.isMethod")
	  assert(request.httpRequest:hasHeader("X-Test"), "httpRequest.hasHeader")
	  assert(request.httpRequest:header("X-Test") == "a", "httpRequest.header")
	  assert(request.httpRequest:headerEquals("X-Test", "b"), "httpRequest.headerEquals")
	  assert(request.httpRequest:queryParam("uploadId") == "upload-123", "httpRequest.queryParam")
	  assert(request.httpRequest:hasQueryParam("uploadId"), "httpRequest.hasQueryParam")
	  assert(request.httpRequest:queryParamEquals("uploadId", "upload-456"), "httpRequest.queryParamEquals")
	  assert(request.httpRequest:pathEquals("/my-bucket/public/file.txt"), "httpRequest.pathEquals")
	  assert(request.httpRequest:pathHasPrefix("/my-bucket/public/"), "httpRequest.pathHasPrefix")
	  assert(request.httpRequest:hostEquals("assets.example.com"), "httpRequest.hostEquals")
	  assert(request.httpRequest:hostHasSuffix("example.com"), "httpRequest.hostHasSuffix")
	  assert(request.httpRequest:isScheme("https"), "httpRequest.isScheme")
	  assert(request.httpRequest:isProto("http/1.1"), "httpRequest.isProto")
	  assert(request.httpRequest:clientIPInCIDR("198.51.100.0/24"), "httpRequest.clientIPInCIDR")
	  assert(request.httpRequest:clientIPInCIDRs({"203.0.113.0/24", "198.51.100.0/24"}), "httpRequest.clientIPInCIDRs")
	  assert(request.httpRequest:remoteIPInCIDR("10.0.0.0/8"), "httpRequest.remoteIPInCIDR")
	  return true
	end
	`
	authorizer, err := NewLuaAuthorizerWithOptions(luaCode, Options{
		TrustForwardedHeaders: true,
		TrustedProxyCIDRs:     []string{"10.0.0.0/8"},
	})
	assert.Nil(t, err)

	request := authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket: ptrutils.ToPtr("my-bucket"),
		Key:    ptrutils.ToPtr("public/file.txt"),
		HttpRequest: authorization.HTTPRequest{
			Method:        "GET",
			Path:          "/my-bucket/public/file.txt",
			Host:          "assets.example.com",
			Proto:         "HTTP/1.1",
			ContentLength: ptrutils.ToPtr(128),
			RemoteIP:      ptrutils.ToPtr("10.1.2.3"),
			ClientIP:      ptrutils.ToPtr("198.51.100.7"),
			Scheme:        "https",
			QueryParams: map[string][]string{
				"uploadId": []string{"upload-123", "upload-456"},
			},
			Headers: map[string][]string{
				"X-Test":           []string{"a", "b"},
				"CF-Connecting-IP": []string{"198.51.100.7"},
			},
		},
	}

	authorized, err := authorizer.AuthorizeRequest(context.Background(), &request)
	assert.True(t, authorized)
	assert.Nil(t, err)
}
