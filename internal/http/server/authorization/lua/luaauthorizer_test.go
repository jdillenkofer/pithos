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
		authorization.OperationAbortMultipartUpload,
		authorization.OperationDeleteObject,
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
