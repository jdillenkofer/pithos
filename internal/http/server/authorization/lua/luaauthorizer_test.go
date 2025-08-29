package lua

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
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
			AccessKeyId: "AKIAIOSFODNN7EXAMPLE",
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(&request)
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
			AccessKeyId: "AKIAIOSFODNN7EXAMPLE",
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(&request)
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
			AccessKeyId: "AKIAIOSFODNN7EXAMPLE",
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(&allowedRequest)
	assert.True(t, authorized)
	assert.Nil(t, err)

	deniedRequest := authorization.Request{
		Operation: authorization.OperationCreateBucket,
		Authorization: authorization.Authorization{
			AccessKeyId: "AKIAIOSFODNN7EXAMPLE",
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err = authorizer.AuthorizeRequest(&deniedRequest)
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
			AccessKeyId: "AKIAIOSFODNN7EXAMPLE",
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err := authorizer.AuthorizeRequest(&allowedRequest)
	assert.True(t, authorized)
	assert.Nil(t, err)

	deniedRequest := authorization.Request{
		Operation: authorization.OperationCreateBucket,
		Authorization: authorization.Authorization{
			AccessKeyId: "AKIAIOSFODNN7EXAMPLE",
		},
		Bucket: nil,
		Key:    nil,
	}
	authorized, err = authorizer.AuthorizeRequest(&deniedRequest)
	assert.False(t, authorized)
	assert.Nil(t, err)
}
