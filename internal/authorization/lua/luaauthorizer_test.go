package lua

import (
	"testing"

	"github.com/jdillenkofer/pithos/internal/authorization"
	"github.com/stretchr/testify/assert"
)

func TestAuthorizationAlwaysDenied(t *testing.T) {
	luaCode := `
	function authorizeRequest(request)
	  return false
	end
	`
	authorizer := NewLuaAuthorizer(luaCode)
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
	luaCode := `
	function authorizeRequest(request)
	  return true
	end
	`
	authorizer := NewLuaAuthorizer(luaCode)
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
	luaCode := `
	function authorizeRequest(request)
	  return request.operation == "PutObject"
	end
	`
	authorizer := NewLuaAuthorizer(luaCode)
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
	luaCode := `
	function authorizeRequest(request)
	  authorization = request.authorization
	  return authorization.accessKeyId == "AKIAIOSFODNN7EXAMPLE" and request.operation == "PutObject"
	end
	`
	authorizer := NewLuaAuthorizer(luaCode)
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
