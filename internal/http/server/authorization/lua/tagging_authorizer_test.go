package lua

import (
	"context"
	"errors"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func taggingRequest(resolver func(ctx context.Context) (map[string]string, error), requestTags map[string]string) *authorization.Request {
	return &authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket:                    ptrutils.ToPtr("my-bucket"),
		Key:                       ptrutils.ToPtr("my-key"),
		ResolveExistingObjectTags: resolver,
		RequestObjectTags:         requestTags,
	}
}

func TestObjectTagEqualsMatchesExistingTag(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:objectTagEquals("team", "storage")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	resolver := func(ctx context.Context) (map[string]string, error) {
		return map[string]string{"team": "storage"}, nil
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), taggingRequest(resolver, nil))
	assert.Nil(t, err)
	assert.True(t, authorized)
}

func TestObjectTagEqualsRejectsDifferentValue(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:objectTagEquals("team", "storage")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	resolver := func(ctx context.Context) (map[string]string, error) {
		return map[string]string{"team": "other"}, nil
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), taggingRequest(resolver, nil))
	assert.Nil(t, err)
	assert.False(t, authorized)
}

func TestHasObjectTagAndObjectTag(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:hasObjectTag("env") and request:objectTag("env") == "prod" and not request:hasObjectTag("missing")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	resolver := func(ctx context.Context) (map[string]string, error) {
		return map[string]string{"env": "prod"}, nil
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), taggingRequest(resolver, nil))
	assert.Nil(t, err)
	assert.True(t, authorized)
}

func TestObjectTagsReturnsTable(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  local t = request:objectTags()
	  return t["team"] == "storage" and t["env"] == "prod"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	resolver := func(ctx context.Context) (map[string]string, error) {
		return map[string]string{"team": "storage", "env": "prod"}, nil
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), taggingRequest(resolver, nil))
	assert.Nil(t, err)
	assert.True(t, authorized)
}

func TestExistingObjectTagsResolvedAtMostOncePerCall(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:objectTagEquals("team", "storage") and request:hasObjectTag("team") and request:objectTag("team") == "storage"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	callCount := 0
	resolver := func(ctx context.Context) (map[string]string, error) {
		callCount++
		return map[string]string{"team": "storage"}, nil
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), taggingRequest(resolver, nil))
	assert.Nil(t, err)
	assert.True(t, authorized)
	assert.Equal(t, 1, callCount, "resolver should be invoked at most once per authorization call")
}

func TestExistingObjectTagsResolverErrorFailsClosed(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  -- even though the policy would "allow" on the negation, a resolver error must
	  -- abort evaluation and deny.
	  return not request:objectTagEquals("team", "storage")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	resolver := func(ctx context.Context) (map[string]string, error) {
		return nil, errors.New("storage unavailable")
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), taggingRequest(resolver, nil))
	assert.NotNil(t, err)
	assert.False(t, authorized)
}

func TestObjectTagPredicatesFalseWhenResolverNil(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return (not request:hasObjectTag("team")) and (request:objectTag("team") == nil) and (next(request:objectTags()) == nil)
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	authorized, err := authorizer.AuthorizeRequest(context.Background(), taggingRequest(nil, nil))
	assert.Nil(t, err)
	assert.True(t, authorized)
}

func TestRequestTagPredicates(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:requestTagEquals("team", "storage") and request:hasRequestTag("team") and request:requestTag("team") == "storage" and request:requestTags()["team"] == "storage"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	authorized, err := authorizer.AuthorizeRequest(context.Background(), taggingRequest(nil, map[string]string{"team": "storage"}))
	assert.Nil(t, err)
	assert.True(t, authorized)
}

func TestListObjectTagFilteringViaResource(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return true
	end
	function authorizeListObject(request, key)
	  return request:objectTagEquals("team", "storage")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	resolver := func(ctx context.Context) (map[string]string, error) {
		return map[string]string{"team": "storage"}, nil
	}
	allowed, err := authorizer.AuthorizeListObject(context.Background(), taggingRequest(resolver, nil), "my-key")
	assert.Nil(t, err)
	assert.True(t, allowed)

	denyResolver := func(ctx context.Context) (map[string]string, error) {
		return map[string]string{"team": "other"}, nil
	}
	allowed, err = authorizer.AuthorizeListObject(context.Background(), taggingRequest(denyResolver, nil), "my-key")
	assert.Nil(t, err)
	assert.False(t, allowed)
}

func TestSourceObjectTagPredicates(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return request:sourceObjectTagEquals("team", "storage") and request:hasSourceObjectTag("team") and request:sourceObjectTag("team") == "storage" and request:sourceObjectTags()["team"] == "storage"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := taggingRequest(nil, nil)
	request.Operation = authorization.OperationCopyObject
	request.SourceBucket = ptrutils.ToPtr("src-bucket")
	request.SourceKey = ptrutils.ToPtr("src-key")
	request.ResolveExistingSourceObjectTags = func(ctx context.Context) (map[string]string, error) {
		return map[string]string{"team": "storage"}, nil
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), request)
	assert.Nil(t, err)
	assert.True(t, authorized)
}

func TestSourceObjectTagPredicatesFalseWhenResolverNil(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return (not request:hasSourceObjectTag("team")) and (request:sourceObjectTag("team") == nil) and (next(request:sourceObjectTags()) == nil)
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	authorized, err := authorizer.AuthorizeRequest(context.Background(), taggingRequest(nil, nil))
	assert.Nil(t, err)
	assert.True(t, authorized)
}

func TestSourceObjectTagsResolverErrorFailsClosed(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  return not request:sourceObjectTagEquals("team", "storage")
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)

	request := taggingRequest(nil, nil)
	request.ResolveExistingSourceObjectTags = func(ctx context.Context) (map[string]string, error) {
		return nil, errors.New("storage unavailable")
	}
	authorized, err := authorizer.AuthorizeRequest(context.Background(), request)
	assert.NotNil(t, err)
	assert.False(t, authorized)
}
