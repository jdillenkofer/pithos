package lua

import (
	"context"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
)

func sandboxTestRequest() *authorization.Request {
	return &authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
	}
}

func TestUnsafeGlobalsAreNotAvailable(t *testing.T) {
	testutils.SkipIfIntegration(t)

	for _, name := range []string{"dofile", "loadfile", "load", "require", "io", "os", "package", "debug"} {
		t.Run(name, func(t *testing.T) {
			luaCode := `
			function authorizeRequest(request)
			  return ` + name + ` == nil
			end
			`
			authorizer, err := NewLuaAuthorizer(luaCode)
			assert.Nil(t, err)
			decision, err := authorizer.AuthorizeRequest(context.Background(), sandboxTestRequest())
			assert.Nil(t, err)
			assert.Equal(t, authorization.Allow, decision.Effect)
		})
	}
}

func TestSafeLibrariesRemainAvailable(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  local ok = pcall(function() error("boom") end)
	  return not ok
	    and string.upper("a") == "A"
	    and table.concat({ "a", "b" }) == "ab"
	    and math.max(1, 2) == 2
	    and tostring(1) == "1"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)
	decision, err := authorizer.AuthorizeRequest(context.Background(), sandboxTestRequest())
	assert.Nil(t, err)
	assert.Equal(t, authorization.Allow, decision.Effect)
}
