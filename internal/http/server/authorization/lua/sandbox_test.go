package lua

import (
	"context"
	"testing"
	"time"

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

// spinPolicy loops forever only for the "spin" bucket, so the startup dry run
// (which has no bucket) still succeeds.
func spinPolicy(loop string) string {
	return `
	function authorizeRequest(request)
	  if request.bucket == "spin" then
	    ` + loop + `
	  end
	  return true
	end
	`
}

func spinRequest() *authorization.Request {
	request := sandboxTestRequest()
	request.Bucket = ptrutils.ToPtr("spin")
	return request
}

func TestRunawayScriptIsAbortedAtTimeLimit(t *testing.T) {
	testutils.SkipIfIntegration(t)

	loops := map[string]string{
		"plain loop":      `while true do end`,
		"pcall swallowed": `while true do pcall(function() while true do end end) end`,
		"error handler":   `while true do xpcall(function() while true do end end, function() while true do end end) end`,
	}
	for name, loop := range loops {
		t.Run(name, func(t *testing.T) {
			authorizer, err := NewLuaAuthorizer(spinPolicy(loop))
			assert.Nil(t, err)
			started := time.Now()
			decision, err := authorizer.AuthorizeRequest(context.Background(), spinRequest())
			assert.Error(t, err)
			assert.NotEqual(t, authorization.Allow, decision.Effect)
			assert.Less(t, time.Since(started), 4*maxExecutionTime)
		})
	}
}

func TestRunawayTopLevelScriptFailsAtStartup(t *testing.T) {
	testutils.SkipIfIntegration(t)

	_, err := NewLuaAuthorizer(`
	while true do end
	function authorizeRequest(request)
	  return true
	end
	`)
	assert.Error(t, err)
}

func TestCanceledContextAbortsScript(t *testing.T) {
	testutils.SkipIfIntegration(t)

	authorizer, err := NewLuaAuthorizer(spinPolicy(`while true do end`))
	assert.Nil(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	decision, err := authorizer.AuthorizeRequest(ctx, spinRequest())
	assert.Error(t, err)
	assert.NotEqual(t, authorization.Allow, decision.Effect)
	assert.Less(t, time.Since(started), maxExecutionTime)
}

func TestXPCallKeepsStandardSemantics(t *testing.T) {
	testutils.SkipIfIntegration(t)

	luaCode := `
	function authorizeRequest(request)
	  local ok, a, b = xpcall(function(x, y) return x + y, x * y end, error, 2, 3)
	  local failed, handled = xpcall(function() error("inner", 0) end, function(e) return "handled:" .. e end)
	  local nested, msg = xpcall(function() error("inner", 0) end, function() error("handler", 0) end)
	  return ok and a == 5 and b == 6
	    and failed == false and handled == "handled:inner"
	    and nested == false and msg == "handler"
	end
	`
	authorizer, err := NewLuaAuthorizer(luaCode)
	assert.Nil(t, err)
	decision, err := authorizer.AuthorizeRequest(context.Background(), sandboxTestRequest())
	assert.Nil(t, err)
	assert.Equal(t, authorization.Allow, decision.Effect)
}
