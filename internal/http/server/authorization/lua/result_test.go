package lua

import (
	"context"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestNonBooleanAuthorizationResultsFailClosed(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, result := range []string{`"false"`, `"true"`, "0", "1", "nil", "{}", "function() end", ""} {
		t.Run(result, func(t *testing.T) {
			_, err := NewLuaAuthorizer(`function authorizeRequest(r) return ` + result + ` end`)
			require.ErrorIs(t, err, errAuthorizationResultNotBoolean)
			// A successful startup check must not mask bad return types on
			// another request path.
			authorizer, err := NewLuaAuthorizer(`function authorizeRequest(r)
				if r.operation == "PutObject" then return true end
				return ` + result + `
			end`)
			require.NoError(t, err)
			d, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{Operation: authorization.OperationGetObject})
			require.ErrorIs(t, err, errAuthorizationResultNotBoolean)
			require.Equal(t, authorization.ImplicitDeny, d.Effect)
		})
	}
}

func TestTopLevelChunkResultsCannotOverrideAuthorization(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, tc := range []struct {
		code string
		want authorization.Effect
	}{
		{`function authorizeRequest(r) return false end; return true`, authorization.ExplicitDeny},
		{`function authorizeRequest(r) return true end; return false`, authorization.Allow},
	} {
		authorizer, err := NewLuaAuthorizer(tc.code)
		require.NoError(t, err)
		d, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{})
		require.NoError(t, err)
		require.Equal(t, tc.want, d.Effect)
	}
}
