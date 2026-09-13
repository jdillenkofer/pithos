package authentication

import "context"

type AuthType string

const (
	AuthTypeAnonymous    AuthType = "anonymous"
	AuthTypeSigV4Header  AuthType = "sigv4-header"
	AuthTypeSigV4Presign AuthType = "sigv4-presign"
)

type AuthenticatedIdentity struct {
	AccessKeyID string
	PrincipalID string
}

// RequestAuthentication is the authentication result attached to a request.
// Identity is non-nil exactly when Authenticated is true.
type RequestAuthentication struct {
	Authenticated bool
	Identity      *AuthenticatedIdentity
	Type          AuthType
}

type requestAuthenticationContextKey struct{}

func WithRequestAuthentication(ctx context.Context, auth RequestAuthentication) context.Context {
	return context.WithValue(ctx, requestAuthenticationContextKey{}, auth)
}

// RequestAuthenticationFromContext returns the request authentication state.
// A context without authentication middleware is treated as anonymous.
func RequestAuthenticationFromContext(ctx context.Context) RequestAuthentication {
	auth, ok := ctx.Value(requestAuthenticationContextKey{}).(RequestAuthentication)
	if !ok {
		return RequestAuthentication{Type: AuthTypeAnonymous}
	}
	return auth
}
