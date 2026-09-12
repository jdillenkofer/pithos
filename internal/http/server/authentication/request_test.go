package authentication

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRequestAuthenticationFromContextDefaultsToAnonymous(t *testing.T) {
	auth := RequestAuthenticationFromContext(context.Background())

	assert.False(t, auth.Authenticated)
	assert.Nil(t, auth.Identity)
	assert.Equal(t, AuthTypeAnonymous, auth.Type)
}

func TestRequestAuthenticationContextRoundTrip(t *testing.T) {
	identity := &AuthenticatedIdentity{AccessKeyID: "key", PrincipalID: "principal"}
	want := RequestAuthentication{
		Authenticated: true,
		Identity:      identity,
		Type:          AuthTypeSigV4Header,
	}

	got := RequestAuthenticationFromContext(WithRequestAuthentication(context.Background(), want))

	assert.Equal(t, want, got)
}
