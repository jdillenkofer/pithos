package policy

import (
	"context"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestNumericConditionsRejectNonFiniteRequestValues(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, operator := range []string{"NumericEquals", "NumericNotEquals", "NumericLessThan", "NumericLessThanEquals", "NumericGreaterThan", "NumericGreaterThanEquals"} {
		for _, effect := range []string{"Allow", "Deny"} {
			t.Run(operator+"/"+effect, func(t *testing.T) {
				statement := `{"Effect":"` + effect + `","Action":"s3:GetObject","Resource":"*","Condition":{"` + operator + `":{"s3:RequestObjectTag/score":"30"}}}`
				if effect == "Deny" {
					statement = `[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"},` + statement + `]`
				}
				s := compileTestPolicy(t, statement)
				for _, value := range []string{"NaN", "+Inf", "-Inf", "Infinity", "-Infinity"} {
					t.Run(value, func(t *testing.T) {
						r := request(authorization.OperationGetObject, "bucket", "key")
						r.RequestObjectTags = map[string]string{"score": value}
						d, err := s.AuthorizeRequest(context.Background(), r)
						require.ErrorContains(t, err, "non-finite numeric context value")
						require.NotEqual(t, authorization.Allow, d.Effect)
					})
				}
				for _, value := range []string{"-1.5", "0", "30", "1e100"} {
					t.Run(value, func(t *testing.T) {
						r := request(authorization.OperationGetObject, "bucket", "key")
						r.RequestObjectTags = map[string]string{"score": value}
						_, err := s.AuthorizeRequest(context.Background(), r)
						require.NoError(t, err)
					})
				}
			})
		}
	}
}
