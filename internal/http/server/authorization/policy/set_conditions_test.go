package policy

import (
	"context"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestSetConditionMissingAndEmptyValues(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, prefix := range []string{"", "ForAnyValue:", "ForAllValues:"} {
		for _, op := range []string{"StringEquals", "StringNotEquals"} {
			for _, suffix := range []string{"", "IfExists"} {
				operator := prefix + op + suffix
				t.Run(operator, func(t *testing.T) {
					c := condition{operator: operator, values: []string{"team"}}
					wantMissing := prefix != "ForAnyValue:" && (prefix == "ForAllValues:" || suffix != "" || op == "StringNotEquals")
					for _, values := range [][]string{nil, {}} {
						got, err := compareCondition(c, values, false)
						require.NoError(t, err)
						require.Equal(t, wantMissing, got)
						got, err = compareCondition(c, values, true)
						require.NoError(t, err)
						require.Equal(t, prefix == "ForAllValues:", got)
					}
					for _, value := range []string{"team", "other"} {
						got, err := compareCondition(c, []string{value}, true)
						require.NoError(t, err)
						require.Equal(t, (value == "team") == (op == "StringEquals"), got)
					}
				})
			}
		}
	}
}

func TestForAnyValueDoesNotAuthorizeMissingTags(t *testing.T) {
	testutils.SkipIfIntegration(t)
	for _, op := range []string{"StringEquals", "StringNotEquals", "StringEqualsIfExists", "StringNotEqualsIfExists"} {
		for _, effect := range []string{"Allow", "Deny"} {
			t.Run(effect+"/"+op, func(t *testing.T) {
				s := compileTestPolicy(t, `{"Effect":"`+effect+`","Action":"s3:GetObject","Resource":"*","Condition":{"ForAnyValue:`+op+`":{"s3:RequestObjectTagKeys":"team"}}}`)
				for _, tags := range []map[string]string{nil, {}} {
					r := request(authorization.OperationGetObject, "bucket", "key")
					r.RequestObjectTags = tags
					d, err := s.AuthorizeRequest(context.Background(), r)
					require.NoError(t, err)
					require.Equal(t, authorization.ImplicitDeny, d.Effect)
				}
			})
		}
	}
}
