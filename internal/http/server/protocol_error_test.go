package server

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http/httptest"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestHandleErrorRecognizesWrappedAndJoinedErrors(t *testing.T) {
	testutils.SkipIfIntegration(t)
	wrappers := []struct {
		name string
		wrap func(error) error
	}{
		{"direct", func(err error) error { return err }},
		{"wrapped", func(err error) error { return fmt.Errorf("upload failed: %w", err) }},
		{"joined with nil", func(err error) error { return errors.Join(err, nil) }},
		{"joined with cleanup error", func(err error) error { return errors.Join(err, errors.New("private cleanup detail")) }},
	}
	for i, mapping := range s3ErrorMappings {
		for _, wrapper := range wrappers {
			t.Run(fmt.Sprintf("%d/%s/%s", i, mapping.err, wrapper.name), func(t *testing.T) {
				response := httptest.NewRecorder()
				request := httptest.NewRequest("PUT", "/bucket/key", nil)
				handleError(wrapper.wrap(mapping.err), response, request)
				require.Equal(t, mapping.status, response.Code)
				var body ErrorResponse
				require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &body))
				code := mapping.code
				if code == "" {
					code = mapping.err.Error()
				}
				require.Equal(t, code, body.Code)
				require.Equal(t, request.URL.Path, body.Resource)
				require.NotContains(t, response.Body.String(), "private cleanup detail")
				require.NotContains(t, response.Body.String(), "upload failed")
			})
		}
	}
}

func TestHandleErrorKeepsUnknownErrorsInternal(t *testing.T) {
	testutils.SkipIfIntegration(t)
	response := httptest.NewRecorder()
	request := httptest.NewRequest("PUT", "/bucket/key", nil)
	handleError(errors.New("private internal detail"), response, request)
	require.Equal(t, 500, response.Code)
	var body ErrorResponse
	require.NoError(t, xml.Unmarshal(response.Body.Bytes(), &body))
	require.Equal(t, "InternalError", body.Code)
	require.Equal(t, "InternalError", body.Message)
	require.NotContains(t, response.Body.String(), "private internal detail")
}
