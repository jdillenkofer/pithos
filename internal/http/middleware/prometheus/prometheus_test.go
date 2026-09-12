package prometheus

import (
	"net/http"
	"net/http/httptest"
	"testing"

	pithostesting "github.com/jdillenkofer/pithos/internal/testing"
	client "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestMiddlewareCapturesOperationAndStatus(t *testing.T) {
	pithostesting.WithTestRegisterer(t, func(_ client.Registerer) {
		handler := New(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNotFound) }))
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/bucket/key", nil))
		metric := &dto.Metric{}
		require.NoError(t, requests.WithLabelValues("GetObject", "404").Write(metric))
		require.Equal(t, float64(1), metric.GetCounter().GetValue())
	})
}
