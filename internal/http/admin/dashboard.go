package admin

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/prometheus/client_golang/prometheus"
)

type dashboardSample struct {
	at                               time.Time
	requests, errors, latency, count float64
}
type metricRow struct{ Name, Labels, Value string }
type dashboardView struct {
	DatabaseHealth, StorageState, RequestRate, ErrorRate, Latency, MetricsError string
	Metrics                                                                     []metricRow
}

func (a *App) dashboard(w http.ResponseWriter, r *http.Request) {
	view := dashboardView{DatabaseHealth: "Healthy", StorageState: "Not started", RequestRate: "Collecting baseline", ErrorRate: "Collecting baseline", Latency: "Collecting baseline"}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := server.CheckDatabaseHealth(ctx, a.options.Databases); err != nil {
		view.DatabaseHealth = "Unhealthy"
	}
	if a.options.StorageStarted {
		view.StorageState = "Started"
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		view.MetricsError = "Some metrics could not be collected"
	}
	sample := dashboardSample{at: time.Now()}
	for _, family := range families {
		name := family.GetName()
		for _, metric := range family.Metric {
			switch name {
			case "pithos_http_requests_total":
				value := metric.GetCounter().GetValue()
				sample.requests += value
				for _, label := range metric.Label {
					if label.GetName() == "status" && (strings.HasPrefix(label.GetValue(), "4") || strings.HasPrefix(label.GetValue(), "5")) {
						sample.errors += value
					}
				}
			case "pithos_http_request_duration_seconds":
				sample.latency += metric.GetHistogram().GetSampleSum()
				sample.count += float64(metric.GetHistogram().GetSampleCount())
			}
			if strings.HasPrefix(name, "pithos_storage_") || name == "pithos_outbox_pending_entries" || name == "pithos_replication_pending_operations" {
				if metric.Gauge == nil {
					continue
				}
				var labels []string
				for _, label := range metric.Label {
					labels = append(labels, label.GetName()+"="+label.GetValue())
				}
				view.Metrics = append(view.Metrics, metricRow{name, strings.Join(labels, ", "), fmt.Sprintf("%.2f", metric.Gauge.GetValue())})
			}
		}
	}
	a.dashboardMu.Lock()
	last := a.lastDashboard
	if !last.at.IsZero() {
		elapsed := sample.at.Sub(last.at).Seconds()
		if elapsed > 0 && sample.requests >= last.requests && sample.errors >= last.errors && sample.count >= last.count {
			view.RequestRate = fmt.Sprintf("%.2f requests/s", (sample.requests-last.requests)/elapsed)
			view.ErrorRate = fmt.Sprintf("%.2f errors/s", (sample.errors-last.errors)/elapsed)
			if n := sample.count - last.count; n > 0 {
				view.Latency = fmt.Sprintf("%.2f ms mean", 1000*(sample.latency-last.latency)/n)
			} else {
				view.Latency = "No completed requests in interval"
			}
		}
	}
	// A baseline shared by concurrent viewers advances at most once per second.
	if last.at.IsZero() || sample.at.Sub(last.at) >= time.Second {
		a.lastDashboard = sample
	}
	a.dashboardMu.Unlock()
	a.render(w, r, "Dashboard", "dashboard", view)
}
