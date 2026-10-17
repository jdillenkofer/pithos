package admin

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/prometheus/client_golang/prometheus"
)

type dashboardSample struct {
	at                               time.Time
	requests, errors, latency, count float64
}
type metricRow struct {
	Name, Label, Value string
	Labels             []string
}
type usageTile struct{ Label, Value string }
type bucketUsage struct {
	Database, Bucket, Objects, Size string
	objects, size                   float64
}
type dashboardView struct {
	DatabaseHealth, StorageState, RequestRate, ErrorRate, Latency, MetricsError string
	DatabaseHealthy, StorageStarted                                             bool
	Usage                                                                       []usageTile
	Buckets                                                                     []bucketUsage
	Metrics                                                                     []metricRow
}

// metricLabels gives the storage and queue gauges readable names.
var metricLabels = map[string]string{
	"pithos_storage_parts_logical_total":          "Part references",
	"pithos_storage_parts_physical_total":         "Stored parts (deduplicated)",
	"pithos_storage_parts_physical_by_store":      "Stored parts by part store",
	"pithos_storage_part_bytes_physical_by_store": "Stored bytes by part store",
	"pithos_outbox_pending_entries":               "Outbox pending entries",
	"pithos_replication_pending_operations":       "Replication backlog",
	"pithos_storage_part_bytes_logical":           "Logical data",
	"pithos_storage_part_bytes_physical":          "Stored data",
	"pithos_storage_dedup_ratio":                  "Deduplication ratio",
}

// humanBytes formats a byte count with binary units.
func humanBytes(n float64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%.0f B", n)
	}
	exponent, value := 0, n/unit
	for value >= unit && exponent < 5 {
		value /= unit
		exponent++
	}
	return fmt.Sprintf("%.1f %ciB", value, "KMGTPE"[exponent])
}

func formatMetricValue(name string, value float64) string {
	switch {
	case strings.Contains(name, "bytes"):
		return humanBytes(value)
	case strings.HasSuffix(name, "_ratio"):
		return fmt.Sprintf("%.2f×", value)
	case value == math.Trunc(value):
		return fmt.Sprintf("%.0f", value)
	}
	return fmt.Sprintf("%.2f", value)
}

func (a *App) dashboard(w http.ResponseWriter, r *http.Request) {
	view := dashboardView{DatabaseHealth: "Healthy", StorageState: "Not started", RequestRate: "Measuring…", ErrorRate: "Measuring…", Latency: "Measuring…"}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := server.CheckDatabaseHealth(ctx, a.options.Databases); err != nil {
		view.DatabaseHealth = "Unhealthy"
	} else {
		view.DatabaseHealthy = true
	}
	if a.options.StorageStarted {
		view.StorageState = "Started"
		view.StorageStarted = true
	}
	families, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		view.MetricsError = "Some metrics could not be collected"
	}
	sample := dashboardSample{at: time.Now()}
	buckets := map[string]*bucketUsage{}
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
				value := metric.Gauge.GetValue()
				labelValues := map[string]string{}
				var labels []string
				for _, label := range metric.Label {
					labelValues[label.GetName()] = label.GetValue()
					labels = append(labels, label.GetName()+"="+label.GetValue())
				}
				switch name {
				case "pithos_storage_bucket_bytes", "pithos_storage_bucket_objects":
					key := labelValues["database"] + "\x00" + labelValues["bucket"]
					usage, ok := buckets[key]
					if !ok {
						usage = &bucketUsage{Database: labelValues["database"], Bucket: labelValues["bucket"]}
						buckets[key] = usage
					}
					if name == "pithos_storage_bucket_bytes" {
						usage.size = value
					} else {
						usage.objects = value
					}
				case "pithos_storage_part_bytes_logical", "pithos_storage_part_bytes_physical", "pithos_storage_dedup_ratio":
					view.Usage = append(view.Usage, usageTile{metricLabels[name], formatMetricValue(name, value)})
				default:
					label := metricLabels[name]
					if label == "" {
						label = name
					}
					view.Metrics = append(view.Metrics, metricRow{Name: name, Label: label, Value: formatMetricValue(name, value), Labels: labels})
				}
			}
		}
	}
	for _, usage := range buckets {
		usage.Objects, usage.Size = formatMetricValue("objects", usage.objects), humanBytes(usage.size)
		view.Buckets = append(view.Buckets, *usage)
	}
	sort.Slice(view.Buckets, func(i, j int) bool {
		if view.Buckets[i].Database != view.Buckets[j].Database {
			return view.Buckets[i].Database < view.Buckets[j].Database
		}
		return view.Buckets[i].Bucket < view.Buckets[j].Bucket
	})
	a.dashboardMu.Lock()
	last := a.lastDashboard
	if !last.at.IsZero() {
		elapsed := sample.at.Sub(last.at).Seconds()
		if elapsed > 0 && sample.requests >= last.requests && sample.errors >= last.errors && sample.count >= last.count {
			view.RequestRate = fmt.Sprintf("%.2f req/s", (sample.requests-last.requests)/elapsed)
			view.ErrorRate = fmt.Sprintf("%.2f errors/s", (sample.errors-last.errors)/elapsed)
			if n := sample.count - last.count; n > 0 {
				view.Latency = fmt.Sprintf("%.1f ms", 1000*(sample.latency-last.latency)/n)
			} else {
				view.Latency = "No requests"
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
