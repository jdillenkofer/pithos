package admin

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/jdillenkofer/pithos/internal/buildinfo"
	"github.com/jdillenkofer/pithos/internal/http/server"
	"github.com/prometheus/client_golang/prometheus"
)

type dashboardSample struct {
	at                               time.Time
	requests, errors, latency, count float64
}

// bucketUsage is sampled from the periodic bucket gauges. Mirrored databases
// report the same bucket, so the largest value per bucket is used.
type bucketUsage struct {
	Objects, Bytes float64
}

// metricsSnapshot is the subset of the in-process Prometheus registry the
// admin UI presents. Absent collectors leave their has* flag unset.
type metricsSnapshot struct {
	sample                                  dashboardSample
	buckets                                 map[string]bucketUsage
	logicalBytes, physicalBytes, dedupRatio float64
	hasParts                                bool
	outboxPending, replicationPending       float64
	hasOutbox, hasReplication               bool
}

func gatherMetrics() (metricsSnapshot, error) {
	snapshot := metricsSnapshot{sample: dashboardSample{at: time.Now()}, buckets: map[string]bucketUsage{}}
	families, err := prometheus.DefaultGatherer.Gather()
	for _, family := range families {
		name := family.GetName()
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			gauge := metric.GetGauge().GetValue()
			switch name {
			case "pithos_http_requests_total":
				value := metric.GetCounter().GetValue()
				snapshot.sample.requests += value
				if status := labels["status"]; strings.HasPrefix(status, "4") || strings.HasPrefix(status, "5") {
					snapshot.sample.errors += value
				}
			case "pithos_http_request_duration_seconds":
				snapshot.sample.latency += metric.GetHistogram().GetSampleSum()
				snapshot.sample.count += float64(metric.GetHistogram().GetSampleCount())
			case "pithos_storage_bucket_bytes":
				usage := snapshot.buckets[labels["bucket"]]
				usage.Bytes = max(usage.Bytes, gauge)
				snapshot.buckets[labels["bucket"]] = usage
			case "pithos_storage_bucket_objects":
				usage := snapshot.buckets[labels["bucket"]]
				usage.Objects = max(usage.Objects, gauge)
				snapshot.buckets[labels["bucket"]] = usage
			case "pithos_storage_part_bytes_logical":
				snapshot.logicalBytes, snapshot.hasParts = gauge, true
			case "pithos_storage_part_bytes_physical":
				snapshot.physicalBytes = gauge
			case "pithos_storage_dedup_ratio":
				snapshot.dedupRatio = gauge
			case "pithos_outbox_pending_entries":
				snapshot.outboxPending += gauge
				snapshot.hasOutbox = true
			case "pithos_replication_pending_operations":
				snapshot.replicationPending += gauge
				snapshot.hasReplication = true
			}
		}
	}
	return snapshot, err
}

type keyValue struct{ Label, Value string }
type bucketBar struct {
	Name, Objects, Size string
	Bytes               float64
}
type dashboardView struct {
	DatabaseHealthy                               bool
	Version, Uptime                               string
	Buckets                                       string
	Sampled                                       bool
	Objects, TotalSize, AverageSize               string
	Storage                                       []keyValue
	RequestRate, ErrorRate, Latency, MetricsError string
	TopBuckets                                    []bucketBar
	MaxBytes                                      float64
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

func formatCount(n float64) string {
	digits := fmt.Sprintf("%.0f", n)
	var out strings.Builder
	for i, digit := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			out.WriteByte(',')
		}
		out.WriteRune(digit)
	}
	return out.String()
}

func formatUptime(d time.Duration) string {
	days, hours, minutes := int(d.Hours())/24, int(d.Hours())%24, int(d.Minutes())%60
	switch {
	case days > 0:
		return fmt.Sprintf("%dd %dh", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh %dm", hours, minutes)
	}
	return fmt.Sprintf("%dm", minutes)
}

func (a *App) dashboard(w http.ResponseWriter, r *http.Request) {
	view := dashboardView{
		Version:     buildinfo.Current().Version,
		Uptime:      formatUptime(time.Since(a.started)),
		Buckets:     "—",
		RequestRate: "—", ErrorRate: "—", Latency: "—",
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	view.DatabaseHealthy = server.CheckDatabaseHealth(ctx, a.options.Databases) == nil
	if buckets, err := a.options.Storage.ListBuckets(ctx); err == nil {
		view.Buckets = formatCount(float64(len(buckets)))
	}
	snapshot, err := gatherMetrics()
	if err != nil {
		view.MetricsError = "Some metrics could not be collected"
	}

	var objects, bytes float64
	for name, usage := range snapshot.buckets {
		objects += usage.Objects
		bytes += usage.Bytes
		view.TopBuckets = append(view.TopBuckets, bucketBar{Name: name, Objects: formatCount(usage.Objects), Size: humanBytes(usage.Bytes), Bytes: usage.Bytes})
		view.MaxBytes = max(view.MaxBytes, usage.Bytes)
	}
	view.Sampled = len(snapshot.buckets) > 0
	view.Objects, view.TotalSize, view.AverageSize = formatCount(objects), humanBytes(bytes), "—"
	if objects > 0 {
		view.AverageSize = humanBytes(bytes / objects)
	}
	sort.Slice(view.TopBuckets, func(i, j int) bool {
		if view.TopBuckets[i].Bytes != view.TopBuckets[j].Bytes {
			return view.TopBuckets[i].Bytes > view.TopBuckets[j].Bytes
		}
		return view.TopBuckets[i].Name < view.TopBuckets[j].Name
	})
	if len(view.TopBuckets) > 10 {
		view.TopBuckets = view.TopBuckets[:10]
	}
	if snapshot.hasParts {
		view.Storage = append(view.Storage, keyValue{"Stored after deduplication", humanBytes(snapshot.physicalBytes)})
		view.Storage = append(view.Storage, keyValue{"Deduplication ratio", fmt.Sprintf("%.2f×", snapshot.dedupRatio)})
	}
	if snapshot.hasOutbox {
		view.Storage = append(view.Storage, keyValue{"Outbox backlog", formatCount(snapshot.outboxPending)})
	}
	if snapshot.hasReplication {
		view.Storage = append(view.Storage, keyValue{"Replication backlog", formatCount(snapshot.replicationPending)})
	}

	sample := snapshot.sample
	a.dashboardMu.Lock()
	last := a.lastDashboard
	if !last.at.IsZero() {
		elapsed := sample.at.Sub(last.at).Seconds()
		if elapsed > 0 && sample.requests >= last.requests && sample.errors >= last.errors && sample.count >= last.count {
			view.RequestRate = fmt.Sprintf("%.1f/s", (sample.requests-last.requests)/elapsed)
			view.ErrorRate = fmt.Sprintf("%.1f/s", (sample.errors-last.errors)/elapsed)
			if n := sample.count - last.count; n > 0 {
				view.Latency = fmt.Sprintf("%.1f ms", 1000*(sample.latency-last.latency)/n)
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
