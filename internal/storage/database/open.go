package database

import (
	"database/sql"
	"sync/atomic"

	"github.com/XSAM/otelsql"
	"go.opentelemetry.io/otel/attribute"
)

var queryTracingEnabled atomic.Bool

// SetQueryTracingEnabled configures whether database handles opened afterwards
// are instrumented with OpenTelemetry. It should be called during application
// startup, before any database is opened. Without an OpenTelemetry pipeline
// the instrumentation only costs time and allocations on every query, so it is
// disabled by default.
func SetQueryTracingEnabled(enabled bool) {
	queryTracingEnabled.Store(enabled)
}

// OpenSQL opens a database handle for the given driver, instrumented with
// OpenTelemetry if query tracing is enabled.
func OpenSQL(driverName string, dataSourceName string, attributes ...attribute.KeyValue) (*sql.DB, error) {
	if queryTracingEnabled.Load() {
		return otelsql.Open(driverName, dataSourceName, otelsql.WithAttributes(attributes...))
	}
	return sql.Open(driverName, dataSourceName)
}
