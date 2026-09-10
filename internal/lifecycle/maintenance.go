package lifecycle

import "context"

type maintenanceKey struct{}
type maintenanceMode struct{ dryRun bool }

// WithMaintenance disables lifecycle and replication background work. A dry run
// also disables outbox dispatch; reads fail if dispatch would be necessary.
func WithMaintenance(ctx context.Context, dryRun bool) context.Context {
	return context.WithValue(ctx, maintenanceKey{}, maintenanceMode{dryRun: dryRun})
}
func IsMaintenance(ctx context.Context) bool {
	_, ok := ctx.Value(maintenanceKey{}).(maintenanceMode)
	return ok
}
func IsDryRun(ctx context.Context) bool {
	mode, ok := ctx.Value(maintenanceKey{}).(maintenanceMode)
	return ok && mode.dryRun
}
