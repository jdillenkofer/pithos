package auditlog

import "context"

// AdminEventRecorder appends an administrative action to the normal audit chain.
// Operation names must be application constants, not arbitrary form values.
type AdminEventRecorder interface {
	RecordAdminEvent(context.Context, string)
}

type adminActorKey struct{}
type adminStatusKey struct{}

// WithAdminStatus preserves the outcome of non-storage admin actions.
func WithAdminStatus(ctx context.Context, status int32) context.Context {
	return context.WithValue(ctx, adminStatusKey{}, status)
}

func AdminStatusFromContext(ctx context.Context) int32 {
	if status, ok := ctx.Value(adminStatusKey{}).(int32); ok {
		return status
	}
	return 200
}

// WithAdminActor attaches an authenticated admin session identity without making
// the audit package depend on HTTP session or authentication implementations.
func WithAdminActor(ctx context.Context, actor ActorDetails) context.Context {
	return context.WithValue(ctx, adminActorKey{}, actor)
}

func AdminActorFromContext(ctx context.Context) (ActorDetails, bool) {
	actor, ok := ctx.Value(adminActorKey{}).(ActorDetails)
	return actor, ok
}

type AuthorizationDenialRecorder interface {
	RecordAuthorizationDenied(context.Context, Operation, ResourceDetails, *ObjectLockDetails)
}
