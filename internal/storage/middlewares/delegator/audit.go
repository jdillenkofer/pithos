package delegator

import (
	"context"
	"github.com/jdillenkofer/pithos/internal/auditlog"
)

func (d *DelegatingStorage) RecordAdminEvent(ctx context.Context, operation string) {
	if recorder, ok := d.Next.(auditlog.AdminEventRecorder); ok {
		recorder.RecordAdminEvent(ctx, operation)
	}
}

func (d *DelegatingStorage) RecordAuthorizationDenied(ctx context.Context, operation auditlog.Operation, resource auditlog.ResourceDetails, lock *auditlog.ObjectLockDetails) {
	if recorder, ok := d.Next.(auditlog.AuthorizationDenialRecorder); ok {
		recorder.RecordAuthorizationDenied(ctx, operation, resource, lock)
	}
}
