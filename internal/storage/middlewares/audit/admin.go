package audit

import (
	"context"
	"errors"

	"github.com/jdillenkofer/pithos/internal/auditlog"
)

var _ auditlog.AdminEventRecorder = (*AuditLogMiddleware)(nil)

// RecordAdminEvent uses the same lock, signing, sink and Merkle grounding path
// as storage operations. No request body, query, credential secret or config is
// included in an administrative event.
func (m *AuditLogMiddleware) RecordAdminEvent(ctx context.Context, operation string) {
	status := auditlog.AdminStatusFromContext(ctx)
	resource := auditResource{denied: status == 401 || status == 403 || status == 429}
	var err error
	if status >= 400 {
		err = errors.New("administrative action failed")
	}
	m.log(ctx, auditlog.Operation(operation), auditlog.PhaseComplete, resource, err, status, 0)
}
