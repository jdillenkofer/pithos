package auditlog

import "context"

type AuthorizationDenialRecorder interface {
	RecordAuthorizationDenied(context.Context, Operation, ResourceDetails, *ObjectLockDetails)
}
