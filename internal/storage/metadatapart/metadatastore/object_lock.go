package metadatastore

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

type ObjectLockOptions struct {
	VersionID *string
	// BypassGovernanceRetention is already authorized by the caller.
	BypassGovernanceRetention bool
}

type CreateBucketOptions struct {
	ObjectLockEnabled bool
}

type ObjectLockObservation struct {
	Key           string
	VersionID     *string
	Effective     ObjectLock
	Configuration *ObjectLockConfiguration
	BypassUsed    bool
	Err           error
}

type objectLockObserverKey struct{}

func WithObjectLockObserver(ctx context.Context, observer func(ObjectLockObservation)) context.Context {
	return context.WithValue(ctx, objectLockObserverKey{}, observer)
}

func ObserveObjectLock(ctx context.Context, observation ObjectLockObservation) {
	if observer, ok := ctx.Value(objectLockObserverKey{}).(func(ObjectLockObservation)); ok && observer != nil {
		observer(observation)
	}
}

type ObjectLockStore interface {
	// LockBuckets acquires bucket locks before any object or part locks.
	LockBuckets(context.Context, *sql.Tx, ...BucketName) error
	GetObjectLockConfiguration(context.Context, *sql.Tx, BucketName) (*ObjectLockConfiguration, error)
	PutObjectLockConfiguration(context.Context, *sql.Tx, BucketName, *ObjectLockConfiguration) error
	GetObjectRetention(context.Context, *sql.Tx, BucketName, ObjectKey, *ObjectLockOptions) (*ObjectRetention, error)
	PutObjectRetention(context.Context, *sql.Tx, BucketName, ObjectKey, *ObjectRetention, *ObjectLockOptions) error
	GetObjectLegalHold(context.Context, *sql.Tx, BucketName, ObjectKey, *ObjectLockOptions) (*LegalHoldStatus, error)
	PutObjectLegalHold(context.Context, *sql.Tx, BucketName, ObjectKey, LegalHoldStatus, *ObjectLockOptions) error
}

type RetentionMode string

const (
	RetentionModeGovernance RetentionMode = "GOVERNANCE"
	RetentionModeCompliance RetentionMode = "COMPLIANCE"
)

type LegalHoldStatus string

const (
	LegalHoldOn  LegalHoldStatus = "ON"
	LegalHoldOff LegalHoldStatus = "OFF"
)

var (
	ErrInvalidObjectLockConfiguration  = errors.New("InvalidObjectLockConfiguration")
	ErrObjectLockConfigurationNotFound = errors.New("ObjectLockConfigurationNotFoundError")
	ErrObjectLockAccessDenied          = errors.New("AccessDenied")
	ErrObjectLockMethodNotAllowed      = errors.New("MethodNotAllowed")
)

// ObjectRetention belongs to one version, identified internally by object ID.
// A nil retention means no retention; a partially populated retention is invalid.
type ObjectRetention struct {
	Mode            RetentionMode
	RetainUntilDate time.Time
}

type DefaultRetention struct {
	Mode  RetentionMode
	Days  *int32
	Years *int32
}

// ObjectLockConfiguration can only enable Object Lock. Removing the default
// does not disable Object Lock or change protection on existing versions.
type ObjectLockConfiguration struct {
	ObjectLockEnabled string
	DefaultRetention  *DefaultRetention
}

// ObjectLock contains the independent protections on an object version.
type ObjectLock struct {
	Retention *ObjectRetention
	LegalHold *LegalHoldStatus
}

func NormalizeObjectRetention(retention *ObjectRetention) *ObjectRetention {
	if retention == nil {
		return nil
	}
	normalized := *retention
	// PostgreSQL TIMESTAMPTZ stores microseconds. Use the same precision for
	// every backend so replicated and subsequently read values remain equal.
	normalized.RetainUntilDate = normalized.RetainUntilDate.UTC().Truncate(time.Microsecond)
	return &normalized
}

func (m RetentionMode) Valid() bool {
	return m == RetentionModeGovernance || m == RetentionModeCompliance
}

func (s LegalHoldStatus) Valid() bool {
	return s == LegalHoldOn || s == LegalHoldOff
}

// Validate checks stored retention as well as client input. Expired retention
// remains valid metadata and must be preserved by replication.
func (r *ObjectRetention) Validate() error {
	if r == nil {
		return nil
	}
	if !r.Mode.Valid() || r.RetainUntilDate.IsZero() || r.RetainUntilDate.Year() < 1 || r.RetainUntilDate.Year() > 9999 {
		return ErrInvalidObjectLockConfiguration
	}
	return nil
}

func (d *DefaultRetention) Validate() error {
	if d == nil {
		return nil
	}
	if !d.Mode.Valid() || (d.Days == nil) == (d.Years == nil) {
		return ErrInvalidObjectLockConfiguration
	}
	if d.Days != nil && *d.Days <= 0 || d.Years != nil && *d.Years <= 0 {
		return ErrInvalidObjectLockConfiguration
	}
	return nil
}

func (c *ObjectLockConfiguration) Validate() error {
	if c == nil || c.ObjectLockEnabled != "Enabled" {
		return ErrInvalidObjectLockConfiguration
	}
	return c.DefaultRetention.Validate()
}

func (l ObjectLock) Validate() error {
	if err := l.Retention.Validate(); err != nil {
		return err
	}
	if l.LegalHold != nil && !l.LegalHold.Valid() {
		return ErrInvalidObjectLockConfiguration
	}
	return nil
}

// EffectiveObjectLock calculates defaults at completion, not multipart
// initiation. Explicit retention replaces the default. The returned values are
// detached from input pointers and use UTC, suitable for durable replication.
func EffectiveObjectLock(config *ObjectLockConfiguration, requested ObjectLock, completedAt time.Time) (ObjectLock, error) {
	if err := requested.Validate(); err != nil {
		return ObjectLock{}, err
	}
	if config == nil {
		if requested.Retention != nil || requested.LegalHold != nil {
			return ObjectLock{}, ErrObjectLockConfigurationNotFound
		}
		return ObjectLock{}, nil
	}
	if err := config.Validate(); err != nil {
		return ObjectLock{}, err
	}
	result := ObjectLock{}
	if requested.LegalHold != nil {
		status := *requested.LegalHold
		result.LegalHold = &status
	}
	if requested.Retention != nil {
		result.Retention = NormalizeObjectRetention(requested.Retention)
	} else if d := config.DefaultRetention; d != nil {
		var years, days int
		if d.Days != nil {
			days = int(*d.Days)
		} else {
			years = int(*d.Years)
		}
		until := completedAt.UTC().AddDate(years, 0, days).Truncate(time.Microsecond)
		result.Retention = &ObjectRetention{Mode: d.Mode, RetainUntilDate: until}
		if err := result.Retention.Validate(); err != nil {
			return ObjectLock{}, err
		}
	}
	return result, nil
}

// CheckDelete authorizes permanent deletion of this version's content. Call it
// under the same database locks and transaction as the mutation. Marker
// creation and deletion are not content deletion and do not call this method.
// bypassAuthorized must reflect BOTH an explicit request and authorization;
// callers must never populate it from the request header alone.
// The bool reports whether governance bypass was actually needed, for audit.
func (l ObjectLock) CheckDelete(now time.Time, bypassAuthorized bool) (bool, error) {
	if err := l.Validate(); err != nil {
		return false, err
	}
	if l.LegalHold != nil && *l.LegalHold == LegalHoldOn {
		return false, ErrObjectLockAccessDenied
	}
	if l.Retention == nil || !now.Before(l.Retention.RetainUntilDate) {
		return false, nil
	}
	if l.Retention.Mode == RetentionModeGovernance && bypassAuthorized {
		return true, nil
	}
	return false, ErrObjectLockAccessDenied
}

// CheckRetentionChange does not inspect legal hold: hold changes and retention
// changes are independent. Callers enforce PutObjectRetention permission and
// validate future dates for new client retention before invoking this method.
func (l ObjectLock) CheckRetentionChange(next *ObjectRetention, now time.Time, bypassAuthorized bool) (bool, error) {
	if err := l.Validate(); err != nil {
		return false, err
	}
	if err := next.Validate(); err != nil {
		return false, err
	}
	current := l.Retention
	if current == nil || !now.Before(current.RetainUntilDate) {
		return false, nil
	}
	if next != nil && next.Mode == current.Mode && !next.RetainUntilDate.Before(current.RetainUntilDate) {
		return false, nil
	}
	if current.Mode == RetentionModeGovernance && bypassAuthorized {
		return true, nil
	}
	return false, ErrObjectLockAccessDenied
}
