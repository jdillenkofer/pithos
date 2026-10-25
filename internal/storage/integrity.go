package storage

import (
	"context"
	"io"
)

// IntegrityPart describes a physical part and provides access to its decoded data.
// Open and its returned reader are valid only during the InspectObject callback.
// The callback must close every reader it opens before returning.
type IntegrityPart struct {
	ID        string
	Size      int64
	Checksums ChecksumValues
	Open      func() (io.ReadCloser, error)
}

// IntegrityInspector is an optional backend capability. Inspection uses the
// backend's own metadata transaction and part-store routing, bypassing object
// caches. Storage middleware may expose its backend through Unwrap instead.
type IntegrityInspector interface {
	InspectObject(ctx context.Context, bucket BucketName, key ObjectKey, versionID *string, inspect func(Object, []IntegrityPart) error) error
}
