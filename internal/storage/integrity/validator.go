package integrity

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type Validator struct {
	storage         storage.Storage
	deleteCorrupted bool
	force           bool
	progress        func(int)
	byteProgress    func(int64)
	limits          ResultLimits
}

// listPageSize bounds how many object listings are held in memory at once.
const listPageSize = 1000

// ResultLimits bounds the memory retained by a validation report. Zero values
// mean unlimited. Aggregate counts in the report are always exact.
type ResultLimits struct {
	// FailuresOnly retains only failed results; successes are only counted.
	FailuresOnly bool
	// MaxResults caps retained results; the rest are counted in OmittedResults.
	MaxResults int
	// MaxFailuresPerResult caps the part and object failure details of one
	// result; the rest are counted in ValidationResult.OmittedFailures.
	MaxFailuresPerResult int
	// MaxErrorLength caps the length in bytes of each retained error message.
	MaxErrorLength int
}

// WithProgress reports the number of completed objects on the validation goroutine.
// The callback must not modify the validator or block indefinitely.
func (v *Validator) WithProgress(progress func(int)) *Validator {
	v.progress = progress
	return v
}

// WithByteProgress reports the number of part bytes read since the previous
// call, as validation reads them. It is called frequently on the validation
// goroutine and must be cheap.
func (v *Validator) WithByteProgress(progress func(int64)) *Validator {
	v.byteProgress = progress
	return v
}

// WithResultLimits bounds the results retained in the report.
func (v *Validator) WithResultLimits(limits ResultLimits) *Validator {
	v.limits = limits
	return v
}

func NewValidator(store storage.Storage, deleteCorrupted bool, force bool) *Validator {
	return &Validator{storage: store, deleteCorrupted: deleteCorrupted, force: force}
}

func findInspector(store storage.Storage) (storage.IntegrityInspector, error) {
	for store != nil {
		if inspector, ok := store.(storage.IntegrityInspector); ok {
			return inspector, nil
		}
		wrapper, ok := store.(interface{ Unwrap() storage.Storage })
		if !ok {
			break
		}
		store = wrapper.Unwrap()
	}
	return nil, fmt.Errorf("storage backend does not support integrity inspection")
}

func (v *Validator) ValidateAll(ctx context.Context) (*ValidationReport, error) {
	inspector, err := findInspector(v.storage)
	if err != nil {
		return nil, err
	}
	report := &ValidationReport{StartTime: time.Now(), Results: []ValidationResult{}}
	defer func() { report.EndTime = time.Now() }()
	buckets, err := v.storage.ListBuckets(ctx)
	if err != nil {
		return report, fmt.Errorf("list buckets: %w", err)
	}
	report.TotalBuckets = len(buckets)
	for _, bucket := range buckets {
		opts := storage.ListObjectVersionsOptions{MaxKeys: listPageSize}
		for {
			if err := ctx.Err(); err != nil {
				return report, err
			}
			page, err := v.storage.ListObjectVersions(ctx, bucket.Name, opts)
			if err != nil {
				return report, fmt.Errorf("list versions in bucket %s: %w", bucket.Name, err)
			}
			for _, version := range page.Versions {
				if version.IsDeleteMarker {
					continue
				}
				if err := ctx.Err(); err != nil {
					return report, err
				}
				result := v.validateObject(ctx, inspector, bucket.Name, version)
				if err := ctx.Err(); err != nil {
					return report, err
				}
				report.TotalObjects++
				if result.Success {
					report.SuccessfulObjects++
				} else {
					report.FailedObjects++
					// Read, database and network failures are not proof of corruption.
					// Never delete data merely because it could not be inspected.
					if v.deleteCorrupted && result.Corrupted {
						if version.ETag == nil || *version.ETag == "" {
							result.ActionTaken = "Skipped: missing ETag precondition"
						} else if v.confirmDeletion(result) {
							_, err := v.storage.DeleteObject(ctx, bucket.Name, version.Key, &storage.DeleteObjectOptions{
								VersionID: result.VersionID, IfMatchETag: version.ETag,
							})
							if err != nil {
								result.ActionTaken = fmt.Sprintf("Failed to delete: %v", err)
							} else {
								result.ActionTaken = "Deleted"
								report.DeletedObjects++
							}
						} else {
							result.ActionTaken = "Skipped"
						}
					}
				}
				v.retainResult(report, result)
				if v.progress != nil {
					v.progress(report.TotalObjects)
				}
			}
			if !page.IsTruncated {
				break
			}
			if page.NextKeyMarker == nil || (sameMarker(page.NextKeyMarker, opts.KeyMarker) && sameMarker(page.NextVersionIDMarker, opts.VersionIDMarker)) {
				return report, fmt.Errorf("version listing in bucket %s did not advance", bucket.Name)
			}
			opts.KeyMarker, opts.VersionIDMarker = page.NextKeyMarker, page.NextVersionIDMarker
		}
	}
	return report, nil
}

func (v *Validator) retainResult(report *ValidationReport, result ValidationResult) {
	limits := v.limits
	if result.Success && limits.FailuresOnly {
		return
	}
	if limits.MaxResults > 0 && len(report.Results) >= limits.MaxResults {
		report.OmittedResults++
		return
	}
	if limits.MaxFailuresPerResult > 0 {
		remaining := limits.MaxFailuresPerResult
		if len(result.PartFailures) > remaining {
			result.OmittedFailures += len(result.PartFailures) - remaining
			result.PartFailures = result.PartFailures[:remaining]
		}
		remaining -= len(result.PartFailures)
		if len(result.ObjectFailures) > remaining {
			result.OmittedFailures += len(result.ObjectFailures) - remaining
			result.ObjectFailures = result.ObjectFailures[:remaining]
		}
	}
	if limits.MaxErrorLength > 0 {
		result.ErrorType = truncateError(result.ErrorType, limits.MaxErrorLength)
		result.ActionTaken = truncateError(result.ActionTaken, limits.MaxErrorLength)
		for i := range result.PartFailures {
			result.PartFailures[i].Error = truncateError(result.PartFailures[i].Error, limits.MaxErrorLength)
		}
		for i := range result.ObjectFailures {
			result.ObjectFailures[i] = truncateError(result.ObjectFailures[i], limits.MaxErrorLength)
		}
	}
	report.Results = append(report.Results, result)
}

const truncationMarker = "... (truncated)"

// truncateError cuts message to at most maxLength bytes without splitting a
// UTF-8 sequence. The result never aliases a longer message, so the original
// can be released.
func truncateError(message string, maxLength int) string {
	if len(message) <= maxLength {
		return message
	}
	marker := truncationMarker
	if maxLength <= len(marker) {
		marker = ""
	}
	keep := maxLength - len(marker)
	for keep > 0 && !utf8.RuneStart(message[keep]) {
		keep--
	}
	return strings.Clone(message[:keep]) + marker
}

func (v *Validator) validateObject(ctx context.Context, inspector storage.IntegrityInspector, bucket storage.BucketName, version storage.ObjectVersion) ValidationResult {
	versionID := version.VersionID
	result := ValidationResult{BucketName: bucket.String(), ObjectKey: version.Key.String(), VersionID: &versionID, Success: true}
	err := inspector.InspectObject(ctx, bucket, version.Key, &versionID, func(object storage.Object, parts []storage.IntegrityPart) error {
		var calculatedParts []checksumutils.PartChecksums
		var totalSize int64
		for i, part := range parts {
			if err := ctx.Err(); err != nil {
				return err
			}
			reader, err := part.Open()
			if err != nil {
				result.PartFailures = append(result.PartFailures, PartFailure{PartID: part.ID, SequenceNumber: i + 1, Error: fmt.Sprintf("GetPart failed: %v", err)})
				continue
			}
			var partData io.Reader = reader
			if v.byteProgress != nil {
				partData = &progressReader{reader: reader, progress: v.byteProgress}
			}
			var size int64
			_, calculated, readErr := checksumutils.CalculateChecksumsStreaming(ctx, partData, func(r io.Reader) error {
				var err error
				size, err = io.Copy(io.Discard, r)
				return err
			})
			err = errors.Join(readErr, reader.Close())
			if err != nil {
				result.PartFailures = append(result.PartFailures, PartFailure{PartID: part.ID, SequenceNumber: i + 1, Error: fmt.Sprintf("Read part failed: %v", err)})
				continue
			}
			totalSize += size
			if err := verifyChecksums(part.Checksums, *calculated); err != nil || size != part.Size {
				result.Corrupted = true
				message := fmt.Sprintf("Size mismatch: expected %d, got %d", part.Size, size)
				if err != nil {
					message = err.Error()
				}
				result.PartFailures = append(result.PartFailures, PartFailure{PartID: part.ID, SequenceNumber: i + 1, Error: message})
			}
			calculatedParts = append(calculatedParts, checksumutils.PartChecksums{
				ETag: *calculated.ETag, Size: size,
				ChecksumCRC32: calculated.ChecksumCRC32, ChecksumCRC32C: calculated.ChecksumCRC32C,
				ChecksumCRC64NVME: calculated.ChecksumCRC64NVME, ChecksumSHA1: calculated.ChecksumSHA1,
				ChecksumSHA256: calculated.ChecksumSHA256,
			})
		}
		if len(result.PartFailures) > 0 {
			result.Success, result.ErrorType = false, "Part validation failed"
			return nil
		}
		if totalSize != object.Size || len(parts) == 0 {
			result.Success, result.Corrupted, result.ErrorType = false, true, "Object size or part count mismatch"
			return nil
		}
		var calculated storage.ChecksumValues
		if len(calculatedParts) == 1 && !strings.Contains(object.ETag, "-") {
			p := calculatedParts[0]
			calculated = storage.ChecksumValues{ETag: &p.ETag, ChecksumCRC32: p.ChecksumCRC32, ChecksumCRC32C: p.ChecksumCRC32C, ChecksumCRC64NVME: p.ChecksumCRC64NVME, ChecksumSHA1: p.ChecksumSHA1, ChecksumSHA256: p.ChecksumSHA256}
		} else {
			checksumType := storage.ChecksumTypeFullObject
			if object.ChecksumType != nil {
				checksumType = *object.ChecksumType
			}
			var err error
			calculated, err = checksumutils.CalculateMultipartChecksums(calculatedParts, checksumType)
			if err != nil {
				return fmt.Errorf("calculate multipart checksums: %w", err)
			}
		}
		expected := storage.ChecksumValues{ETag: &object.ETag, ChecksumCRC32: object.ChecksumCRC32, ChecksumCRC32C: object.ChecksumCRC32C, ChecksumCRC64NVME: object.ChecksumCRC64NVME, ChecksumSHA1: object.ChecksumSHA1, ChecksumSHA256: object.ChecksumSHA256}
		if err := verifyChecksums(expected, calculated); err != nil {
			result.Success, result.Corrupted, result.ErrorType = false, true, "Object checksum mismatch"
			result.ObjectFailures = append(result.ObjectFailures, err.Error())
		}
		return nil
	})
	if err != nil {
		result.Success = false
		result.ErrorType = fmt.Sprintf("Inspection failed: %v", err)
		// A failed transaction cannot establish a safe basis for deletion.
		result.Corrupted = false
	}
	return result
}

// progressReader reports every successful read to a progress callback.
type progressReader struct {
	reader   io.Reader
	progress func(int64)
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.progress(int64(n))
	}
	return n, err
}

func sameMarker(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func verifyChecksums(expected, calculated storage.ChecksumValues) error {
	for _, checksum := range []struct {
		name                 string
		expected, calculated *string
	}{
		{"ETag", expected.ETag, calculated.ETag},
		{"CRC32", expected.ChecksumCRC32, calculated.ChecksumCRC32},
		{"CRC32C", expected.ChecksumCRC32C, calculated.ChecksumCRC32C},
		{"CRC64NVME", expected.ChecksumCRC64NVME, calculated.ChecksumCRC64NVME},
		{"SHA1", expected.ChecksumSHA1, calculated.ChecksumSHA1},
		{"SHA256", expected.ChecksumSHA256, calculated.ChecksumSHA256},
	} {
		if checksum.expected == nil || *checksum.expected == "" {
			continue
		}
		if checksum.calculated == nil || *checksum.expected != *checksum.calculated {
			return fmt.Errorf("%s mismatch", checksum.name)
		}
	}
	return nil
}

func (v *Validator) confirmDeletion(result ValidationResult) bool {
	if v.force {
		return true
	}
	fmt.Printf("\nCorrupted Object Found:\n  Bucket: %s\n  Key: %s\n  Version: %s\n  Error: %s\nDelete this version? [y/N]: ", result.BucketName, result.ObjectKey, *result.VersionID, result.ErrorType)
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		text := strings.ToLower(strings.TrimSpace(scanner.Text()))
		return text == "y" || text == "yes"
	}
	return false
}
