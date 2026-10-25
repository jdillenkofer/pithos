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

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/storage"
)

type Validator struct {
	storage         storage.Storage
	deleteCorrupted bool
	force           bool
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
		opts := storage.ListObjectVersionsOptions{MaxKeys: 1000}
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
				report.Results = append(report.Results, result)
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
			var size int64
			_, calculated, readErr := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(r io.Reader) error {
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
