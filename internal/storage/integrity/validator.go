package integrity

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"os"
	"reflect"
	"strings"
	"time"
	"unsafe"

	"github.com/jdillenkofer/pithos/internal/checksumutils"
	"github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/part"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/object"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

type Validator struct {
	storage         storage.Storage
	dbContainer     *config.DbContainer
	deleteCorrupted bool
	force           bool
}

func NewValidator(storage storage.Storage, dbContainer *config.DbContainer, deleteCorrupted bool, force bool) *Validator {
	return &Validator{
		storage:         storage,
		dbContainer:     dbContainer,
		deleteCorrupted: deleteCorrupted,
		force:           force,
	}
}

func (v *Validator) ValidateAll(ctx context.Context) (*ValidationReport, error) {
	report := &ValidationReport{
		StartTime: time.Now(),
		Results:   []ValidationResult{},
	}

	// Find PartStore using reflection
	partStore := findPartStore(v.storage)
	if partStore == nil {
		return nil, fmt.Errorf("could not find PartStore in storage hierarchy")
	}

	// Get database connection
	dbs := v.dbContainer.Dbs()
	if len(dbs) == 0 {
		return nil, fmt.Errorf("no databases found")
	}
	// Use the first DB (usually "db")
	var db database.Database
	for _, d := range dbs {
		db = d
		break
	}

	// Create repositories
	partRepo, err := repository.NewPartRepository(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create part repository: %w", err)
	}
	objectRepo, err := repository.NewObjectRepository(db)
	if err != nil {
		return nil, fmt.Errorf("failed to create object repository: %w", err)
	}

	// List all buckets
	buckets, err := v.storage.ListBuckets(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list buckets: %w", err)
	}
	report.TotalBuckets = len(buckets)

	processedObjects := 0
	startTime := time.Now()

	for i, bucket := range buckets {
		slog.Info(fmt.Sprintf("Processing bucket %s (%d/%d)", bucket.Name, i+1, len(buckets)))

		objects, err := storage.ListAllObjectsOfBucket(ctx, v.storage, bucket.Name)
		if err != nil {
			slog.Error(fmt.Sprintf("Failed to list objects in bucket %s: %v", bucket.Name, err))
			continue
		}

		for _, object := range objects {
			processedObjects++
			report.TotalObjects++

			elapsed := time.Since(startTime)
			rate := float64(processedObjects) / elapsed.Seconds()
			slog.Info(fmt.Sprintf("Validating object %d (Bucket: %s, Object: %s) - Rate: %.2f obj/s",
				processedObjects, bucket.Name, object.Key, rate))

			result := v.validateObject(ctx, db, partStore, partRepo, objectRepo, bucket.Name, object)
			report.Results = append(report.Results, result)

			if result.Success {
				report.SuccessfulObjects++
			} else {
				report.FailedObjects++
				if v.deleteCorrupted {
					if v.confirmDeletion(result) {
						err := v.storage.DeleteObject(ctx, bucket.Name, object.Key)
						if err != nil {
							result.ActionTaken = fmt.Sprintf("Failed to delete: %v", err)
						} else {
							result.ActionTaken = "Deleted"
							report.DeletedObjects++
						}
						// Update the result in the report
						report.Results[len(report.Results)-1] = result
					} else {
						result.ActionTaken = "Skipped"
						report.Results[len(report.Results)-1] = result
					}
				}
			}
		}
	}

	report.EndTime = time.Now()
	return report, nil
}

func (v *Validator) validateObject(ctx context.Context, db database.Database, partStore partstore.PartStore,
	partRepo part.Repository, objectRepo object.Repository,
	bucketName storage.BucketName, object storage.Object) ValidationResult {

	result := ValidationResult{
		BucketName: bucketName.String(),
		ObjectKey:  object.Key.String(),
		Success:    true,
	}

	// Get object ID (we need to query the DB to get the ULID for the object)
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		result.Success = false
		result.ErrorType = fmt.Sprintf("DB Error: %v", err)
		return result
	}
	defer tx.Rollback()

	objEntity, err := objectRepo.FindObjectByBucketNameAndKey(ctx, tx, bucketName, object.Key)
	if err != nil {
		result.Success = false
		result.ErrorType = fmt.Sprintf("Object not found in DB: %v", err)
		return result
	}

	// Get parts
	parts, err := partRepo.FindPartsByObjectIdOrderBySequenceNumberAsc(ctx, tx, *objEntity.Id)
	if err != nil {
		result.Success = false
		result.ErrorType = fmt.Sprintf("Failed to get parts: %v", err)
		return result
	}

	// Validate each part
	var partChecksums []storage.ChecksumValues

	for _, part := range parts {
		// Read part content
		reader, err := partStore.GetPart(ctx, tx, part.PartId)
		if err != nil {
			result.Success = false
			result.ErrorType = "Part retrieval failed"
			result.PartFailures = append(result.PartFailures, PartFailure{
				PartID:         part.PartId.String(),
				SequenceNumber: part.SequenceNumber,
				Error:          fmt.Sprintf("GetPart failed: %v", err),
			})
			continue
		}

		// Calculate checksums
		_, calculated, err := checksumutils.CalculateChecksumsStreaming(ctx, reader, func(r io.Reader) error {
			_, err := io.Copy(io.Discard, r)
			return err
		})
		reader.Close()

		if err != nil {
			result.Success = false
			result.PartFailures = append(result.PartFailures, PartFailure{
				PartID:         part.PartId.String(),
				SequenceNumber: part.SequenceNumber,
				Error:          fmt.Sprintf("Checksum calculation failed: %v", err),
			})
			continue
		}

		partChecksums = append(partChecksums, *calculated)

		// Verify part checksums
		if err := verifyPartChecksums(part, *calculated); err != nil {
			result.Success = false
			result.PartFailures = append(result.PartFailures, PartFailure{
				PartID:         part.PartId.String(),
				SequenceNumber: part.SequenceNumber,
				Error:          err.Error(),
			})
		}
	}

	if !result.Success {
		result.ErrorType = "Part validation failed"
		return result
	}

	// Validate object checksums (derived from parts)
	if err := verifyObjectChecksums(object, parts, partChecksums); err != nil {
		result.Success = false
		result.ErrorType = "Object checksum mismatch"
		result.ObjectFailures = append(result.ObjectFailures, err.Error())
	}

	return result
}

func verifyPartChecksums(part part.Entity, calculated storage.ChecksumValues) error {
	// ETag (MD5)
	if part.ETag != "" && calculated.ETag != nil {
		if part.ETag != *calculated.ETag {
			return fmt.Errorf("ETag mismatch: expected %s, got %s", part.ETag, *calculated.ETag)
		}
	}

	if part.ChecksumCRC32 != nil && calculated.ChecksumCRC32 != nil {
		if *part.ChecksumCRC32 != *calculated.ChecksumCRC32 {
			return fmt.Errorf("CRC32 mismatch")
		}
	}
	if part.ChecksumCRC32C != nil && calculated.ChecksumCRC32C != nil {
		if *part.ChecksumCRC32C != *calculated.ChecksumCRC32C {
			return fmt.Errorf("CRC32C mismatch")
		}
	}
	if part.ChecksumCRC64NVME != nil && calculated.ChecksumCRC64NVME != nil {
		if *part.ChecksumCRC64NVME != *calculated.ChecksumCRC64NVME {
			return fmt.Errorf("CRC64NVME mismatch")
		}
	}
	if part.ChecksumSHA1 != nil && calculated.ChecksumSHA1 != nil {
		if *part.ChecksumSHA1 != *calculated.ChecksumSHA1 {
			return fmt.Errorf("SHA1 mismatch")
		}
	}
	if part.ChecksumSHA256 != nil && calculated.ChecksumSHA256 != nil {
		if *part.ChecksumSHA256 != *calculated.ChecksumSHA256 {
			return fmt.Errorf("SHA256 mismatch")
		}
	}
	return nil
}

func verifyObjectChecksums(object storage.Object, parts []part.Entity, partChecksums []storage.ChecksumValues) error {
	// If single part, object checksums should match part checksums
	if len(parts) == 1 {
		calculated := partChecksums[0]

		if object.ETag != "" && calculated.ETag != nil {
			if object.ETag != *calculated.ETag {
				return fmt.Errorf("object ETag mismatch")
			}
		}
		if object.ChecksumCRC32 != nil && calculated.ChecksumCRC32 != nil {
			if *object.ChecksumCRC32 != *calculated.ChecksumCRC32 {
				return fmt.Errorf("object CRC32 mismatch")
			}
		}
		if object.ChecksumCRC32C != nil && calculated.ChecksumCRC32C != nil {
			if *object.ChecksumCRC32C != *calculated.ChecksumCRC32C {
				return fmt.Errorf("object CRC32C mismatch")
			}
		}
		if object.ChecksumCRC64NVME != nil && calculated.ChecksumCRC64NVME != nil {
			if *object.ChecksumCRC64NVME != *calculated.ChecksumCRC64NVME {
				return fmt.Errorf("object CRC64NVME mismatch")
			}
		}
		if object.ChecksumSHA1 != nil && calculated.ChecksumSHA1 != nil {
			if *object.ChecksumSHA1 != *calculated.ChecksumSHA1 {
				return fmt.Errorf("object SHA1 mismatch")
			}
		}
		if object.ChecksumSHA256 != nil && calculated.ChecksumSHA256 != nil {
			if *object.ChecksumSHA256 != *calculated.ChecksumSHA256 {
				return fmt.Errorf("object SHA256 mismatch")
			}
		}
		return nil
	}

	// Multipart upload
	// Convert parts to PartChecksums
	pChecksums := make([]checksumutils.PartChecksums, len(parts))
	for i, part := range parts {
		pChecksums[i] = checksumutils.PartChecksums{
			ETag:              part.ETag,
			ChecksumCRC32:     part.ChecksumCRC32,
			ChecksumCRC32C:    part.ChecksumCRC32C,
			ChecksumCRC64NVME: part.ChecksumCRC64NVME,
			ChecksumSHA1:      part.ChecksumSHA1,
			ChecksumSHA256:    part.ChecksumSHA256,
			Size:              part.Size,
		}
	}

	checksumType := metadatastore.ChecksumTypeFullObject
	if object.ChecksumType != nil {
		checksumType = *object.ChecksumType
	}

	calculatedChecksums, err := checksumutils.CalculateMultipartChecksums(pChecksums, checksumType)
	if err != nil {
		return fmt.Errorf("failed to calculate multipart checksums: %v", err)
	}

	if object.ETag != "" && calculatedChecksums.ETag != nil {
		if object.ETag != *calculatedChecksums.ETag {
			return fmt.Errorf("multipart ETag mismatch: expected %s, got %s", *calculatedChecksums.ETag, object.ETag)
		}
	}

	if object.ChecksumCRC32 != nil && calculatedChecksums.ChecksumCRC32 != nil {
		if *object.ChecksumCRC32 != *calculatedChecksums.ChecksumCRC32 {
			return fmt.Errorf("multipart CRC32 mismatch: expected %s, got %s", *calculatedChecksums.ChecksumCRC32, *object.ChecksumCRC32)
		}
	}
	if object.ChecksumCRC32C != nil && calculatedChecksums.ChecksumCRC32C != nil {
		if *object.ChecksumCRC32C != *calculatedChecksums.ChecksumCRC32C {
			return fmt.Errorf("multipart CRC32C mismatch: expected %s, got %s", *calculatedChecksums.ChecksumCRC32C, *object.ChecksumCRC32C)
		}
	}
	if object.ChecksumCRC64NVME != nil && calculatedChecksums.ChecksumCRC64NVME != nil {
		if *object.ChecksumCRC64NVME != *calculatedChecksums.ChecksumCRC64NVME {
			return fmt.Errorf("multipart CRC64NVME mismatch: expected %s, got %s", *calculatedChecksums.ChecksumCRC64NVME, *object.ChecksumCRC64NVME)
		}
	}
	if object.ChecksumSHA1 != nil && calculatedChecksums.ChecksumSHA1 != nil {
		if *object.ChecksumSHA1 != *calculatedChecksums.ChecksumSHA1 {
			return fmt.Errorf("multipart SHA1 mismatch: expected %s, got %s", *calculatedChecksums.ChecksumSHA1, *object.ChecksumSHA1)
		}
	}
	if object.ChecksumSHA256 != nil && calculatedChecksums.ChecksumSHA256 != nil {
		if *object.ChecksumSHA256 != *calculatedChecksums.ChecksumSHA256 {
			return fmt.Errorf("multipart SHA256 mismatch: expected %s, got %s", *calculatedChecksums.ChecksumSHA256, *object.ChecksumSHA256)
		}
	}

	return nil
}

func (v *Validator) confirmDeletion(result ValidationResult) bool {
	if v.force {
		return true
	}

	fmt.Printf("\nCorrupted Object Found:\n")
	fmt.Printf("  Bucket: %s\n", result.BucketName)
	fmt.Printf("  Key:    %s\n", result.ObjectKey)
	fmt.Printf("  Error:  %s\n", result.ErrorType)
	fmt.Printf("Delete this object? [y/N]: ")

	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		text := strings.ToLower(strings.TrimSpace(scanner.Text()))
		return text == "y" || text == "yes"
	}
	return false
}

func findPartStore(s interface{}) partstore.PartStore {
	val := reflect.ValueOf(s)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}

	// Check if any field is a PartStore
	partStoreType := reflect.TypeOf((*partstore.PartStore)(nil)).Elem()

	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if field.Type().Implements(partStoreType) {
			// Handle unexported fields
			return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface().(partstore.PartStore)
		}
	}

	// Recurse into fields that implement Storage
	storageType := reflect.TypeOf((*storage.Storage)(nil)).Elem()
	for i := 0; i < val.NumField(); i++ {
		field := val.Field(i)
		if field.Type().Implements(storageType) {
			// Recurse
			inner := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
			if bs := findPartStore(inner); bs != nil {
				return bs
			}
		}
	}
	return nil
}
