package migrator

import (
	"context"
	"errors"
	"log"

	"github.com/jdillenkofer/pithos/internal/storage"
)

var ErrDestinationNotEmpty = errors.New("destination storage not empty")

func MigrateStorage(ctx context.Context, source storage.Storage, destination storage.Storage) error {
	missingBuckets, err := determineMissingBuckets(ctx, source, destination)
	if err != nil {
		return err
	}
	err = createMissingBuckets(ctx, missingBuckets, destination)
	if err != nil {
		return err
	}
	allSourceBuckets, err := source.ListBuckets(ctx)
	if err != nil {
		return err
	}
	for i, sourceBucket := range allSourceBuckets {
		log.Printf("Migrating bucket \"%s\" (%d/%d items [%.2f%%])\n", sourceBucket.Name, i, len(allSourceBuckets), float64(i)/float64(len(allSourceBuckets))*100.0)
		err = migrateObjectsOfBucketFromSourceStorageToDestinationStorage(ctx, source, destination, sourceBucket.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

func determineMissingBuckets(ctx context.Context, source, destination storage.Storage) ([]storage.Bucket, error) {
	allSourceBuckets, err := source.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}
	allDestinationBuckets, err := destination.ListBuckets(ctx)
	if err != nil {
		return nil, err
	}

	existingDestinationBucketsByBucketName := map[string]storage.Bucket{}
	for _, destinationBucket := range allDestinationBuckets {
		existingDestinationBucketsByBucketName[destinationBucket.Name] = destinationBucket
	}

	missingSourceBuckets := []storage.Bucket{}
	for _, sourceBucket := range allSourceBuckets {
		_, bucketAlreadyExists := existingDestinationBucketsByBucketName[sourceBucket.Name]
		if !bucketAlreadyExists {
			missingSourceBuckets = append(missingSourceBuckets, sourceBucket)
		}
	}
	return missingSourceBuckets, nil
}

func createMissingBuckets(ctx context.Context, missingBuckets []storage.Bucket, destination storage.Storage) error {
	for _, missingBucket := range missingBuckets {
		err := destination.CreateBucket(ctx, missingBucket.Name)
		if err != nil {
			return err
		}
	}
	return nil
}

func migrateObjectsOfBucketFromSourceStorageToDestinationStorage(ctx context.Context, source, destination storage.Storage, bucketName string) error {
	destinationObjects, err := storage.ListAllObjectsOfBucket(ctx, destination, bucketName)
	if err != nil {
		return err
	}
	if len(destinationObjects) != 0 {
		return ErrDestinationNotEmpty
	}
	sourceObjects, err := storage.ListAllObjectsOfBucket(ctx, source, bucketName)
	if err != nil {
		return err
	}

	var copiedBytes int64 = 0
	var totalBytes int64 = 0
	for _, sourceObject := range sourceObjects {
		totalBytes += sourceObject.Size
	}
	for i, sourceObject := range sourceObjects {
		log.Printf("Migrating object \"%s\" from bucket \"%s\" (%d/%d items [%.2f%%]; %d/%d bytes [%.2f%%])\n", sourceObject.Key, bucketName, i, len(sourceObjects), (float64(i)+1.0)/float64(len(sourceObjects))*100.0, copiedBytes, totalBytes, float64(copiedBytes)*100.0/float64(totalBytes))
		obj, err := source.GetObject(ctx, bucketName, sourceObject.Key, nil, nil)
		if err != nil {
			return err
		}
		// @TODO: Use checksumInput
		_, err = destination.PutObject(ctx, bucketName, sourceObject.Key, nil, obj, nil)
		if err != nil {
			return err
		}
		copiedBytes += sourceObject.Size
	}
	return nil
}
