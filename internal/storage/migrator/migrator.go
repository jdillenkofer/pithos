package migrator

import (
	"context"
	"errors"

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
	for _, sourceBucket := range allSourceBuckets {
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
	for _, sourceObject := range sourceObjects {
		obj, err := source.GetObject(ctx, bucketName, sourceObject.Key, nil, nil)
		if err != nil {
			return err
		}
		err = destination.PutObject(ctx, bucketName, sourceObject.Key, obj)
		if err != nil {
			return err
		}
	}
	return nil
}
