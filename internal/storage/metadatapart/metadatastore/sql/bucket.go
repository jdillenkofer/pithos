package sql

import (
	"context"
	"database/sql"
	"encoding/json"

	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bucket"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

func (sms *sqlMetadataStore) CreateBucket(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.CreateBucket")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if *exists {
		return metadatastore.ErrBucketAlreadyExists
	}

	err = sms.bucketRepository.SaveBucket(ctx, tx, &bucket.Entity{
		Name: bucketName,
	})
	if err != nil {
		return err
	}

	return nil
}

func (sms *sqlMetadataStore) DeleteBucket(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteBucket")
	defer span.End()

	exists, err := sms.bucketRepository.ExistsBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if !*exists {
		return metadatastore.ErrNoSuchBucket
	}

	containsBucketObjects, err := sms.objectRepository.ContainsBucketObjectsByBucketName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if *containsBucketObjects {
		return metadatastore.ErrBucketNotEmpty
	}

	err = sms.bucketRepository.DeleteBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}

	return nil
}

func (sms *sqlMetadataStore) ListBuckets(ctx context.Context, tx *sql.Tx) ([]metadatastore.Bucket, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.ListBuckets")
	defer span.End()

	bucketEntities, err := sms.bucketRepository.FindAllBuckets(ctx, tx)
	if err != nil {
		return nil, err
	}
	buckets := sliceutils.Map(func(bucketEntity bucket.Entity) metadatastore.Bucket {
		return metadatastore.Bucket{
			Name:         bucketEntity.Name,
			CreationDate: bucketEntity.CreatedAt,
		}
	}, bucketEntities)

	return buckets, nil
}

func (sms *sqlMetadataStore) HeadBucket(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.Bucket, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.HeadBucket")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	bucket := metadatastore.Bucket{
		Name:         bucketEntity.Name,
		CreationDate: bucketEntity.CreatedAt,
	}

	return &bucket, nil
}

func (sms *sqlMetadataStore) GetBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.WebsiteConfiguration, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetBucketWebsiteConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	if bucketEntity.WebsiteIndexDocumentSuffix == nil && bucketEntity.WebsiteRedirectAllHostName == nil && bucketEntity.WebsiteRoutingRulesJSON == nil {
		return nil, metadatastore.ErrNoSuchWebsiteConfiguration
	}

	config := &metadatastore.WebsiteConfiguration{
		ErrorDocumentKey: bucketEntity.WebsiteErrorDocumentKey,
	}
	if bucketEntity.WebsiteIndexDocumentSuffix != nil {
		config.IndexDocumentSuffix = *bucketEntity.WebsiteIndexDocumentSuffix
	}
	if bucketEntity.WebsiteRedirectAllHostName != nil {
		config.RedirectAllRequestsTo = &metadatastore.WebsiteRedirectAllRequestsTo{
			HostName: *bucketEntity.WebsiteRedirectAllHostName,
			Protocol: bucketEntity.WebsiteRedirectAllProtocol,
		}
	}
	if bucketEntity.WebsiteRoutingRulesJSON != nil {
		if err := json.Unmarshal([]byte(*bucketEntity.WebsiteRoutingRulesJSON), &config.RoutingRules); err != nil {
			return nil, err
		}
	}

	return config, nil
}

func (sms *sqlMetadataStore) PutBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, config *metadatastore.WebsiteConfiguration) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutBucketWebsiteConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	if config.IndexDocumentSuffix == "" {
		bucketEntity.WebsiteIndexDocumentSuffix = nil
	} else {
		bucketEntity.WebsiteIndexDocumentSuffix = &config.IndexDocumentSuffix
	}
	bucketEntity.WebsiteErrorDocumentKey = config.ErrorDocumentKey
	bucketEntity.WebsiteRedirectAllHostName = nil
	bucketEntity.WebsiteRedirectAllProtocol = nil
	if config.RedirectAllRequestsTo != nil {
		bucketEntity.WebsiteRedirectAllHostName = &config.RedirectAllRequestsTo.HostName
		bucketEntity.WebsiteRedirectAllProtocol = config.RedirectAllRequestsTo.Protocol
	}
	bucketEntity.WebsiteRoutingRulesJSON = nil
	if len(config.RoutingRules) > 0 {
		routingRulesJSON, err := json.Marshal(config.RoutingRules)
		if err != nil {
			return err
		}
		routingRulesJSONStr := string(routingRulesJSON)
		bucketEntity.WebsiteRoutingRulesJSON = &routingRulesJSONStr
	}

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) DeleteBucketWebsiteConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteBucketWebsiteConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	bucketEntity.WebsiteIndexDocumentSuffix = nil
	bucketEntity.WebsiteErrorDocumentKey = nil
	bucketEntity.WebsiteRedirectAllHostName = nil
	bucketEntity.WebsiteRedirectAllProtocol = nil
	bucketEntity.WebsiteRoutingRulesJSON = nil

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) GetBucketVersioningConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.BucketVersioningConfiguration, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetBucketVersioningConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}

	if bucketEntity.VersioningStatus == nil {
		return &metadatastore.BucketVersioningConfiguration{}, nil
	}

	status := metadatastore.BucketVersioningStatus(*bucketEntity.VersioningStatus)
	return &metadatastore.BucketVersioningConfiguration{Status: &status}, nil
}

func (sms *sqlMetadataStore) PutBucketVersioningConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, config *metadatastore.BucketVersioningConfiguration) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutBucketVersioningConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	if config == nil || config.Status == nil {
		bucketEntity.VersioningStatus = nil
	} else {
		status := string(*config.Status)
		bucketEntity.VersioningStatus = &status
	}

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) GetBucketCORSConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.BucketCORSConfiguration, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetBucketCORSConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}
	if bucketEntity.CORSConfigurationJSON == nil {
		return nil, metadatastore.ErrNoSuchCORSConfiguration
	}

	var config metadatastore.BucketCORSConfiguration
	err = json.Unmarshal([]byte(*bucketEntity.CORSConfigurationJSON), &config)
	if err != nil {
		return nil, err
	}
	if config.Rules == nil {
		config.Rules = []metadatastore.CORSRule{}
	}
	return &config, nil
}

func (sms *sqlMetadataStore) PutBucketCORSConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, config *metadatastore.BucketCORSConfiguration) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutBucketCORSConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	jsonConfig, err := json.Marshal(config)
	if err != nil {
		return err
	}
	jsonString := string(jsonConfig)
	bucketEntity.CORSConfigurationJSON = &jsonString

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) DeleteBucketCORSConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteBucketCORSConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	bucketEntity.CORSConfigurationJSON = nil

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) GetBucketLifecycleConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.BucketLifecycleConfiguration, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetBucketLifecycleConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}
	if bucketEntity.LifecycleConfigurationJSON == nil {
		return nil, metadatastore.ErrNoSuchLifecycleConfiguration
	}

	var config metadatastore.BucketLifecycleConfiguration
	err = json.Unmarshal([]byte(*bucketEntity.LifecycleConfigurationJSON), &config)
	if err != nil {
		return nil, err
	}
	if config.Rules == nil {
		config.Rules = []metadatastore.LifecycleRule{}
	}
	return &config, nil
}

func (sms *sqlMetadataStore) PutBucketLifecycleConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, config *metadatastore.BucketLifecycleConfiguration) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutBucketLifecycleConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	jsonConfig, err := json.Marshal(config)
	if err != nil {
		return err
	}
	jsonString := string(jsonConfig)
	bucketEntity.LifecycleConfigurationJSON = &jsonString

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) DeleteBucketLifecycleConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.DeleteBucketLifecycleConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	bucketEntity.LifecycleConfigurationJSON = nil

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}

func (sms *sqlMetadataStore) GetBucketNotificationConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName) (*metadatastore.BucketNotificationConfiguration, error) {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.GetBucketNotificationConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return nil, err
	}
	if bucketEntity == nil {
		return nil, metadatastore.ErrNoSuchBucket
	}
	if bucketEntity.NotificationConfigurationJSON == nil {
		return &metadatastore.BucketNotificationConfiguration{}, nil
	}

	var config metadatastore.BucketNotificationConfiguration
	err = json.Unmarshal([]byte(*bucketEntity.NotificationConfigurationJSON), &config)
	if err != nil {
		return nil, err
	}
	if config.TopicConfigurations == nil {
		config.TopicConfigurations = []metadatastore.NotificationConfigurationRule{}
	}
	if config.QueueConfigurations == nil {
		config.QueueConfigurations = []metadatastore.NotificationConfigurationRule{}
	}
	if config.CloudFunctionConfigurations == nil {
		config.CloudFunctionConfigurations = []metadatastore.NotificationConfigurationRule{}
	}
	return &config, nil
}

func (sms *sqlMetadataStore) PutBucketNotificationConfiguration(ctx context.Context, tx *sql.Tx, bucketName metadatastore.BucketName, config *metadatastore.BucketNotificationConfiguration) error {
	ctx, span := sms.tracer.Start(ctx, "SqlMetadataStore.PutBucketNotificationConfiguration")
	defer span.End()

	bucketEntity, err := sms.bucketRepository.FindBucketByName(ctx, tx, bucketName)
	if err != nil {
		return err
	}
	if bucketEntity == nil {
		return metadatastore.ErrNoSuchBucket
	}

	if config == nil || (len(config.TopicConfigurations) == 0 && len(config.QueueConfigurations) == 0 && len(config.CloudFunctionConfigurations) == 0 && !config.EventBridgeEnabled) {
		bucketEntity.NotificationConfigurationJSON = nil
		return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
	}

	jsonConfig, err := json.Marshal(config)
	if err != nil {
		return err
	}
	jsonString := string(jsonConfig)
	bucketEntity.NotificationConfigurationJSON = &jsonString

	return sms.bucketRepository.SaveBucket(ctx, tx, bucketEntity)
}
