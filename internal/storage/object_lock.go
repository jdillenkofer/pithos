package storage

import (
	"context"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/metadatastore"
)

type ObjectLock = metadatastore.ObjectLock
type ObjectLockObservation = metadatastore.ObjectLockObservation

var WithObjectLockObserver = metadatastore.WithObjectLockObserver
var ObserveObjectLock = metadatastore.ObserveObjectLock

type CreateBucketOptions = metadatastore.CreateBucketOptions
type ObjectRetention = metadatastore.ObjectRetention
type DefaultRetention = metadatastore.DefaultRetention
type ObjectLockConfiguration = metadatastore.ObjectLockConfiguration
type ObjectLockOptions = metadatastore.ObjectLockOptions
type LegalHoldStatus = metadatastore.LegalHoldStatus
type RetentionMode = metadatastore.RetentionMode

const RetentionModeGovernance = metadatastore.RetentionModeGovernance
const RetentionModeCompliance = metadatastore.RetentionModeCompliance
const LegalHoldOn = metadatastore.LegalHoldOn
const LegalHoldOff = metadatastore.LegalHoldOff

var ErrInvalidObjectLockConfiguration = metadatastore.ErrInvalidObjectLockConfiguration
var ErrObjectLockConfigurationNotFound = metadatastore.ErrObjectLockConfigurationNotFound
var ErrObjectLockAccessDenied = metadatastore.ErrObjectLockAccessDenied
var ErrObjectLockMethodNotAllowed = metadatastore.ErrObjectLockMethodNotAllowed

type ObjectLockManager interface {
	GetObjectLockConfiguration(ctx context.Context, bucketName BucketName) (*ObjectLockConfiguration, error)
	PutObjectLockConfiguration(ctx context.Context, bucketName BucketName, config *ObjectLockConfiguration) error
	GetObjectRetention(ctx context.Context, bucketName BucketName, key ObjectKey, opts *ObjectLockOptions) (*ObjectRetention, error)
	PutObjectRetention(ctx context.Context, bucketName BucketName, key ObjectKey, retention *ObjectRetention, opts *ObjectLockOptions) error
	GetObjectLegalHold(ctx context.Context, bucketName BucketName, key ObjectKey, opts *ObjectLockOptions) (*LegalHoldStatus, error)
	PutObjectLegalHold(ctx context.Context, bucketName BucketName, key ObjectKey, status LegalHoldStatus, opts *ObjectLockOptions) error
}
