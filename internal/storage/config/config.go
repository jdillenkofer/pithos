package config

import (
	"encoding/json"
	"errors"

	"github.com/jdillenkofer/pithos/internal/storage"
	"github.com/jdillenkofer/pithos/internal/storage/cache"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/prometheus"
	"github.com/jdillenkofer/pithos/internal/storage/middlewares/tracing"
	"github.com/jdillenkofer/pithos/internal/storage/outbox"
	"github.com/jdillenkofer/pithos/internal/storage/replication"
	"github.com/jdillenkofer/pithos/internal/storage/s3client"
)

const (
	CacheStorageType                = "CacheStorage"
	MetadataBlobStorageType         = "MetadataBlobStorage"
	PrometheusStorageMiddlewareType = "PrometheusStorageMiddleware"
	TracingStorageMiddlewareType    = "TracingStorageMiddleware"
	OutboxStorageType               = "OutboxStorage"
	ReplicationStorageType          = "ReplicationStorage"
	S3ClientStorageType             = "S3ClientStorage"
)

type storageInstatiator interface {
	Instatiate() (storage.Storage, error)
}

type StorageConfiguration struct {
	Type string `json:"type"`
}

type CacheStorageConfiguration struct {
	InnerStorageInstatiator storageInstatiator `json:"-"`
	RawInnerStorage         json.RawMessage    `json:"innerStorage"`
	StorageConfiguration
}

func (c *CacheStorageConfiguration) UnmarshalJSON(b []byte) error {
	type cacheStorageConfiguration CacheStorageConfiguration
	err := json.Unmarshal(b, (*cacheStorageConfiguration)(c))
	if err != nil {
		return err
	}
	c.InnerStorageInstatiator, err = createStorageInstatiatorFromJson(c.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (c *CacheStorageConfiguration) Instatiate() (storage.Storage, error) {
	innerStorage, err := c.InnerStorageInstatiator.Instatiate()
	if err != nil {
		return nil, err
	}
	return cache.New(nil, innerStorage)
}

type MetadataBlobStorageConfiguration struct {
	StorageConfiguration
}

func (m *MetadataBlobStorageConfiguration) Instatiate() (storage.Storage, error) {
	return metadatablob.NewStorage(nil, nil, nil)
}

type PrometheusStorageMiddlewareConfiguration struct {
	InnerStorageInstatiator storageInstatiator `json:"-"`
	RawInnerStorage         json.RawMessage    `json:"innerStorage"`
	StorageConfiguration
}

func (p *PrometheusStorageMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type prometheusStorageMiddlewareConfiguration PrometheusStorageMiddlewareConfiguration
	err := json.Unmarshal(b, (*prometheusStorageMiddlewareConfiguration)(p))
	if err != nil {
		return err
	}
	p.InnerStorageInstatiator, err = createStorageInstatiatorFromJson(p.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (p *PrometheusStorageMiddlewareConfiguration) Instatiate() (storage.Storage, error) {
	innerStorage, err := p.InnerStorageInstatiator.Instatiate()
	if err != nil {
		return nil, err
	}
	// TODO: prometheus registerer
	return prometheus.NewStorageMiddleware(innerStorage, nil)
}

type TracingStorageMiddlewareConfiguration struct {
	RegionName              string             `json:"regionName"`
	InnerStorageInstatiator storageInstatiator `json:"-"`
	RawInnerStorage         json.RawMessage    `json:"innerStorage"`
	StorageConfiguration
}

func (t *TracingStorageMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type tracingStorageMiddlewareConfiguration TracingStorageMiddlewareConfiguration
	err := json.Unmarshal(b, (*tracingStorageMiddlewareConfiguration)(t))
	if err != nil {
		return err
	}
	t.InnerStorageInstatiator, err = createStorageInstatiatorFromJson(t.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (t *TracingStorageMiddlewareConfiguration) Instatiate() (storage.Storage, error) {
	innerStorage, err := t.InnerStorageInstatiator.Instatiate()
	if err != nil {
		return nil, err
	}
	return tracing.NewStorageMiddleware(t.RegionName, innerStorage)
}

type OutboxStorageConfiguration struct {
	InnerStorageInstatiator storageInstatiator `json:"-"`
	RawInnerStorage         json.RawMessage    `json:"innerStorage"`
	StorageConfiguration
}

func (o *OutboxStorageConfiguration) UnmarshalJSON(b []byte) error {
	type outboxStorageConfiguration OutboxStorageConfiguration
	err := json.Unmarshal(b, (*outboxStorageConfiguration)(o))
	if err != nil {
		return err
	}
	o.InnerStorageInstatiator, err = createStorageInstatiatorFromJson(o.RawInnerStorage)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxStorageConfiguration) Instatiate() (storage.Storage, error) {
	innerStorage, err := o.InnerStorageInstatiator.Instatiate()
	if err != nil {
		return nil, err
	}
	return outbox.NewStorage(nil, innerStorage, nil)
}

type ReplicationStorageConfiguration struct {
	PrimaryStorageInstatiator    storageInstatiator   `json:"-"`
	RawPrimaryStorage            json.RawMessage      `json:"primaryStorage"`
	SecondaryStorageInstatiators []storageInstatiator `json:"-"`
	RawSecondaryStorages         []json.RawMessage    `json:"secondaryStorages"`
	StorageConfiguration
}

func (r *ReplicationStorageConfiguration) UnmarshalJSON(b []byte) error {
	type replicationStorageConfiguration ReplicationStorageConfiguration
	err := json.Unmarshal(b, (*replicationStorageConfiguration)(r))
	if err != nil {
		return err
	}
	r.PrimaryStorageInstatiator, err = createStorageInstatiatorFromJson(r.RawPrimaryStorage)
	if err != nil {
		return err
	}
	for _, rawSecondaryStorage := range r.RawSecondaryStorages {
		secondaryStorageInstatiator, err := createStorageInstatiatorFromJson(rawSecondaryStorage)
		if err != nil {
			return err
		}
		r.SecondaryStorageInstatiators = append(r.SecondaryStorageInstatiators, secondaryStorageInstatiator)
	}
	return nil
}

func (r *ReplicationStorageConfiguration) Instatiate() (storage.Storage, error) {
	primaryStorage, err := r.PrimaryStorageInstatiator.Instatiate()
	if err != nil {
		return nil, err
	}
	secondaryStorages := []storage.Storage{}
	for _, secondaryStorageInstatiator := range r.SecondaryStorageInstatiators {
		secondaryStorage, err := secondaryStorageInstatiator.Instatiate()
		if err != nil {
			return nil, err
		}
		secondaryStorages = append(secondaryStorages, secondaryStorage)
	}
	return replication.NewStorage(primaryStorage, secondaryStorages...)
}

type S3ClientStorageConfiguration struct {
	StorageConfiguration
}

func (s *S3ClientStorageConfiguration) Instatiate() (storage.Storage, error) {
	// TODO: s3client
	return s3client.NewStorage(nil)
}

func CreateStorageFromJson(b []byte) (storage.Storage, error) {
	si, err := createStorageInstatiatorFromJson(b)
	if err != nil {
		return nil, err
	}
	return si.Instatiate()
}

func createStorageInstatiatorFromJson(b []byte) (storageInstatiator, error) {
	var sc StorageConfiguration
	err := json.Unmarshal(b, &sc)
	if err != nil {
		return nil, err
	}

	var si storageInstatiator
	switch sc.Type {
	case CacheStorageType:
		si = &CacheStorageConfiguration{}
	case MetadataBlobStorageType:
		si = &MetadataBlobStorageConfiguration{}
	case PrometheusStorageMiddlewareType:
		si = &PrometheusStorageMiddlewareConfiguration{}
	case TracingStorageMiddlewareType:
		si = &TracingStorageMiddlewareConfiguration{}
	case OutboxStorageType:
		si = &OutboxStorageConfiguration{}
	case ReplicationStorageType:
		si = &ReplicationStorageConfiguration{}
	case S3ClientStorageType:
		si = &S3ClientStorageConfiguration{}
	default:
		return nil, errors.New("unknown storage type")
	}
	err = json.Unmarshal(b, &si)
	if err != nil {
		return nil, err
	}
	return si, nil
}
