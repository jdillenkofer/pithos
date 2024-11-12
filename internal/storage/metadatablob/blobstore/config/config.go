package config

import (
	"database/sql"
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent"
	"github.com/jdillenkofer/pithos/internal/storage/database/repository/bloboutboxentry"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/encryption"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/tracing"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/outbox"
	sqlBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/sql"
)

const (
	FilesystemBlobStoreType           = "FilesystemBlobStore"
	EncryptionBlobStoreMiddlewareType = "EncryptionBlobStoreMiddleware"
	TracingBlobStoreMiddlewareType    = "TracingBlobStoreMiddleware"
	OutboxBlobStoreType               = "OutboxBlobStore"
	SqlBlobStoreType                  = "SqlBlobStore"
)

type BlobStoreInstantiator = internalConfig.DynamicJsonInstantiator[blobstore.BlobStore]

type FilesystemBlobStoreConfiguration struct {
	Root string `json:"root"`
	internalConfig.DynamicJsonType
}

func (f *FilesystemBlobStoreConfiguration) Instantiate() (blobstore.BlobStore, error) {
	return filesystem.New(f.Root)
}

type EncryptionBlobStoreMiddlewareConfiguration struct {
	Password                   string                `json:"password"`
	InnerBlobStoreInstantiator BlobStoreInstantiator `json:"-"`
	RawInnerBlobStore          json.RawMessage       `json:"innerBlobStore"`
	internalConfig.DynamicJsonType
}

func (e *EncryptionBlobStoreMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type encryptionBlobStoreMiddlewareConfiguration EncryptionBlobStoreMiddlewareConfiguration
	err := json.Unmarshal(b, (*encryptionBlobStoreMiddlewareConfiguration)(e))
	if err != nil {
		return err
	}
	e.InnerBlobStoreInstantiator, err = CreateBlobStoreInstantiatorFromJson(e.RawInnerBlobStore)
	if err != nil {
		return err
	}
	return nil
}

func (e *EncryptionBlobStoreMiddlewareConfiguration) Instantiate() (blobstore.BlobStore, error) {
	innerBlobStore, err := e.InnerBlobStoreInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	return encryption.New(e.Password, innerBlobStore)
}

type TracingBlobStoreMiddlewareConfiguration struct {
	RegionName                 string                `json:"regionName"`
	InnerBlobStoreInstantiator BlobStoreInstantiator `json:"-"`
	RawInnerBlobStore          json.RawMessage       `json:"innerBlobStore"`
	internalConfig.DynamicJsonType
}

func (t *TracingBlobStoreMiddlewareConfiguration) UnmarshalJSON(b []byte) error {
	type tracingBlobStoreMiddlewareConfiguration TracingBlobStoreMiddlewareConfiguration
	err := json.Unmarshal(b, (*tracingBlobStoreMiddlewareConfiguration)(t))
	if err != nil {
		return err
	}
	t.InnerBlobStoreInstantiator, err = CreateBlobStoreInstantiatorFromJson(t.RawInnerBlobStore)
	if err != nil {
		return err
	}
	return nil
}

func (t *TracingBlobStoreMiddlewareConfiguration) Instantiate() (blobstore.BlobStore, error) {
	innerBlobStore, err := t.InnerBlobStoreInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	return tracing.New(t.RegionName, innerBlobStore)
}

type OutboxBlobStoreConfiguration struct {
	InnerBlobStoreInstantiator BlobStoreInstantiator `json:"-"`
	RawInnerBlobStore          json.RawMessage       `json:"innerBlobStore"`
	internalConfig.DynamicJsonType
}

func (o *OutboxBlobStoreConfiguration) UnmarshalJSON(b []byte) error {
	type outboxBlobStoreConfiguration OutboxBlobStoreConfiguration
	err := json.Unmarshal(b, (*outboxBlobStoreConfiguration)(o))
	if err != nil {
		return err
	}
	o.InnerBlobStoreInstantiator, err = CreateBlobStoreInstantiatorFromJson(o.RawInnerBlobStore)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxBlobStoreConfiguration) Instantiate() (blobstore.BlobStore, error) {
	// @TODO: use real db
	var db *sql.DB = nil
	// @TODO: use real repository
	var blobOutboxEntryRepository bloboutboxentry.Repository = nil
	/*
		blobOutboxEntryRepository, err := sqliteBlobOutboxEntry.NewRepository(db)
		if err != nil {
			return nil, err
		}
	*/
	innerBlobStore, err := o.InnerBlobStoreInstantiator.Instantiate()
	if err != nil {
		return nil, err
	}
	return outbox.New(db, innerBlobStore, blobOutboxEntryRepository)
}

type SqlBlobStoreConfiguration struct {
	internalConfig.DynamicJsonType
}

func (s *SqlBlobStoreConfiguration) Instantiate() (blobstore.BlobStore, error) {
	// @TODO: use real db
	var db *sql.DB = nil
	// @TODO: use real repository
	var blobContentRepository blobcontent.Repository = nil
	/*
		blobContentRepository, err := sqliteBlobContent.NewRepository(db)
		if err != nil {
			return nil, err
		}
	*/
	return sqlBlobStore.New(db, blobContentRepository)
}

func CreateBlobStoreInstantiatorFromJson(b []byte) (BlobStoreInstantiator, error) {
	var bc internalConfig.DynamicJsonType
	err := json.Unmarshal(b, &bc)
	if err != nil {
		return nil, err
	}

	var bi BlobStoreInstantiator
	switch bc.Type {
	case FilesystemBlobStoreType:
		bi = &FilesystemBlobStoreConfiguration{}
	case EncryptionBlobStoreMiddlewareType:
		bi = &EncryptionBlobStoreMiddlewareConfiguration{}
	case TracingBlobStoreMiddlewareType:
		bi = &TracingBlobStoreMiddlewareConfiguration{}
	case OutboxBlobStoreType:
		bi = &OutboxBlobStoreConfiguration{}
	case SqlBlobStoreType:
		bi = &SqlBlobStoreConfiguration{}
	default:
		return nil, errors.New("unknown blobStore type")
	}
	err = json.Unmarshal(b, &bi)
	if err != nil {
		return nil, err
	}
	return bi, nil
}
