package config

import (
	"encoding/json"
	"errors"

	internalConfig "github.com/jdillenkofer/pithos/internal/config"
	"github.com/jdillenkofer/pithos/internal/dependencyinjection"
	databaseConfig "github.com/jdillenkofer/pithos/internal/storage/database/config"
	sqliteBlobContent "github.com/jdillenkofer/pithos/internal/storage/database/repository/blobcontent/sqlite"
	sqliteBlobOutboxEntry "github.com/jdillenkofer/pithos/internal/storage/database/repository/bloboutboxentry/sqlite"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/filesystem"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/encryption"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/tracing"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/outbox"
	sqlBlobStore "github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/sql"
)

const (
	filesystemBlobStoreType           = "FilesystemBlobStore"
	encryptionBlobStoreMiddlewareType = "EncryptionBlobStoreMiddleware"
	tracingBlobStoreMiddlewareType    = "TracingBlobStoreMiddleware"
	outboxBlobStoreType               = "OutboxBlobStore"
	sqlBlobStoreType                  = "SqlBlobStore"
)

type BlobStoreInstantiator = internalConfig.DynamicJsonInstantiator[blobstore.BlobStore]

type FilesystemBlobStoreConfiguration struct {
	Root string `json:"root"`
	internalConfig.DynamicJsonType
}

func (f *FilesystemBlobStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	return nil
}

func (f *FilesystemBlobStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
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

func (e *EncryptionBlobStoreMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := e.InnerBlobStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (e *EncryptionBlobStoreMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	innerBlobStore, err := e.InnerBlobStoreInstantiator.Instantiate(diProvider)
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

func (t *TracingBlobStoreMiddlewareConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := t.InnerBlobStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (t *TracingBlobStoreMiddlewareConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	innerBlobStore, err := t.InnerBlobStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return tracing.New(t.RegionName, innerBlobStore)
}

type OutboxBlobStoreConfiguration struct {
	DatabaseInstantiator       databaseConfig.DatabaseInstantiator `json:"-"`
	RawDatabase                json.RawMessage                     `json:"db"`
	InnerBlobStoreInstantiator BlobStoreInstantiator               `json:"-"`
	RawInnerBlobStore          json.RawMessage                     `json:"innerBlobStore"`
	internalConfig.DynamicJsonType
}

func (o *OutboxBlobStoreConfiguration) UnmarshalJSON(b []byte) error {
	type outboxBlobStoreConfiguration OutboxBlobStoreConfiguration
	err := json.Unmarshal(b, (*outboxBlobStoreConfiguration)(o))
	if err != nil {
		return err
	}
	o.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(o.RawDatabase)
	if err != nil {
		return err
	}
	o.InnerBlobStoreInstantiator, err = CreateBlobStoreInstantiatorFromJson(o.RawInnerBlobStore)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxBlobStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := o.DatabaseInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	err = o.InnerBlobStoreInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (o *OutboxBlobStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	db, err := o.DatabaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	blobOutboxEntryRepository, err := sqliteBlobOutboxEntry.NewRepository(db)
	if err != nil {
		return nil, err
	}
	innerBlobStore, err := o.InnerBlobStoreInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	return outbox.New(db, innerBlobStore, blobOutboxEntryRepository)
}

type SqlBlobStoreConfiguration struct {
	DatabaseInstantiator databaseConfig.DatabaseInstantiator `json:"-"`
	RawDatabase          json.RawMessage                     `json:"db"`
	internalConfig.DynamicJsonType
}

func (s *SqlBlobStoreConfiguration) UnmarshalJSON(b []byte) error {
	type sqlBlobStoreConfiguration SqlBlobStoreConfiguration
	err := json.Unmarshal(b, (*sqlBlobStoreConfiguration)(s))
	if err != nil {
		return err
	}
	s.DatabaseInstantiator, err = databaseConfig.CreateDatabaseInstantiatorFromJson(s.RawDatabase)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlBlobStoreConfiguration) RegisterReferences(diCollection dependencyinjection.DICollection) error {
	err := s.DatabaseInstantiator.RegisterReferences(diCollection)
	if err != nil {
		return err
	}
	return nil
}

func (s *SqlBlobStoreConfiguration) Instantiate(diProvider dependencyinjection.DIProvider) (blobstore.BlobStore, error) {
	db, err := s.DatabaseInstantiator.Instantiate(diProvider)
	if err != nil {
		return nil, err
	}
	blobContentRepository, err := sqliteBlobContent.NewRepository(db)
	if err != nil {
		return nil, err
	}
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
	case filesystemBlobStoreType:
		bi = &FilesystemBlobStoreConfiguration{}
	case encryptionBlobStoreMiddlewareType:
		bi = &EncryptionBlobStoreMiddlewareConfiguration{}
	case tracingBlobStoreMiddlewareType:
		bi = &TracingBlobStoreMiddlewareConfiguration{}
	case outboxBlobStoreType:
		bi = &OutboxBlobStoreConfiguration{}
	case sqlBlobStoreType:
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
