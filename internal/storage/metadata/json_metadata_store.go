package metadata

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jdillenkofer/pithos/internal/sliceutils"
	"github.com/jdillenkofer/pithos/internal/storage/blob"
)

type objectJson struct {
	Key          string        `json:"key"`
	LastModified time.Time     `json:"lastModified"`
	ETag         string        `json:"etag"`
	Size         int64         `json:"size"`
	BlobIds      []blob.BlobId `json:"blobIds"`
}

type bucketJson struct {
	Name         string       `json:"name"`
	CreationDate time.Time    `json:"creationDate"`
	Objects      []objectJson `json:"objects"`
}

type metadata struct {
	Buckets []bucketJson `json:"buckets"`
}

type JsonMetadataStore struct {
	root     string
	metadata metadata
	mutex    sync.RWMutex
}

func (jms *JsonMetadataStore) ensureRootDir() error {
	err := os.MkdirAll(jms.root, os.ModePerm)
	return err
}

func (jms *JsonMetadataStore) metadataFilepath() string {
	filename := filepath.Join(jms.root, "metadata.json")
	return filename
}

func (jms *JsonMetadataStore) loadMetadata() error {
	filename := jms.metadataFilepath()
	f, err := os.OpenFile(filename, os.O_RDONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	return decoder.Decode(&jms.metadata)
}

func (jms *JsonMetadataStore) storeMetadata() error {
	filename := jms.metadataFilepath()
	f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	return encoder.Encode(&jms.metadata)
}

func (jms *JsonMetadataStore) findBucketIndexByName(bucketName string) int {
	for i, bucket := range jms.metadata.Buckets {
		if bucket.Name == bucketName {
			return i
		}
	}
	return -1
}

func (jms *JsonMetadataStore) findBucketByName(bucketName string) *bucketJson {
	index := jms.findBucketIndexByName(bucketName)
	if index == -1 {
		return nil
	}
	return &jms.metadata.Buckets[index]
}

func (bucketJson *bucketJson) findObjectIndexByKey(key string) int {
	for i, object := range bucketJson.Objects {
		if object.Key == key {
			return i
		}
	}
	return -1
}

func (bucketJson *bucketJson) findObjectByKey(key string) *objectJson {
	index := bucketJson.findObjectIndexByKey(key)
	if index == -1 {
		return nil
	}
	return &bucketJson.Objects[index]
}

func convertBucket(bucketJson bucketJson) Bucket {
	return Bucket{
		Name:         bucketJson.Name,
		CreationDate: bucketJson.CreationDate,
	}
}

func convertObject(objectJson objectJson) Object {
	return Object(objectJson)
}

func NewJsonMetadataStore(root string) (*JsonMetadataStore, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	jms := &JsonMetadataStore{
		root: root,
	}
	err = jms.ensureRootDir()
	if err != nil {
		return nil, err
	}
	jms.loadMetadata()
	return jms, nil
}

func (jms *JsonMetadataStore) CreateBucket(bucketName string) error {
	jms.mutex.Lock()
	defer jms.mutex.Unlock()
	index := jms.findBucketIndexByName(bucketName)
	if index != -1 {
		return ErrBucketAlreadyExists
	}
	jms.metadata.Buckets = append(jms.metadata.Buckets, bucketJson{
		Name:         bucketName,
		CreationDate: time.Now(),
		Objects:      []objectJson{},
	})
	err := jms.storeMetadata()
	return err
}

func (jms *JsonMetadataStore) DeleteBucket(bucketName string) error {
	jms.mutex.Lock()
	defer jms.mutex.Unlock()
	index := jms.findBucketIndexByName(bucketName)
	if index == -1 {
		return ErrNoSuchBucket
	}
	b := jms.metadata.Buckets[index]
	if len(b.Objects) > 0 {
		return ErrBucketNotEmpty
	}
	jms.metadata.Buckets = sliceutils.RemoveByIndex(jms.metadata.Buckets, index)
	err := jms.storeMetadata()
	return err
}

func (jms *JsonMetadataStore) ListBuckets() ([]Bucket, error) {
	jms.mutex.RLock()
	defer jms.mutex.RUnlock()
	return sliceutils.Map(convertBucket, jms.metadata.Buckets), nil
}

func (jms *JsonMetadataStore) HeadBucket(bucketName string) (*Bucket, error) {
	jms.mutex.RLock()
	defer jms.mutex.RUnlock()
	bucket := jms.findBucketByName(bucketName)
	if bucket == nil {
		return nil, ErrNoSuchBucket
	}
	b := convertBucket(*bucket)
	return &b, nil
}

func (jms *JsonMetadataStore) ListObjects(bucketName string, prefix string, delimiter string) ([]Object, []string, error) {
	jms.mutex.RLock()
	defer jms.mutex.RUnlock()
	bucket := jms.findBucketByName(bucketName)
	if bucket == nil {
		return nil, nil, ErrNoSuchBucket
	}
	return sliceutils.Map(convertObject, bucket.Objects), nil, nil
}

func (jms *JsonMetadataStore) HeadObject(bucketName string, key string) (*Object, error) {
	jms.mutex.RLock()
	defer jms.mutex.RUnlock()
	bucket := jms.findBucketByName(bucketName)
	if bucket == nil {
		return nil, ErrNoSuchBucket
	}
	object := bucket.findObjectByKey(key)
	if object == nil {
		return nil, ErrNoSuchKey
	}
	obj := convertObject(*object)
	return &obj, nil
}

func (jms *JsonMetadataStore) PutObject(bucketName string, key string, object *Object) error {
	jms.mutex.Lock()
	defer jms.mutex.Unlock()
	bucket := jms.findBucketByName(bucketName)
	if bucket == nil {
		return ErrNoSuchBucket
	}
	index := bucket.findObjectIndexByKey(key)
	if index != -1 {
		bucket.Objects = sliceutils.RemoveByIndex(bucket.Objects, index)
	}
	bucket.Objects = append(bucket.Objects, objectJson{
		Key:          object.Key,
		LastModified: object.LastModified,
		ETag:         object.ETag,
		Size:         object.Size,
		BlobIds:      object.BlobIds,
	})
	err := jms.storeMetadata()
	return err
}

func (jms *JsonMetadataStore) DeleteObject(bucketName string, key string) error {
	jms.mutex.Lock()
	defer jms.mutex.Unlock()
	bucket := jms.findBucketByName(bucketName)
	if bucket == nil {
		return ErrNoSuchBucket
	}
	index := bucket.findObjectIndexByKey(key)
	if index == -1 {
		return ErrNoSuchKey
	}
	bucket.Objects = sliceutils.RemoveByIndex(bucket.Objects, index)
	err := jms.storeMetadata()
	return err
}

func (jms *JsonMetadataStore) Clear() error {
	jms.mutex.Lock()
	defer jms.mutex.Unlock()
	err := os.Remove(jms.metadataFilepath())
	if err != nil {
		return err
	}
	jms.metadata = metadata{}
	return nil
}
