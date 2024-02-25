package storage

import (
	"io"
	"os"
	"path/filepath"
)

type FilesystemStorage struct {
	root string
}

func NewFilesystemStorage(root string) (*FilesystemStorage, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	err = os.MkdirAll(root, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &FilesystemStorage{
		root: root,
	}, nil
}

func (fs *FilesystemStorage) getBucketPath(bucket string) string {
	return filepath.Join(fs.root, bucket)
}

func (fs *FilesystemStorage) getKeyPath(bucket string, key string) string {
	return filepath.Join(fs.root, bucket, filepath.Clean(key))
}

func isDirEmpty(name string) (bool, error) {
	f, err := os.Open(name)
	if err != nil {
		return false, err
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err == io.EOF {
		return true, nil
	}
	return false, err
}

func (fs *FilesystemStorage) CreateBucket(bucket string) error {
	bucketFolder := fs.getBucketPath(bucket)
	err := os.Mkdir(bucketFolder, os.ModePerm)
	return err
}

func (fs *FilesystemStorage) DeleteBucket(bucket string) error {
	bucketFolder := fs.getBucketPath(bucket)
	isEmpty, err := isDirEmpty(bucketFolder)
	if err != nil {
		return err
	}
	if !isEmpty {
		return ErrBucketNotEmpty
	}
	err = os.Remove(bucketFolder)
	return err
}

func (fs *FilesystemStorage) ListBuckets() ([]Bucket, error) {
	dirEntries, err := os.ReadDir(fs.root)
	if err != nil {
		return nil, err
	}
	buckets := []Bucket{}
	for _, dirEntry := range dirEntries {
		fileInfo, err := dirEntry.Info()
		if err != nil {
			return nil, err
		}
		buckets = append(buckets, Bucket{
			Name:         dirEntry.Name(),
			CreationDate: fileInfo.ModTime(),
		})
	}
	return buckets, nil
}

func (fs *FilesystemStorage) ExistBucket(bucket string) (*Bucket, error) {
	bucketFolder := fs.getBucketPath(bucket)
	f, err := os.Open(bucketFolder)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &Bucket{
		Name:         f.Name(),
		CreationDate: fileInfo.ModTime(),
	}, nil
}

func (fs *FilesystemStorage) ListObjects(bucket string) ([]Object, error) {
	bucketPath := fs.getBucketPath(bucket)
	objects := []Object{}
	err := filepath.WalkDir(bucketPath, func(path string, d os.DirEntry, err error) error {
		if !d.IsDir() {
			relPath, err := filepath.Rel(bucketPath, path)
			if err == nil {
				fileInfo, err := d.Info()
				if err != nil {
					return err
				}
				objects = append(objects, Object{
					Key:          relPath,
					LastModified: fileInfo.ModTime(),
					ETag:         "",
					Size:         fileInfo.Size(),
				})
			}
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return objects, nil
}

func (fs *FilesystemStorage) ExistObject(bucket string, key string) (*Object, error) {
	keyPath := fs.getKeyPath(bucket, key)
	f, err := os.OpenFile(keyPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &Object{
		Key:          key,
		LastModified: fileInfo.ModTime(),
		ETag:         "",
		Size:         fileInfo.Size(),
	}, nil
}

func (fs *FilesystemStorage) GetObject(bucket string, key string) (io.ReadCloser, error) {
	keyPath := fs.getKeyPath(bucket, key)
	f, err := os.OpenFile(keyPath, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (fs *FilesystemStorage) PutObject(bucket string, key string, reader io.Reader) error {
	keyPath := fs.getKeyPath(bucket, key)
	tmpFile, err := os.CreateTemp(os.TempDir(), "tmp")
	if err != nil {
		return err
	}
	_, err = io.Copy(tmpFile, reader)
	if err != nil {
		return err
	}
	tmpFile.Close()
	dirPath := filepath.Dir(keyPath)
	err = os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return err
	}
	err = os.Rename(tmpFile.Name(), keyPath)
	if err != nil {
		return err
	}
	return nil
}

func (fs *FilesystemStorage) DeleteObject(bucket string, key string) error {
	keyFolder := fs.getKeyPath(bucket, key)
	err := os.Remove(keyFolder)
	return err
}
