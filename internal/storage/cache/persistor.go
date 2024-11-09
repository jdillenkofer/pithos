package cache

import (
	"encoding/base64"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

type CachePersistor interface {
	Store(key string, val []byte) error
	Get(key string) ([]byte, error)
	Remove(key string) error
	RemoveAll() error
}

type InMemoryCachePersistor struct {
	keyToCacheEntryMap map[string][]byte
}

func NewInMemoryCachePersistor() (*InMemoryCachePersistor, error) {
	return &InMemoryCachePersistor{
		keyToCacheEntryMap: make(map[string][]byte),
	}, nil
}

func (cs *InMemoryCachePersistor) Store(key string, val []byte) error {
	cs.keyToCacheEntryMap[key] = val
	return nil
}

func (cs *InMemoryCachePersistor) Get(key string) ([]byte, error) {
	val, ok := cs.keyToCacheEntryMap[key]
	if !ok {
		return nil, ErrCacheMiss
	}
	return val, nil
}

func (cs *InMemoryCachePersistor) Remove(key string) error {
	delete(cs.keyToCacheEntryMap, key)
	return nil
}

func (cs *InMemoryCachePersistor) RemoveAll() error {
	for key := range cs.keyToCacheEntryMap {
		delete(cs.keyToCacheEntryMap, key)
	}
	return nil
}

type FilesystemCachePersistor struct {
	root string
}

func NewFilesystemCachePersistor(root string) (*FilesystemCachePersistor, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	fp := &FilesystemCachePersistor{
		root: root,
	}
	err = fp.ensureRootDir()
	if err != nil {
		return nil, err
	}
	return fp, nil
}

func (fp *FilesystemCachePersistor) ensureRootDir() error {
	err := os.MkdirAll(fp.root, os.ModePerm)
	return err
}

func (fp *FilesystemCachePersistor) getFilename(key string) string {
	filename := base64.StdEncoding.EncodeToString([]byte(key)) + ".cache"
	return filepath.Join(fp.root, filename)
}

func (fp *FilesystemCachePersistor) Store(key string, val []byte) error {
	filename := fp.getFilename(key)
	{
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = f.Write(val)
		if err != nil {
			return err
		}
	}
	return nil
}

func (fp *FilesystemCachePersistor) Get(key string) ([]byte, error) {
	filename := fp.getFilename(key)
	f, err := os.OpenFile(filename, os.O_RDONLY, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrCacheMiss
		}
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (fp *FilesystemCachePersistor) Remove(key string) error {
	filename := fp.getFilename(key)
	err := os.Remove(filename)
	if err != nil {
		e, ok := err.(*os.PathError)
		if ok && e.Err == syscall.ENOENT {
			// The file didn't exist
		} else {
			return err
		}
	}
	return nil
}

func (fp *FilesystemCachePersistor) RemoveAll() error {
	glob := filepath.Join(fp.root, "*.cache")
	files, _ := filepath.Glob(glob)
	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			e, ok := err.(*os.PathError)
			if ok && e.Err == syscall.ENOENT {
				// The file didn't exist
			} else {
				return err
			}
		}
	}

	return nil
}
