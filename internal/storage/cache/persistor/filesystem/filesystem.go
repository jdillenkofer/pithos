package filesystem

import (
	"encoding/base64"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"

	"github.com/jdillenkofer/pithos/internal/storage/cache/persistor"
)

type filesystemCachePersistor struct {
	root string
}

func New(root string) (persistor.CachePersistor, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	fp := &filesystemCachePersistor{
		root: root,
	}
	err = fp.ensureRootDir()
	if err != nil {
		return nil, err
	}
	return fp, nil
}

func (fp *filesystemCachePersistor) ensureRootDir() error {
	err := os.MkdirAll(fp.root, os.ModePerm)
	return err
}

func (fp *filesystemCachePersistor) getFilename(key string) string {
	filename := base64.StdEncoding.EncodeToString([]byte(key)) + ".cache"
	return filepath.Join(fp.root, filename)
}

func (fp *filesystemCachePersistor) Store(key string, val []byte) error {
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

func (fp *filesystemCachePersistor) Get(key string) ([]byte, error) {
	filename := fp.getFilename(key)
	f, err := os.OpenFile(filename, os.O_RDONLY, 0o600)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, persistor.ErrCacheMiss
		}
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (fp *filesystemCachePersistor) Remove(key string) error {
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

func (fp *filesystemCachePersistor) RemoveAll() error {
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
