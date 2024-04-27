package blob

import (
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
)

type FilesystemBlobStore struct {
	root string
}

func (bs *FilesystemBlobStore) ensureRootDir() error {
	err := os.MkdirAll(bs.root, os.ModePerm)
	return err
}

func (bs *FilesystemBlobStore) getFilename(blobId BlobId) string {
	blobIdBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(blobIdBytes, uint64(blobId))
	blobFilename := hex.EncodeToString(blobIdBytes)
	return filepath.Join(bs.root, blobFilename)
}

func NewFilesystemBlobStore(root string) (*FilesystemBlobStore, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	bs := &FilesystemBlobStore{
		root: root,
	}
	err = bs.ensureRootDir()
	if err != nil {
		return nil, err
	}
	return bs, nil
}

func calculateMd5Sum(file *os.File) (*string, error) {
	hash := md5.New()
	data, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}
	_, err = hash.Write(data)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	hexSum := hex.EncodeToString(sum)
	return &hexSum, nil
}

func calculateETag(file *os.File) (*string, error) {
	md5Sum, err := calculateMd5Sum(file)
	if err != nil {
		return nil, err
	}
	etag := "\"" + *md5Sum + "\""
	return &etag, nil
}

func calculateETagFromPath(path string) (*string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	etag, err := calculateETag(f)
	if err != nil {
		return nil, err
	}
	return etag, nil
}

func (bs *FilesystemBlobStore) PutBlob(blob io.Reader) (*PutBlobResult, error) {
	blobIdBytes := make([]byte, 8)
	_, err := rand.Read(blobIdBytes)
	if err != nil {
		return nil, err
	}
	blobId := BlobId(binary.LittleEndian.Uint64(blobIdBytes))
	filename := bs.getFilename(blobId)
	{
		f, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		_, err = io.Copy(f, blob)
		if err != nil {
			return nil, err
		}
	}
	etag, err := calculateETagFromPath(filename)
	if err != nil {
		return nil, err
	}
	stat, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	return &PutBlobResult{
		BlobId: blobId,
		ETag:   *etag,
		Size:   stat.Size(),
	}, nil
}

func (bs *FilesystemBlobStore) GetBlob(blobId BlobId) (io.ReadSeekCloser, error) {
	filename := bs.getFilename(blobId)
	f, err := os.OpenFile(filename, os.O_RDONLY, 0o600)
	return f, err
}

func (bs *FilesystemBlobStore) DeleteBlob(blobId BlobId) error {
	filename := bs.getFilename(blobId)
	err := os.Remove(filename)
	return err
}

func (bs *FilesystemBlobStore) Clear() error {
	err := os.RemoveAll(bs.root)
	if err != nil {
		return err
	}
	err = bs.ensureRootDir()
	return err
}
