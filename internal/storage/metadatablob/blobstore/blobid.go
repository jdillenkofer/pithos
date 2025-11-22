package blobstore

import (
	"crypto/rand"
	"errors"

	"github.com/oklog/ulid/v2"
)

type BlobId struct {
	value ulid.ULID
}

func NewRandomBlobId() (*BlobId, error) {
	blobIdBytes := make([]byte, 8)
	_, err := rand.Read(blobIdBytes)
	if err != nil {
		return nil, err
	}
	blobId := BlobId{value: ulid.Make()}
	return &blobId, nil
}

func NewBlobIdFromString(s string) (*BlobId, error) {
	ulidValue, err := ulid.Parse(s)
	if err != nil {
		return nil, err
	}
	return &BlobId{value: ulidValue}, nil
}

func NewBlobIdFromBytes(b []byte) (*BlobId, error) {
	if len(b) != 16 {
		return nil, errors.New("invalid blob id byte length")
	}
	return &BlobId{value: ulid.ULID{
		b[0], b[1], b[2], b[3],
		b[4], b[5], b[6], b[7],
		b[8], b[9], b[10], b[11],
		b[12], b[13], b[14], b[15],
	}}, nil
}

func (b *BlobId) Bytes() []byte {
	return b.value.Bytes()
}

func (b *BlobId) String() string {
	return b.value.String()
}

func (b *BlobId) Equal(other BlobId) bool {
	return b.value.Compare(other.value) == 0
}
