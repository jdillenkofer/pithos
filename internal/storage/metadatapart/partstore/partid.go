package partstore

import (
	"crypto/rand"
	"errors"

	"github.com/oklog/ulid/v2"
)

type PartId struct {
	value ulid.ULID
}

func NewRandomPartId() (*PartId, error) {
	partIdBytes := make([]byte, 8)
	_, err := rand.Read(partIdBytes)
	if err != nil {
		return nil, err
	}
	partId := PartId{value: ulid.Make()}
	return &partId, nil
}

func MustNewPartIdFromString(s string) *PartId {
	ulidValue := ulid.MustParse(s)
	return &PartId{value: ulidValue}
}

func NewPartIdFromBytes(b []byte) (*PartId, error) {
	if len(b) != 16 {
		return nil, errors.New("invalid part id byte length")
	}
	return &PartId{value: ulid.ULID{
		b[0], b[1], b[2], b[3],
		b[4], b[5], b[6], b[7],
		b[8], b[9], b[10], b[11],
		b[12], b[13], b[14], b[15],
	}}, nil
}

func (b *PartId) Bytes() []byte {
	return b.value.Bytes()
}

func (b *PartId) String() string {
	return b.value.String()
}

func (b *PartId) Equal(other PartId) bool {
	return b.value.Compare(other.value) == 0
}
