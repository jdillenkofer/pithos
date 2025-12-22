package serialization

import (
	"io"

	"github.com/jdillenkofer/pithos/internal/auditlog"
)

type Serializer interface {
	Encode(w io.Writer, entry *auditlog.Entry) error
	NewDecoder(r io.Reader) Decoder
}

type Decoder interface {
	Decode() (*auditlog.Entry, error)
}
