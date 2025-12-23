package sink

import (
	"github.com/jdillenkofer/pithos/internal/auditlog"
)

type InitialState struct {
	LastHash   []byte
	HashBuffer [][]byte
}

type Sink interface {
	WriteEntry(entry *auditlog.Entry) error
	Close() error
}

type InitialStateProvider interface {
	InitialState() (*InitialState, error)
}
