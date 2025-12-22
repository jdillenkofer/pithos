package sink

import (
	"github.com/jdillenkofer/pithos/internal/auditlog"
)

type Sink interface {
	WriteEntry(entry *auditlog.Entry) error
	Close() error
}
