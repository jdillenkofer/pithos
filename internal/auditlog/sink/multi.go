package sink

import (
	"errors"
	"github.com/jdillenkofer/pithos/internal/auditlog"
)

type MultiSink struct {
	sinks []Sink
}

func NewMultiSink(sinks ...Sink) *MultiSink {
	return &MultiSink{sinks: sinks}
}

func (s *MultiSink) WriteEntry(e *auditlog.Entry) error {
	var errs []error
	for _, sink := range s.sinks {
		if err := sink.WriteEntry(e); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}

func (s *MultiSink) Close() error {
	var errs []error
	for _, sink := range s.sinks {
		if err := sink.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
