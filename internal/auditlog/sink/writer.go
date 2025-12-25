package sink

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
)

type WriterSink struct {
	writer       io.Writer
	closer       io.Closer
	serializer   serialization.Serializer
	mu           sync.Mutex
	buf          bytes.Buffer
	initialState *InitialState
}

func NewWriterSink(writer io.Writer, serializer serialization.Serializer) *WriterSink {
	return &WriterSink{
		writer:     writer,
		serializer: serializer,
	}
}

func (s *WriterSink) WithCloser(closer io.Closer) *WriterSink {
	s.closer = closer
	return s
}

func (s *WriterSink) InitialState() (*InitialState, error) {
	return s.initialState, nil
}

func NewFileSink(path string, serializer serialization.Serializer) (*WriterSink, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}

	var lastHash []byte
	var hashBuffer [][]byte

	if info.Size() > 0 {
		if _, err := f.Seek(0, 0); err != nil {
			f.Close()
			return nil, err
		}

		dec := serializer.NewDecoder(f)
		val := auditlog.NewValidator(nil, nil)
		for {
			e, err := dec.Decode()
			if err != nil {
				if err == io.EOF {
					break // Successfully read all entries
				}
				f.Close()
				return nil, fmt.Errorf("failed to read audit log entry %d: %w", val.Index, err)
			}

			if err := val.ValidateEntry(e); err != nil {
				f.Close()
				return nil, err
			}

			lastHash = e.Hash
			hashBuffer = val.HashBuffer
		}

		if lastHash == nil {
			f.Close()
			return nil, fmt.Errorf("no valid audit log entries could be recovered from non-empty file: %s", path)
		}
	}

	// Reset offset to end for appending
	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return nil, err
	}

	ws := NewWriterSink(f, serializer).WithCloser(f)
	if lastHash != nil {
		ws.initialState = &InitialState{
			LastHash:   lastHash,
			HashBuffer: hashBuffer,
		}
	}

	return ws, nil
}

func (s *WriterSink) WriteEntry(e *auditlog.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.buf.Reset()
	if err := s.serializer.Encode(&s.buf, e); err != nil {
		return err
	}

	_, err := s.writer.Write(s.buf.Bytes())
	return err
}

func (s *WriterSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closer != nil {
		return s.closer.Close()
	}

	return nil
}
