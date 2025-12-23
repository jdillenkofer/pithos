package sink

import (
	"bytes"
	"crypto/sha512"
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
		for {
			e, err := dec.Decode()
			if err != nil {
				break // EOF or error
			}
			lastHash = e.Hash
			
			if e.Type == auditlog.EntryTypeLog {
				hashBuffer = append(hashBuffer, e.Hash)
			} else if e.Type == auditlog.EntryTypeGrounding || e.Type == auditlog.EntryTypeGenesis {
				hashBuffer = nil // Reset on grounding or genesis
			}
		}
		
		if lastHash == nil {
			lastHash = make([]byte, sha512.Size)
		}
	} else {
		lastHash = make([]byte, sha512.Size)
	}
	
	// Reset offset to end for appending
	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return nil, err
	}

	ws := NewWriterSink(f, serializer).WithCloser(f)
	ws.initialState = &InitialState{
		LastHash:   lastHash,
		HashBuffer: hashBuffer,
	}

	return ws, nil
}

func NewBinaryFileSink(path string) (*WriterSink, error) {
	return NewFileSink(path, &serialization.BinarySerializer{})
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
