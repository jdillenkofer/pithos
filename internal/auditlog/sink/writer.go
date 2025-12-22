package sink

import (
	"crypto/sha512"
	"io"
	"os"
	"sync"
	"bufio"

	"github.com/jdillenkofer/pithos/internal/auditlog"
	"github.com/jdillenkofer/pithos/internal/auditlog/serialization"
)

type WriterSink struct {
	writer     io.Writer
	bufWriter  *bufio.Writer
	closer     io.Closer
	serializer serialization.Serializer
	mu         sync.Mutex
}

func NewWriterSink(writer io.Writer, serializer serialization.Serializer) *WriterSink {
	return &WriterSink{
		writer:     writer,
		bufWriter:  bufio.NewWriter(writer),
		serializer: serializer,
	}
}

func (s *WriterSink) WithCloser(closer io.Closer) *WriterSink {
	s.closer = closer
	return s
}

func NewFileSink(path string, serializer serialization.Serializer) (*WriterSink, []byte, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, nil, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	
	var lastHash []byte

	if info.Size() > 0 {
		if _, err := f.Seek(0, 0); err != nil {
			f.Close()
			return nil, nil, err
		}
		
		var lastEntry *auditlog.Entry
		dec := serializer.NewDecoder(f)
		for {
			e, err := dec.Decode()
			if err != nil {
				break // EOF or error
			}
			lastEntry = e
		}
		
		if lastEntry != nil {
			lastHash = lastEntry.Hash
		} else {
			lastHash = make([]byte, sha512.Size)
		}
	} else {
		lastHash = make([]byte, sha512.Size)
	}
	
	// Reset offset to end for appending
	if _, err := f.Seek(0, 2); err != nil {
		f.Close()
		return nil, nil, err
	}

	return NewWriterSink(f, serializer).WithCloser(f), lastHash, nil
}

func NewBinaryFileSink(path string) (*WriterSink, []byte, error) {
	return NewFileSink(path, &serialization.BinarySerializer{})
}

func (s *WriterSink) WriteEntry(e *auditlog.Entry) error {

	s.mu.Lock()

	defer s.mu.Unlock()



	if err := s.serializer.Encode(s.bufWriter, e); err != nil {

		return err

	}

	return s.bufWriter.Flush()

}



func (s *WriterSink) Close() error {

	s.mu.Lock()

	defer s.mu.Unlock()

	

	_ = s.bufWriter.Flush()

	if s.closer != nil {

		return s.closer.Close()

	}

	return nil

}
