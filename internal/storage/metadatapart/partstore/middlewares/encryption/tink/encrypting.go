package tink

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"

	"github.com/tink-crypto/tink-go/v2/subtle"
)

// encryptingReader produces Tink AES-GCM-HKDF streaming ciphertext (see
// seekableDecryptingReader for the format) by pulling plaintext from src.
// tink-go only offers an encrypting io.Writer, which forces an io.Pipe and a
// goroutine between the plaintext source and the inner part store, plus two
// fresh segment-sized buffers per part. This reader instead seals each
// segment in place in a single pooled buffer.
//
// Like tink-go's writer it only emits a full segment once it knows more
// plaintext follows, so the segment flagged as last may be full or empty.
// Compatibility with tink-go's decrypting reader is locked in by tests.
type encryptingReader struct {
	src      io.Reader
	cipher   cipher.AEAD
	nonce    []byte
	pss      int // plaintext segment size
	firstPss int // plaintext size of segment 0, which shares its slot with the tink header

	buf      *[]byte
	out      []byte // ciphertext not yet handed out
	segIndex uint64
	carry    [1]byte
	hasCarry bool
	done     bool
	err      error
}

var segmentBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, DefaultSegmentSize)
		return &buf
	},
}

// getSegmentBuffer returns a buffer of ciphertextSegmentSize bytes, pooled
// for the segment size new parts are written with.
func getSegmentBuffer(ciphertextSegmentSize int) *[]byte {
	if ciphertextSegmentSize == DefaultSegmentSize {
		return segmentBufferPool.Get().(*[]byte)
	}
	buf := make([]byte, ciphertextSegmentSize)
	return &buf
}

func putSegmentBuffer(buf *[]byte) {
	if len(*buf) == DefaultSegmentSize {
		segmentBufferPool.Put(buf)
	}
}

// newEncryptingReader returns a reader yielding prefix, the tink stream
// header and the encrypted segments of src. mainKey, aad and
// ciphertextSegmentSize have the same meaning as for
// streamingaeadsubtle.NewAESGCMHKDF with "SHA256" and firstSegmentOffset 0.
func newEncryptingReader(src io.Reader, prefix []byte, mainKey []byte, aad []byte, ciphertextSegmentSize int) (*encryptingReader, error) {
	tinkHeaderLen := 1 + tinkKeySize + tinkNoncePrefixSize
	if ciphertextSegmentSize <= tinkHeaderLen+tinkTagSize {
		return nil, errors.New("ciphertextSegmentSize too small")
	}

	header := make([]byte, len(prefix)+tinkHeaderLen)
	copy(header, prefix)
	tinkHeader := header[len(prefix):]
	tinkHeader[0] = byte(tinkHeaderLen)
	salt := tinkHeader[1 : 1+tinkKeySize]
	noncePrefix := tinkHeader[1+tinkKeySize:]
	if _, err := rand.Read(salt); err != nil {
		return nil, err
	}
	if _, err := rand.Read(noncePrefix); err != nil {
		return nil, err
	}

	derivedKey, err := subtle.ComputeHKDF("SHA256", mainKey, salt, aad, tinkKeySize)
	if err != nil {
		return nil, err
	}
	aesCipher, err := aes.NewCipher(derivedKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCMWithTagSize(aesCipher, tinkTagSize)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, tinkNonceSize)
	copy(nonce, noncePrefix)

	pss := ciphertextSegmentSize - tinkTagSize
	return &encryptingReader{
		src:      src,
		cipher:   gcm,
		nonce:    nonce,
		pss:      pss,
		firstPss: pss - tinkHeaderLen,
		buf:      getSegmentBuffer(ciphertextSegmentSize),
		out:      header,
	}, nil
}

func (r *encryptingReader) release() {
	if r.buf != nil {
		putSegmentBuffer(r.buf)
		r.buf = nil
		r.out = nil
	}
}

// nextSegment reads and seals the next plaintext segment into r.out.
func (r *encryptingReader) nextSegment() error {
	limit := r.pss
	if r.segIndex == 0 {
		limit = r.firstPss
	}
	buf := (*r.buf)[:limit]
	n := 0
	if r.hasCarry {
		buf[0] = r.carry[0]
		n = 1
		r.hasCarry = false
	}
	m, err := io.ReadFull(r.src, buf[n:])
	n += m
	last := false
	switch err {
	case nil:
		// The segment is full; it is the last one only if src is exhausted.
		k, peekErr := io.ReadFull(r.src, r.carry[:])
		if k == 1 {
			r.hasCarry = true
		} else if peekErr == io.EOF {
			last = true
		} else {
			return peekErr
		}
	case io.EOF, io.ErrUnexpectedEOF:
		last = true
	default:
		return err
	}

	if r.segIndex >= math.MaxUint32 {
		return errors.New("too many segments")
	}
	binary.BigEndian.PutUint32(r.nonce[tinkNoncePrefixSize:], uint32(r.segIndex))
	r.nonce[tinkNoncePrefixSize+4] = 0
	if last {
		r.nonce[tinkNoncePrefixSize+4] = 1
	}
	r.out = r.cipher.Seal(buf[:0], r.nonce, buf[:n], nil)
	r.segIndex++
	r.done = last
	return nil
}

func (r *encryptingReader) Read(p []byte) (int, error) {
	for len(r.out) == 0 {
		if r.err != nil {
			return 0, r.err
		}
		if r.done {
			r.release()
			r.err = io.EOF
			return 0, io.EOF
		}
		if err := r.nextSegment(); err != nil {
			r.release()
			r.err = err
			return 0, err
		}
	}
	n := copy(p, r.out)
	r.out = r.out[n:]
	return n, nil
}

// WriteTo hands each sealed segment to w directly, saving io.Copy the
// intermediate copy through its own buffer.
func (r *encryptingReader) WriteTo(w io.Writer) (int64, error) {
	var total int64
	for {
		if len(r.out) > 0 {
			n, err := w.Write(r.out)
			total += int64(n)
			r.out = r.out[n:]
			if err != nil {
				return total, err
			}
			continue
		}
		if r.err != nil {
			if r.err == io.EOF {
				return total, nil
			}
			return total, r.err
		}
		if r.done {
			r.release()
			r.err = io.EOF
			return total, nil
		}
		if err := r.nextSegment(); err != nil {
			r.release()
			r.err = err
			return total, err
		}
	}
}
