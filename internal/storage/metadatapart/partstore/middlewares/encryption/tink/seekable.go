package tink

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/google/tink/go/subtle"
	streamingaeadsubtle "github.com/google/tink/go/streamingaead/subtle"
)

// seekableDecryptingReader decrypts Tink AES-GCM-HKDF streaming ciphertext
// with support for io.Seeker, which the sequential reader from tink-go cannot
// provide. Range GETs skip to their offset via Seek instead of reading and
// decrypting every byte before the range start.
//
// The ciphertext format is produced by tink-go's
// streamingaead/subtle.AESGCMHKDF (NewEncryptingWriter) and is stable:
//
//	tinkHeader || segment_0 || segment_1 || ... || segment_{k-1}
//	tinkHeader = headerLen(1) || salt(keySize) || noncePrefix(7)
//
// With firstSegmentOffset 0, segment_0's ciphertext is
// ciphertextSegmentSize-len(tinkHeader) bytes so that tinkHeader+segment_0
// fills exactly one segment slot; all later segments are
// ciphertextSegmentSize bytes except the (possibly shorter) last one. Each
// segment is AES-GCM with nonce = noncePrefix || uint32BE(segmentIndex) ||
// lastSegmentFlag(1), so any segment can be decrypted independently given its
// index and whether it is the last — both derivable from the ciphertext size.
// Compatibility with tink-go's own writer is locked in by tests that encrypt
// via tink and decrypt at random offsets via this reader.
type seekableDecryptingReader struct {
	r    io.ReadSeeker
	base int64 // offset in r where the tink ciphertext (incl. tink header) begins

	cipher        cipher.AEAD
	noncePrefix   []byte
	tinkHeaderLen int
	css           int64 // ciphertextSegmentSize
	ciphertextLen int64 // total tink ciphertext length including tink header
	numSegments   int64
	plaintextLen  int64

	pos       int64 // current plaintext position
	segIndex  int64 // segment currently buffered, -1 if none
	segStart  int64 // plaintext offset where the buffered segment begins
	plaintext []byte
	segBuf    []byte
}

const (
	tinkNonceSize       = streamingaeadsubtle.AESGCMHKDFNonceSizeInBytes
	tinkNoncePrefixSize = streamingaeadsubtle.AESGCMHKDFNoncePrefixSizeInBytes
	tinkTagSize         = streamingaeadsubtle.AESGCMHKDFTagSizeInBytes
	tinkKeySize         = 32
)

// newSeekableDecryptingReader reads the tink stream header at base and
// prepares segment-level decryption. mainKey and aad must match the values
// used with NewAESGCMHKDF/NewEncryptingWriter; ciphertextSegmentSize must
// match the segment size recorded in the part header.
func newSeekableDecryptingReader(r io.ReadSeeker, base int64, mainKey []byte, aad []byte, ciphertextSegmentSize int) (*seekableDecryptingReader, error) {
	tinkHeaderLen := 1 + tinkKeySize + tinkNoncePrefixSize

	end, err := r.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}
	ciphertextLen := end - base
	if ciphertextLen < int64(tinkHeaderLen)+tinkTagSize {
		return nil, errors.New("ciphertext shorter than tink header plus tag")
	}

	if _, err := r.Seek(base, io.SeekStart); err != nil {
		return nil, err
	}
	header := make([]byte, tinkHeaderLen)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}
	if int(header[0]) != tinkHeaderLen {
		return nil, fmt.Errorf("invalid tink header length: %d", header[0])
	}
	salt := header[1 : 1+tinkKeySize]
	noncePrefix := header[1+tinkKeySize:]

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

	css := int64(ciphertextSegmentSize)
	if css <= int64(tinkHeaderLen)+tinkTagSize {
		return nil, errors.New("ciphertextSegmentSize too small")
	}

	// The tink header occupies the start of the first segment slot, so the
	// slot grid is aligned to css from base and the segment count is simply
	// the number of slots the ciphertext covers.
	numSegments := (ciphertextLen + css - 1) / css
	plaintextLen := ciphertextLen - int64(tinkHeaderLen) - tinkTagSize*numSegments
	if plaintextLen < 0 {
		return nil, errors.New("ciphertext too short for segment count")
	}

	return &seekableDecryptingReader{
		r:             r,
		base:          base,
		cipher:        gcm,
		noncePrefix:   append([]byte(nil), noncePrefix...),
		tinkHeaderLen: tinkHeaderLen,
		css:           css,
		ciphertextLen: ciphertextLen,
		numSegments:   numSegments,
		plaintextLen:  plaintextLen,
		segIndex:      -1,
	}, nil
}

// segmentForPlaintextOffset returns the index of the segment holding the
// given plaintext offset.
func (s *seekableDecryptingReader) segmentForPlaintextOffset(off int64) int64 {
	pss := s.css - tinkTagSize
	firstPss := pss - int64(s.tinkHeaderLen)
	if off < firstPss {
		return 0
	}
	return 1 + (off-firstPss)/pss
}

// plaintextStartOfSegment returns the plaintext offset where segment j begins.
func (s *seekableDecryptingReader) plaintextStartOfSegment(j int64) int64 {
	if j == 0 {
		return 0
	}
	pss := s.css - tinkTagSize
	firstPss := pss - int64(s.tinkHeaderLen)
	return firstPss + (j-1)*pss
}

func (s *seekableDecryptingReader) loadSegment(j int64) error {
	// Ciphertext offset (relative to base) and length of segment j.
	var ctOff int64
	if j == 0 {
		ctOff = int64(s.tinkHeaderLen)
	} else {
		ctOff = j * s.css
	}
	ctLen := min(s.css, s.ciphertextLen-ctOff)
	if j == 0 {
		ctLen = min(s.css-int64(s.tinkHeaderLen), s.ciphertextLen-ctOff)
	}
	if ctLen < tinkTagSize {
		return errors.New("ciphertext segment shorter than tag")
	}

	if _, err := s.r.Seek(s.base+ctOff, io.SeekStart); err != nil {
		return err
	}
	if int64(cap(s.segBuf)) < ctLen {
		s.segBuf = make([]byte, ctLen)
	}
	segment := s.segBuf[:ctLen]
	if _, err := io.ReadFull(s.r, segment); err != nil {
		return err
	}

	nonce := make([]byte, tinkNonceSize)
	copy(nonce, s.noncePrefix)
	if j > int64(^uint32(0)) {
		return errors.New("segment index exceeds nonce counter range")
	}
	binary.BigEndian.PutUint32(nonce[tinkNoncePrefixSize:], uint32(j))
	if j == s.numSegments-1 {
		nonce[tinkNoncePrefixSize+4] = 1
	}

	plaintext, err := s.cipher.Open(s.plaintext[:0], nonce, segment, nil)
	if err != nil {
		return fmt.Errorf("segment %d decryption failed: %w", j, err)
	}
	s.plaintext = plaintext
	s.segIndex = j
	s.segStart = s.plaintextStartOfSegment(j)
	return nil
}

func (s *seekableDecryptingReader) Read(p []byte) (int, error) {
	if s.pos >= s.plaintextLen {
		return 0, io.EOF
	}
	j := s.segmentForPlaintextOffset(s.pos)
	if j != s.segIndex {
		if err := s.loadSegment(j); err != nil {
			return 0, err
		}
	}
	n := copy(p, s.plaintext[s.pos-s.segStart:])
	s.pos += int64(n)
	return n, nil
}

func (s *seekableDecryptingReader) Seek(offset int64, whence int) (int64, error) {
	var abs int64
	switch whence {
	case io.SeekStart:
		abs = offset
	case io.SeekCurrent:
		abs = s.pos + offset
	case io.SeekEnd:
		abs = s.plaintextLen + offset
	default:
		return 0, errors.New("invalid whence")
	}
	if abs < 0 {
		return 0, errors.New("negative position")
	}
	s.pos = abs
	return abs, nil
}
