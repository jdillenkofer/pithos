package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/klauspost/compress/zstd"
)

const (
	headerVersion     = byte(1)
	headerSize        = 32
	headerPrefixSize  = 24
	headerChecksumPos = 24
	headerFlagsPos    = 18
	defaultSampleSize = 64 * 1024
	defaultAlgorithm  = AlgorithmZstd
	defaultMaxRatio   = 0.95
	minCompressSize   = 1024
)

var (
	headerMagic = [16]byte{0x4d, 0x2b, 0x0a, 0xdc, 0xee, 0x7c, 0x44, 0xa8, 0xb0, 0x49, 0x98, 0x06, 0x7b, 0x5b, 0x84, 0x50}
	crc64Table  = crc64.MakeTable(crc64.ECMA)
)

type Algorithm string

const (
	AlgorithmNone Algorithm = "none"
	AlgorithmGzip Algorithm = "gzip"
	AlgorithmZstd Algorithm = "zstd"
)

const (
	algorithmIdNone byte = 0
	algorithmIdGzip byte = 1
	algorithmIdZstd byte = 2
)

type Config struct {
	SampleSize          int
	Algorithm           Algorithm
	MaxCompressionRatio float64
}

type PartStoreMiddleware struct {
	innerPartStore partstore.PartStore
	sampleSize     int
	algorithm      Algorithm
	minSize        int
	maxRatio       float64
}

func New(innerPartStore partstore.PartStore) (partstore.PartStore, error) {
	return NewWithConfig(innerPartStore, Config{})
}

func NewWithConfig(innerPartStore partstore.PartStore, config Config) (partstore.PartStore, error) {
	if innerPartStore == nil {
		return nil, fmt.Errorf("inner part store is required")
	}

	sampleSize := config.SampleSize
	if sampleSize == 0 {
		sampleSize = defaultSampleSize
	}
	if sampleSize < 0 {
		return nil, fmt.Errorf("sample size must be >= 0")
	}

	algorithm := config.Algorithm
	if algorithm == "" {
		algorithm = defaultAlgorithm
	}
	if algorithm != AlgorithmGzip && algorithm != AlgorithmZstd {
		return nil, fmt.Errorf("unsupported compression algorithm: %s", algorithm)
	}

	maxRatio := config.MaxCompressionRatio
	if maxRatio == 0 {
		maxRatio = defaultMaxRatio
	}
	if maxRatio <= 0 || maxRatio > 1 {
		return nil, fmt.Errorf("max compression ratio must be in range (0, 1]")
	}

	return &PartStoreMiddleware{
		innerPartStore: innerPartStore,
		sampleSize:     sampleSize,
		algorithm:      algorithm,
		minSize:        minCompressSize,
		maxRatio:       maxRatio,
	}, nil
}

func (mw *PartStoreMiddleware) Start(ctx context.Context) error {
	return mw.innerPartStore.Start(ctx)
}

func (mw *PartStoreMiddleware) Stop(ctx context.Context) error {
	return mw.innerPartStore.Stop(ctx)
}

func (mw *PartStoreMiddleware) PutPart(ctx context.Context, tx database.Tx, partId partstore.PartId, reader io.Reader) error {
	sample := make([]byte, mw.sampleSize)
	n, err := io.ReadFull(reader, sample)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}
	sample = sample[:n]

	shouldCompress := len(sample) >= mw.minSize
	if shouldCompress {
		ratio, err := mw.estimateSampleCompressionRatio(sample)
		if err != nil {
			return err
		}
		shouldCompress = ratio <= mw.maxRatio
	}
	bodyReader := io.MultiReader(bytes.NewReader(sample), reader)

	pipeReader, pipeWriter := io.Pipe()
	go func() {
		defer pipeWriter.Close()

		algorithm := AlgorithmNone
		if shouldCompress {
			algorithm = mw.algorithm
		}

		header := newHeader(algorithm)
		if _, err := pipeWriter.Write(header[:]); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}

		if shouldCompress {
			compressWriter, err := mw.newCompressionWriter(mw.algorithm, pipeWriter)
			if err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
			if _, err := ioutils.Copy(compressWriter, bodyReader); err != nil {
				_ = compressWriter.Close()
				pipeWriter.CloseWithError(err)
				return
			}
			if err := compressWriter.Close(); err != nil {
				pipeWriter.CloseWithError(err)
				return
			}
			return
		}

		if _, err := ioutils.Copy(pipeWriter, bodyReader); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}
	}()

	return mw.innerPartStore.PutPart(ctx, tx, partId, pipeReader)
}

func (mw *PartStoreMiddleware) estimateSampleCompressionRatio(sample []byte) (float64, error) {
	if len(sample) == 0 {
		return 1, nil
	}
	var compressed bytes.Buffer
	compressWriter, err := mw.newCompressionWriter(mw.algorithm, &compressed)
	if err != nil {
		return 0, err
	}
	if _, err := compressWriter.Write(sample); err != nil {
		compressWriter.Close()
		return 0, err
	}
	if err := compressWriter.Close(); err != nil {
		return 0, err
	}

	return float64(compressed.Len()) / float64(len(sample)), nil
}

func (mw *PartStoreMiddleware) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	rc, err := mw.innerPartStore.GetPart(ctx, tx, partId)
	if err != nil {
		return nil, err
	}

	header := make([]byte, headerSize)
	n, err := io.ReadFull(rc, header)
	if err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			rc.Close()
			return nil, err
		}
		return ioutils.NewReadCloserWithCloseHook(io.NopCloser(io.MultiReader(bytes.NewReader(header[:n]), rc)), rc.Close), nil
	}

	algorithm, ok, err := parseHeader(header)
	if err != nil {
		rc.Close()
		return nil, err
	}
	if !ok {
		return ioutils.NewReadCloserWithCloseHook(io.NopCloser(io.MultiReader(bytes.NewReader(header), rc)), rc.Close), nil
	}

	if algorithm == AlgorithmNone {
		return rc, nil
	}

	decompressReader, err := mw.newDecompressionReader(algorithm, rc)
	if err != nil {
		rc.Close()
		return nil, err
	}

	return ioutils.NewReadCloserWithCloseHook(decompressReader, rc.Close), nil
}

func (mw *PartStoreMiddleware) newCompressionWriter(algorithm Algorithm, w io.Writer) (io.WriteCloser, error) {
	switch algorithm {
	case AlgorithmGzip:
		return gzip.NewWriter(w), nil
	case AlgorithmZstd:
		return zstd.NewWriter(w)
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %s", algorithm)
	}
}

func (mw *PartStoreMiddleware) newDecompressionReader(algorithm Algorithm, r io.Reader) (io.ReadCloser, error) {
	switch algorithm {
	case AlgorithmGzip:
		return gzip.NewReader(r)
	case AlgorithmZstd:
		decoder, err := zstd.NewReader(r)
		if err != nil {
			return nil, err
		}
		return &zstdReadCloser{decoder: decoder}, nil
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %s", algorithm)
	}
}

type zstdReadCloser struct {
	decoder *zstd.Decoder
}

func (z *zstdReadCloser) Read(p []byte) (int, error) {
	return z.decoder.Read(p)
}

func (z *zstdReadCloser) Close() error {
	z.decoder.Close()
	return nil
}

func (mw *PartStoreMiddleware) GetPartIds(ctx context.Context, tx database.Tx) ([]partstore.PartId, error) {
	return mw.innerPartStore.GetPartIds(ctx, tx)
}

func (mw *PartStoreMiddleware) DeletePart(ctx context.Context, tx database.Tx, partId partstore.PartId) error {
	return mw.innerPartStore.DeletePart(ctx, tx, partId)
}

func algorithmToId(algorithm Algorithm) byte {
	switch algorithm {
	case AlgorithmNone:
		return algorithmIdNone
	case AlgorithmGzip:
		return algorithmIdGzip
	case AlgorithmZstd:
		return algorithmIdZstd
	default:
		return 0
	}
}

func algorithmFromId(id byte) (Algorithm, error) {
	switch id {
	case algorithmIdNone:
		return AlgorithmNone, nil
	case algorithmIdGzip:
		return AlgorithmGzip, nil
	case algorithmIdZstd:
		return AlgorithmZstd, nil
	default:
		return "", fmt.Errorf("unsupported compression algorithm id: %d", id)
	}
}

func newHeader(algorithm Algorithm) [headerSize]byte {
	var header [headerSize]byte
	copy(header[:16], headerMagic[:])
	header[16] = headerVersion
	header[17] = algorithmToId(algorithm)
	header[headerFlagsPos] = 0

	checksum := crc64.Checksum(header[:headerPrefixSize], crc64Table)
	binary.BigEndian.PutUint64(header[headerChecksumPos:], checksum)

	return header
}

func parseHeader(header []byte) (Algorithm, bool, error) {
	if len(header) != headerSize {
		return "", false, fmt.Errorf("invalid compression header size: %d", len(header))
	}
	if !bytes.Equal(header[:16], headerMagic[:]) {
		return "", false, nil
	}
	if header[16] != headerVersion {
		return "", false, nil
	}
	if header[headerFlagsPos] != 0 {
		return "", false, nil
	}
	for i := 19; i < headerPrefixSize; i++ {
		if header[i] != 0 {
			return "", false, nil
		}
	}

	expectedChecksum := crc64.Checksum(header[:headerPrefixSize], crc64Table)
	actualChecksum := binary.BigEndian.Uint64(header[headerChecksumPos:])
	if expectedChecksum != actualChecksum {
		return "", false, nil
	}

	algorithm, err := algorithmFromId(header[17])
	if err != nil {
		return "", false, nil
	}
	return algorithm, true, nil
}
