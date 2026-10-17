package compression

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"strconv"
	"sync"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	pithosmetrics "github.com/jdillenkofer/pithos/internal/metrics"
	"github.com/jdillenkofer/pithos/internal/storage/database"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
)

var compressionMetricsOnce sync.Once
var compressionMetrics struct {
	parts             *prometheus.CounterVec
	bytesIn, bytesOut prometheus.Counter
	ratio             prometheus.Histogram
}

func registerCompressionMetrics() {
	compressionMetricsOnce.Do(func() {
		compressionMetrics.parts = prometheus.NewCounterVec(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "compression", Name: "parts_total", Help: "Number of parts processed by compression"}, []string{"compressed"})
		compressionMetrics.bytesIn = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "compression", Name: "bytes_in_total", Help: "Bytes supplied to compression"})
		compressionMetrics.bytesOut = prometheus.NewCounter(prometheus.CounterOpts{Namespace: "pithos", Subsystem: "compression", Name: "bytes_out_total", Help: "Bytes emitted by compression including headers"})
		compressionMetrics.ratio = prometheus.NewHistogram(prometheus.HistogramOpts{Namespace: "pithos", Subsystem: "compression", Name: "ratio", Help: "Ratio of encoded bytes to input bytes"})
	})
	pithosmetrics.Register(compressionMetrics.parts, compressionMetrics.bytesIn, compressionMetrics.bytesOut, compressionMetrics.ratio)
}

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

func (mw *PartStoreMiddleware) SupportsStats() bool {
	provider, ok := mw.innerPartStore.(partstore.StatsProvider)
	return ok && provider.SupportsStats()
}

func (mw *PartStoreMiddleware) Stats(ctx context.Context, tx database.Tx) (partstore.Stats, error) {
	stats, _, err := partstore.StatsOf(ctx, tx, mw.innerPartStore)
	return stats, err
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

	registerCompressionMetrics()
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
	sampleBuf := sampleBufferPool.Get().(*[]byte)
	defer sampleBufferPool.Put(sampleBuf)
	if cap(*sampleBuf) < mw.sampleSize {
		*sampleBuf = make([]byte, mw.sampleSize)
	}
	sample := (*sampleBuf)[:mw.sampleSize]
	n, err := io.ReadFull(reader, sample)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}
	sample = sample[:n]

	var bytesIn int64
	var bytesOut int64
	var compressed *compressedSample
	if len(sample) >= mw.minSize {
		compressed, err = mw.compressSample(sample)
		if err != nil {
			return err
		}
		if compressed.ratio() > mw.maxRatio {
			compressed.release()
			compressed = nil
		}
	}

	if compressed == nil {
		header := newHeader(AlgorithmNone)
		bodyReader := ioutils.NewCountingReader(io.MultiReader(bytes.NewReader(sample), reader), &bytesIn)
		err = mw.innerPartStore.PutPart(ctx, tx, partId, ioutils.NewCountingReader(io.MultiReader(bytes.NewReader(header[:]), bodyReader), &bytesOut))
		mw.recordPut(false, bytesIn, bytesOut)
		return err
	}

	bytesIn = int64(len(sample))
	restReader := ioutils.NewCountingReader(reader, &bytesIn)
	pipeReader, pipeWriter := io.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer compressed.release()

		header := newHeader(mw.algorithm)
		if _, err := pipeWriter.Write(header[:]); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}
		if _, err := pipeWriter.Write(compressed.out.Bytes()); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}
		compressed.target.w = pipeWriter
		if _, err := ioutils.Copy(compressed.enc, restReader); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}
		if err := compressed.enc.Close(); err != nil {
			pipeWriter.CloseWithError(err)
			return
		}
		pipeWriter.Close()
	}()

	err = mw.innerPartStore.PutPart(ctx, tx, partId, ioutils.NewCountingReader(pipeReader, &bytesOut))
	// Unblock the compression goroutine if the inner store stopped reading
	// early, and wait so it no longer touches reader once we return.
	pipeReader.CloseWithError(io.ErrClosedPipe)
	<-done
	mw.recordPut(true, bytesIn, bytesOut)
	return err
}

func (mw *PartStoreMiddleware) recordPut(compressed bool, bytesIn, bytesOut int64) {
	compressionMetrics.parts.WithLabelValues(strconv.FormatBool(compressed)).Inc()
	compressionMetrics.bytesIn.Add(float64(bytesIn))
	compressionMetrics.bytesOut.Add(float64(bytesOut))
	if bytesIn > 0 {
		compressionMetrics.ratio.Observe(float64(bytesOut) / float64(bytesIn))
	}
}

// compressedSample holds an encoder that has consumed the sample and flushed
// its output into out. Once target.w is set, the encoder writes there
// instead, so the sample is not compressed a second time.
type compressedSample struct {
	pool       *sync.Pool
	enc        compressor
	target     *switchWriter
	out        bytes.Buffer
	sampleSize int
}

func (c *compressedSample) ratio() float64 {
	return float64(c.out.Len()) / float64(c.sampleSize)
}

func (c *compressedSample) release() {
	c.enc.Reset(nil)
	c.pool.Put(c.enc)
}

type switchWriter struct {
	w io.Writer
}

func (s *switchWriter) Write(p []byte) (int, error) {
	return s.w.Write(p)
}

func (mw *PartStoreMiddleware) compressSample(sample []byte) (*compressedSample, error) {
	pool, err := encoderPoolFor(mw.algorithm)
	if err != nil {
		return nil, err
	}
	c := &compressedSample{pool: pool, enc: pool.Get().(compressor), sampleSize: len(sample)}
	c.target = &switchWriter{w: &c.out}
	c.enc.Reset(c.target)
	if _, err := c.enc.Write(sample); err != nil {
		c.release()
		return nil, err
	}
	if err := c.enc.Flush(); err != nil {
		c.release()
		return nil, err
	}
	return c, nil
}

func (mw *PartStoreMiddleware) estimateSampleCompressionRatio(sample []byte) (float64, error) {
	if len(sample) == 0 {
		return 1, nil
	}
	c, err := mw.compressSample(sample)
	if err != nil {
		return 0, err
	}
	defer c.release()
	return c.ratio(), nil
}

func (mw *PartStoreMiddleware) Capabilities() partstore.Capabilities {
	return partstore.CapabilitiesOf(mw.innerPartStore)
}

func (mw *PartStoreMiddleware) GetPart(ctx context.Context, tx database.Tx, partId partstore.PartId) (io.ReadCloser, error) {
	rc, err := mw.innerPartStore.GetPart(ctx, tx, partId)
	if err != nil {
		return nil, err
	}

	var header [headerSize]byte
	n, err := io.ReadFull(rc, header[:])
	if err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			rc.Close()
			return nil, err
		}
		return ioutils.NewReadCloserWithCloseHook(io.NopCloser(io.MultiReader(bytes.NewReader(header[:n]), rc)), rc.Close), nil
	}

	algorithm, ok, err := parseHeader(header[:])
	if err != nil {
		rc.Close()
		return nil, err
	}
	if !ok {
		return ioutils.NewReadCloserWithCloseHook(io.NopCloser(io.MultiReader(bytes.NewReader(header[:]), rc)), rc.Close), nil
	}

	if algorithm == AlgorithmNone {
		return rc, nil
	}

	decompressReader, err := newDecompressionReader(algorithm, rc)
	if err != nil {
		rc.Close()
		return nil, err
	}

	return ioutils.NewReadCloserWithCloseHook(decompressReader, rc.Close), nil
}

// compressor is the subset of *gzip.Writer and *zstd.Encoder used here.
type compressor interface {
	io.WriteCloser
	Flush() error
	Reset(w io.Writer)
}

var (
	sampleBufferPool = sync.Pool{New: func() any { b := make([]byte, defaultSampleSize); return &b }}
	gzipEncoderPool  = sync.Pool{New: func() any { return gzip.NewWriter(nil) }}
	zstdEncoderPool  = sync.Pool{New: func() any {
		enc, err := zstd.NewWriter(nil)
		if err != nil {
			panic(err)
		}
		return enc
	}}
	gzipDecoderPool sync.Pool
	zstdDecoderPool = sync.Pool{New: func() any {
		dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
		if err != nil {
			panic(err)
		}
		return dec
	}}
)

func encoderPoolFor(algorithm Algorithm) (*sync.Pool, error) {
	switch algorithm {
	case AlgorithmGzip:
		return &gzipEncoderPool, nil
	case AlgorithmZstd:
		return &zstdEncoderPool, nil
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %s", algorithm)
	}
}

func newDecompressionReader(algorithm Algorithm, r io.Reader) (io.ReadCloser, error) {
	switch algorithm {
	case AlgorithmGzip:
		dec, _ := gzipDecoderPool.Get().(*gzip.Reader)
		if dec == nil {
			dec = new(gzip.Reader)
		}
		if err := dec.Reset(r); err != nil {
			gzipDecoderPool.Put(dec)
			return nil, err
		}
		return &gzipReadCloser{decoder: dec}, nil
	case AlgorithmZstd:
		dec := zstdDecoderPool.Get().(*zstd.Decoder)
		if err := dec.Reset(r); err != nil {
			zstdDecoderPool.Put(dec)
			return nil, err
		}
		return &zstdReadCloser{decoder: dec}, nil
	default:
		return nil, fmt.Errorf("unsupported compression algorithm: %s", algorithm)
	}
}

type gzipReadCloser struct {
	decoder *gzip.Reader
}

func (g *gzipReadCloser) Read(p []byte) (int, error) {
	if g.decoder == nil {
		return 0, io.ErrClosedPipe
	}
	return g.decoder.Read(p)
}

func (g *gzipReadCloser) Close() error {
	if g.decoder == nil {
		return nil
	}
	err := g.decoder.Close()
	gzipDecoderPool.Put(g.decoder)
	g.decoder = nil
	return err
}

type zstdReadCloser struct {
	decoder *zstd.Decoder
}

func (z *zstdReadCloser) Read(p []byte) (int, error) {
	if z.decoder == nil {
		return 0, io.ErrClosedPipe
	}
	return z.decoder.Read(p)
}

func (z *zstdReadCloser) Close() error {
	if z.decoder == nil {
		return nil
	}
	_ = z.decoder.Reset(nil)
	zstdDecoderPool.Put(z.decoder)
	z.decoder = nil
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
