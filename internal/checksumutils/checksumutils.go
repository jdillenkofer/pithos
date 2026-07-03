package checksumutils

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math"
	"sync"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"go.opentelemetry.io/otel"
)

// crcNvmePolynomial is the CRC-64/NVME polynomial (reversed representation).
const crcNvmePolynomial = 0x9a6c9329ac4bc9b5

// CRC tables are built once at package init and reused across calls. The
// stdlib caches its IEEE/Castagnoli CRC32 tables internally, but an arbitrary
// CRC64 polynomial like NVME is not special-cased, so crc64.MakeTable would
// otherwise allocate and recompute a 256-entry table on every invocation.
var (
	crc32CastagnoliTable = crc32.MakeTable(crc32.Castagnoli)
	crc64NvmeTable       = crc64.MakeTable(crcNvmePolynomial)
)

// This was ported over from localstack and allows you to efficiently combine crc checksums
// https://github.com/localstack/localstack/blob/ea0a194102807b59c44e74dc355ef1dd07981ed8/localstack-core/localstack/services/s3/utils.py#L256

func gf2_matrix_square(square *[]uint64, mat *[]uint64) {
	for n := range len(*mat) {
		(*square)[n] = gf2_matrix_times(*mat, (*mat)[n])
	}
}

func gf2_matrix_times(mat []uint64, vec uint64) uint64 {
	var summary uint64 = 0
	mat_index := 0

	for vec != 0 {
		if vec&1 != 0 {
			summary ^= mat[mat_index]
		}

		vec >>= 1
		mat_index += 1
	}

	return summary
}

func combine(poly uint64, sizeBits uint64, init_crc uint64, xorOut uint64, crc1 uint64, crc2 uint64, len2 uint64) []byte {
	if len2 == 0 {
		return encode_to_bytes(crc1, sizeBits)
	}

	var even []uint64 = make([]uint64, sizeBits)
	var odd []uint64 = make([]uint64, sizeBits)

	crc1 ^= init_crc ^ xorOut

	odd[0] = poly
	var row uint64 = 1
	for n := 1; n < int(sizeBits); n++ {
		odd[n] = row
		row <<= 1
	}

	gf2_matrix_square(&even, &odd)

	gf2_matrix_square(&odd, &even)

	for {
		gf2_matrix_square(&even, &odd)
		if len2&1 != 0 {
			crc1 = gf2_matrix_times(even, crc1)
		}
		len2 >>= 1
		if len2 == 0 {
			break
		}

		gf2_matrix_square(&odd, &even)
		if len2&1 != 0 {
			crc1 = gf2_matrix_times(odd, crc1)
		}
		len2 >>= 1

		if len2 == 0 {
			break
		}
	}

	crc1 ^= crc2

	return encode_to_bytes(crc1, sizeBits)
}

func encode_to_bytes(crc uint64, sizeBits uint64) []byte {
	if sizeBits == 64 {
		bytes := []byte{0, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint64(bytes, crc)
		return bytes
	}
	if sizeBits == 32 {
		bytes := []byte{0, 0, 0, 0}
		binary.BigEndian.PutUint32(bytes, uint32(crc))
		return bytes
	}
	panic("sizeBits must be 32 or 64")
}

func bitrev(x uint64, n uint64) uint64 {
	x = uint64(x)
	var y uint64 = 0
	for range n {
		y = (y << 1) | (x & 1)
		x = x >> 1
	}
	if (uint64(1)<<n)-uint64(1) <= math.MaxInt64 {
		return uint64(uint(y))
	}
	return y
}

func createCombineFunction(poly uint64, sizeBits uint64, xorOut uint64) func(a []byte, b []byte, bLen int64) []byte {
	var initCrc uint64 = 0
	mask := (uint64(1) << sizeBits) - uint64(1)
	poly = bitrev(poly&mask, sizeBits)

	combineFunc := func(crc1 []byte, crc2 []byte, len2 int64) []byte {
		var crc1Uint64 uint64
		if len(crc1) == 4 {
			crc1Uint64 = uint64(binary.BigEndian.Uint32(crc1))
		} else {
			crc1Uint64 = binary.BigEndian.Uint64(crc1)
		}
		var crc2Uint64 uint64
		if len(crc2) == 4 {
			crc2Uint64 = uint64(binary.BigEndian.Uint32(crc2))
		} else {
			crc2Uint64 = binary.BigEndian.Uint64(crc2)
		}
		return combine(poly, sizeBits, initCrc^xorOut, xorOut, crc1Uint64, crc2Uint64, uint64(len2))
	}

	return combineFunc
}

func CombineCrc32(a []byte, b []byte, bLen int64) []byte {
	return createCombineFunction(0x104C11DB7, 32, 0xFFFFFFFF)(a, b, bLen)
}

func CombineCrc32c(a []byte, b []byte, bLen int64) []byte {
	return createCombineFunction(0x1EDC6F41, 32, 0xFFFFFFFF)(a, b, bLen)
}

func CombineCrc64Nvme(a []byte, b []byte, bLen int64) []byte {
	return createCombineFunction(0xAD93D23594C93659, 64, 0xFFFFFFFFFFFFFFFF)(a, b, bLen)
}

type ChecksumValues struct {
	ETag              *string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
}

// hashBlockSize is the block granularity at which buffered data is handed to
// the hash workers. Large enough that per-block synchronization (one channel
// send per hash plus a WaitGroup) is negligible against hashing time.
const hashBlockSize = 256 * 1024

var hashBlockPool = sync.Pool{
	New: func() any { return make([]byte, hashBlockSize) },
}

// parallelHashWriter feeds all hashes concurrently, one persistent goroutine
// per hash. Feeding the hashes serially via io.MultiWriter caps the whole
// upload path at the *sum* of the hash costs; running them in parallel caps it
// at the slowest single hash instead.
//
// Writes are accumulated into fixed-size blocks so the win is independent of
// the caller's read chunk size. Blocks are dispatched asynchronously with two
// buffers ping-ponging between filling and hashing, so hashing also overlaps
// with the caller's own I/O between writes. Flush must be called before
// reading any hash sums; Close must be called to release the goroutines.
type hashBlock struct {
	data []byte
	wg   *sync.WaitGroup
}

type parallelHashWriter struct {
	inputs []chan hashBlock
	bufs   [2][]byte
	wgs    [2]sync.WaitGroup
	fill   int // bytes filled in the active buffer
	active int // index of the buffer currently being filled
	closed bool
}

func newParallelHashWriter(hashes ...hash.Hash) *parallelHashWriter {
	phw := &parallelHashWriter{
		inputs: make([]chan hashBlock, len(hashes)),
	}
	phw.bufs[0] = hashBlockPool.Get().([]byte)
	phw.bufs[1] = hashBlockPool.Get().([]byte)
	for i, h := range hashes {
		// Capacity 1 lets the dispatcher queue the next block while a worker
		// is still hashing the previous one.
		in := make(chan hashBlock, 1)
		phw.inputs[i] = in
		go func(h hash.Hash, in <-chan hashBlock) {
			for blk := range in {
				// hash.Hash.Write never returns an error.
				_, _ = h.Write(blk.data)
				blk.wg.Done()
			}
		}(h, in)
	}
	return phw
}

func (phw *parallelHashWriter) Write(p []byte) (int, error) {
	total := len(p)
	for len(p) > 0 {
		n := copy(phw.bufs[phw.active][phw.fill:], p)
		phw.fill += n
		p = p[n:]
		if phw.fill == hashBlockSize {
			phw.dispatchActive()
		}
	}
	return total, nil
}

// dispatchActive hands the filled portion of the active buffer to every hash
// worker without waiting for them, then swaps to the other buffer. It only
// blocks until the other buffer's previous block has been fully consumed, so
// it can be safely refilled.
func (phw *parallelHashWriter) dispatchActive() {
	if phw.fill == 0 {
		return
	}
	block := phw.bufs[phw.active][:phw.fill]
	wg := &phw.wgs[phw.active]
	wg.Add(len(phw.inputs))
	for _, in := range phw.inputs {
		in <- hashBlock{data: block, wg: wg}
	}
	phw.active = 1 - phw.active
	phw.fill = 0
	phw.wgs[phw.active].Wait()
}

// Flush dispatches any buffered tail and waits until every hash has consumed
// all data written so far.
func (phw *parallelHashWriter) Flush() {
	phw.dispatchActive()
	phw.wgs[0].Wait()
	phw.wgs[1].Wait()
}

// Close releases the worker goroutines and returns the block buffers to the
// pool. The buffers may only be recycled once the workers are guaranteed to be
// done with them, so Close waits for all outstanding blocks first.
func (phw *parallelHashWriter) Close() {
	if phw.closed {
		return
	}
	phw.closed = true
	phw.wgs[0].Wait()
	phw.wgs[1].Wait()
	for _, in := range phw.inputs {
		close(in)
	}
	hashBlockPool.Put(phw.bufs[0])
	hashBlockPool.Put(phw.bufs[1])
	phw.bufs[0] = nil
	phw.bufs[1] = nil
}

func CalculateChecksumsStreaming(ctx context.Context, reader io.Reader, doRead func(reader io.Reader) error) (*int64, *ChecksumValues, error) {
	tracer := otel.Tracer("internal/checksumutils")
	_, span := tracer.Start(ctx, "CalculateChecksumsStreaming")
	defer span.End()

	etagHash := md5.New()
	crc32Hash := crc32.NewIEEE()
	crc32cHash := crc32.New(crc32CastagnoliTable)
	crc64nvmeHash := crc64.New(crc64NvmeTable)
	sha1Hash := sha1.New()
	sha256Hash := sha256.New()

	hashes := []hash.Hash{etagHash, crc32Hash, crc32cHash, crc64nvmeHash, sha1Hash, sha256Hash}
	parallelWriter := newParallelHashWriter(hashes...)
	defer parallelWriter.Close()
	teeReader := io.TeeReader(reader, parallelWriter)

	// Track bytes read
	var bytesRead int64
	countingReader := ioutils.NewCountingReader(teeReader, &bytesRead)

	// Execute doRead with the counting reader
	if err := doRead(countingReader); err != nil {
		return nil, nil, err
	}

	// Make sure every hash has consumed all buffered data before summing.
	parallelWriter.Flush()

	// Compute checksums
	etag := "\"" + hex.EncodeToString(etagHash.Sum(nil)) + "\""
	checksumCRC32 := base64.StdEncoding.EncodeToString(crc32Hash.Sum(nil))
	checksumCRC32C := base64.StdEncoding.EncodeToString(crc32cHash.Sum(nil))
	checksumCRC64NVME := base64.StdEncoding.EncodeToString(crc64nvmeHash.Sum(nil))
	checksumSHA1 := base64.StdEncoding.EncodeToString(sha1Hash.Sum(nil))
	checksumSHA256 := base64.StdEncoding.EncodeToString(sha256Hash.Sum(nil))

	checksums := &ChecksumValues{
		ETag:              &etag,
		ChecksumCRC32:     &checksumCRC32,
		ChecksumCRC32C:    &checksumCRC32C,
		ChecksumCRC64NVME: &checksumCRC64NVME,
		ChecksumSHA1:      &checksumSHA1,
		ChecksumSHA256:    &checksumSHA256,
	}

	return &bytesRead, checksums, nil
}
