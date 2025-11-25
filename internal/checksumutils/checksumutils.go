package checksumutils

import (
	"context"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"hash/crc32"
	"hash/crc64"
	"io"
	"math"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"go.opentelemetry.io/otel"
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

type channelReader struct {
	ch  <-chan []byte
	buf []byte
	pos int
}

func (r *channelReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.buf) {
		var ok bool
		r.buf, ok = <-r.ch
		if !ok {
			return 0, io.EOF
		}
		r.pos = 0
	}
	n = copy(p, r.buf[r.pos:])
	r.pos += n
	return n, nil
}

func CalculateChecksumsStreaming(ctx context.Context, reader io.Reader, doRead func(reader io.Reader) error) (*int64, *ChecksumValues, error) {
	tracer := otel.Tracer("internal/checksumutils")
	_, span := tracer.Start(ctx, "CalculateChecksumsStreaming")
	defer span.End()

	const chunkSize = 5 * 1024 * 1024 // 5MB
	dataChan := make(chan []byte, 3)
	var consumerChans [7]chan []byte
	for i := range consumerChans {
		consumerChans[i] = make(chan []byte, 3)
	}

	// Broadcaster goroutine
	go func() {
		for chunk := range dataChan {
			for _, ch := range consumerChans {
				ch <- chunk
			}
		}
		for _, ch := range consumerChans {
			close(ch)
		}
	}()

	sizeChan := make(chan int64, 1)
	errChan := make(chan error, 8) // Buffer for all possible error sources

	// Reader goroutine
	go func() {
		defer close(dataChan)
		buf := make([]byte, chunkSize)
		var total int64
		for {
			n, err := reader.Read(buf)
			if n > 0 {
				chunk := make([]byte, n)
				copy(chunk, buf[:n])
				dataChan <- chunk
				total += int64(n)
			}
			if err != nil {
				if err == io.EOF {
					sizeChan <- total
					return
				}
				errChan <- err
				return
			}
		}
	}()

	// doRead goroutine
	go func() {
		r := &channelReader{ch: consumerChans[0]}
		if err := doRead(r); err != nil {
			errChan <- err
			return
		}
		errChan <- nil // Signal success
	}()

	// Checksum goroutines
	etagChan := make(chan string, 1)
	go func() {
		hash := md5.New()
		r := &channelReader{ch: consumerChans[1]}
		ioutils.Copy(hash, r)
		sum := hash.Sum(nil)
		hexSum := hex.EncodeToString(sum)
		etag := "\"" + hexSum + "\""
		etagChan <- etag
	}()

	crc32Chan := make(chan string, 1)
	go func() {
		hash := crc32.NewIEEE()
		r := &channelReader{ch: consumerChans[2]}
		ioutils.Copy(hash, r)
		sum := hash.Sum(nil)
		base64Sum := base64.StdEncoding.EncodeToString(sum)
		crc32Chan <- base64Sum
	}()

	crc32cChan := make(chan string, 1)
	go func() {
		hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
		r := &channelReader{ch: consumerChans[3]}
		ioutils.Copy(hash, r)
		sum := hash.Sum(nil)
		base64Sum := base64.StdEncoding.EncodeToString(sum)
		crc32cChan <- base64Sum
	}()

	crc64nvmeChan := make(chan string, 1)
	go func() {
		hash := crc64.New(crc64.MakeTable(0x9a6c9329ac4bc9b5))
		r := &channelReader{ch: consumerChans[4]}
		ioutils.Copy(hash, r)
		sum := hash.Sum(nil)
		base64Sum := base64.StdEncoding.EncodeToString(sum)
		crc64nvmeChan <- base64Sum
	}()

	sha1Chan := make(chan string, 1)
	go func() {
		hash := sha1.New()
		r := &channelReader{ch: consumerChans[5]}
		ioutils.Copy(hash, r)
		sum := hash.Sum(nil)
		base64Sum := base64.StdEncoding.EncodeToString(sum)
		sha1Chan <- base64Sum
	}()

	sha256Chan := make(chan string, 1)
	go func() {
		hash := sha256.New()
		r := &channelReader{ch: consumerChans[6]}
		ioutils.Copy(hash, r)
		sum := hash.Sum(nil)
		base64Sum := base64.StdEncoding.EncodeToString(sum)
		sha256Chan <- base64Sum
	}()

	// Collect results
	// First, wait for either size or error from reader
	var n int64
	select {
	case n = <-sizeChan:
		// Reader completed successfully, continue
	case err := <-errChan:
		// Reader failed
		return nil, nil, err
	}

	// Now wait for doRead to complete
	if err := <-errChan; err != nil {
		return nil, nil, err
	}

	// All goroutines succeeded, collect checksums
	etag := <-etagChan
	checksumCRC32 := <-crc32Chan
	checksumCRC32C := <-crc32cChan
	checksumCRC64NVME := <-crc64nvmeChan
	checksumSHA1 := <-sha1Chan
	checksumSHA256 := <-sha256Chan
	checksums := &ChecksumValues{
		ETag:              &etag,
		ChecksumCRC32:     &checksumCRC32,
		ChecksumCRC32C:    &checksumCRC32C,
		ChecksumCRC64NVME: &checksumCRC64NVME,
		ChecksumSHA1:      &checksumSHA1,
		ChecksumSHA256:    &checksumSHA256,
	}
	return &n, checksums, nil
}
