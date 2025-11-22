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

	"go.opentelemetry.io/otel"

	"github.com/jdillenkofer/pithos/internal/ioutils"
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

func calculateETag(ctx context.Context, reader io.Reader) (*string, error) {
	tracer := otel.Tracer("internal/checksumutils")
	_, span := tracer.Start(ctx, "calculateETag")
	defer span.End()

	hash := md5.New()
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	hexSum := hex.EncodeToString(sum)
	etag := "\"" + hexSum + "\""
	return &etag, nil
}

func calculateCrc32(ctx context.Context, reader io.Reader) (*string, error) {
	tracer := otel.Tracer("internal/checksumutils")
	_, span := tracer.Start(ctx, "calculateCrc32")
	defer span.End()

	hash := crc32.NewIEEE()
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	base64Sum := base64.StdEncoding.EncodeToString(sum)
	return &base64Sum, nil
}

func calculateCrc32c(ctx context.Context, reader io.Reader) (*string, error) {
	tracer := otel.Tracer("internal/checksumutils")
	_, span := tracer.Start(ctx, "calculateCrc32c")
	defer span.End()

	hash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	base64Sum := base64.StdEncoding.EncodeToString(sum)
	return &base64Sum, nil
}

func calculateCrc64Nvme(ctx context.Context, reader io.Reader) (*string, error) {
	tracer := otel.Tracer("internal/checksumutils")
	_, span := tracer.Start(ctx, "calculateCrc64Nvme")
	defer span.End()

	hash := crc64.New(crc64.MakeTable(0x9a6c9329ac4bc9b5))
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	base64Sum := base64.StdEncoding.EncodeToString(sum)
	return &base64Sum, nil
}

func calculateSha1(ctx context.Context, reader io.Reader) (*string, error) {
	tracer := otel.Tracer("internal/checksumutils")
	_, span := tracer.Start(ctx, "calculateSha1")
	defer span.End()

	hash := sha1.New()
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	base64Sum := base64.StdEncoding.EncodeToString(sum)
	return &base64Sum, nil
}

func calculateSha256(ctx context.Context, reader io.Reader) (*string, error) {
	tracer := otel.Tracer("internal/checksumutils")
	_, span := tracer.Start(ctx, "calculateSha256")
	defer span.End()

	hash := sha256.New()
	_, err := io.Copy(hash, reader)
	if err != nil {
		return nil, err
	}
	sum := hash.Sum([]byte{})
	base64Sum := base64.StdEncoding.EncodeToString(sum)
	return &base64Sum, nil
}

type ChecksumValues struct {
	ETag              *string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
}

func CalculateChecksumsStreaming(ctx context.Context, reader io.Reader, doRead func(reader io.Reader) error) (*int64, *ChecksumValues, error) {
	tracer := otel.Tracer("internal/checksumutils")
	ctx, span := tracer.Start(ctx, "CalculateChecksumsStreaming")
	defer span.End()

	readers, writer, closer := ioutils.PipeWriterIntoMultipleReaders(7)

	doneChan := make(chan struct{}, 1)
	errChan := make(chan error, 1)
	go func() {
		err := doRead(readers[0])
		if err != nil {
			errChan <- err
			return
		}
		doneChan <- struct{}{}
	}()

	etagChan := make(chan string, 1)
	errChan2 := make(chan error, 1)
	go func() {
		etag, err := calculateETag(ctx, readers[1])
		if err != nil {
			errChan2 <- err
			return
		}
		etagChan <- *etag
	}()

	crc32Chan := make(chan string, 1)
	errChan3 := make(chan error, 1)
	go func() {
		crc32, err := calculateCrc32(ctx, readers[2])
		if err != nil {
			errChan3 <- err
			return
		}
		crc32Chan <- *crc32
	}()

	crc32cChan := make(chan string, 1)
	errChan4 := make(chan error, 1)
	go func() {
		crc32c, err := calculateCrc32c(ctx, readers[3])
		if err != nil {
			errChan4 <- err
			return
		}
		crc32cChan <- *crc32c
	}()

	crc64nvmeChan := make(chan string, 1)
	errChan5 := make(chan error, 1)
	go func() {
		crc64nvme, err := calculateCrc64Nvme(ctx, readers[4])
		if err != nil {
			errChan5 <- err
			return
		}
		crc64nvmeChan <- *crc64nvme
	}()

	sha1Chan := make(chan string, 1)
	errChan6 := make(chan error, 1)
	go func() {
		sha1, err := calculateSha1(ctx, readers[5])
		if err != nil {
			errChan6 <- err
			return
		}
		sha1Chan <- *sha1
	}()

	sha256Chan := make(chan string, 1)
	errChan7 := make(chan error, 1)
	go func() {
		sha256, err := calculateSha256(ctx, readers[6])
		if err != nil {
			errChan7 <- err
			return
		}
		sha256Chan <- *sha256
	}()

	// @Note: We need a anonymous function here,
	// because defers are always scoped to the function.
	// But if we don't directly defer close after the copy,
	// we deadlock the program
	originalSize, err := func() (*int64, error) {
		defer closer.Close()
		originalSize, err := io.Copy(writer, reader)
		if err != nil {
			return nil, err
		}
		return &originalSize, nil
	}()
	if err != nil {
		return nil, nil, err
	}

	select {
	case <-doneChan:
	case err := <-errChan:
		if err != nil {
			return nil, nil, err
		}
	}

	var etag string
	select {
	case etag = <-etagChan:
	case err := <-errChan2:
		if err != nil {
			return nil, nil, err
		}
	}

	var checksumCRC32 string
	select {
	case checksumCRC32 = <-crc32Chan:
	case err := <-errChan3:
		if err != nil {
			return nil, nil, err
		}
	}

	var checksumCRC32C string
	select {
	case checksumCRC32C = <-crc32cChan:
	case err := <-errChan4:
		if err != nil {
			return nil, nil, err
		}
	}

	var checksumCRC64NVME string
	select {
	case checksumCRC64NVME = <-crc64nvmeChan:
	case err := <-errChan5:
		if err != nil {
			return nil, nil, err
		}
	}

	var checksumSHA1 string
	select {
	case checksumSHA1 = <-sha1Chan:
	case err := <-errChan6:
		if err != nil {
			return nil, nil, err
		}
	}

	var checksumSHA256 string
	select {
	case checksumSHA256 = <-sha256Chan:
	case err := <-errChan7:
		if err != nil {
			return nil, nil, err
		}
	}

	checksums := &ChecksumValues{
		ETag:              &etag,
		ChecksumCRC32:     &checksumCRC32,
		ChecksumCRC32C:    &checksumCRC32C,
		ChecksumCRC64NVME: &checksumCRC64NVME,
		ChecksumSHA1:      &checksumSHA1,
		ChecksumSHA256:    &checksumSHA256,
	}
	return originalSize, checksums, nil
}
