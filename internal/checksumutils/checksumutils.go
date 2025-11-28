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

func CalculateChecksumsStreaming(ctx context.Context, reader io.Reader, doRead func(reader io.Reader) error) (*int64, *ChecksumValues, error) {
	tracer := otel.Tracer("internal/checksumutils")
	_, span := tracer.Start(ctx, "CalculateChecksumsStreaming")
	defer span.End()

	// Create a TeeReader that writes to all hash functions simultaneously
	etagHash := md5.New()
	crc32Hash := crc32.NewIEEE()
	crc32cHash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	crc64nvmeHash := crc64.New(crc64.MakeTable(0x9a6c9329ac4bc9b5))
	sha1Hash := sha1.New()
	sha256Hash := sha256.New()

	multiWriter := io.MultiWriter(etagHash, crc32Hash, crc32cHash, crc64nvmeHash, sha1Hash, sha256Hash)
	teeReader := io.TeeReader(reader, multiWriter)

	// Track bytes read
	var bytesRead int64
	countingReader := ioutils.NewCountingReader(teeReader, &bytesRead)

	// Execute doRead with the counting reader
	if err := doRead(countingReader); err != nil {
		return nil, nil, err
	}

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
