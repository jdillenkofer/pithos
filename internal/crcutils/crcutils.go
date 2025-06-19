package crcutils

import (
	"encoding/binary"
	"math"
)

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
