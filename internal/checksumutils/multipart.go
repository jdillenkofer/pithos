package checksumutils

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"strconv"
)

const (
	ChecksumTypeFullObject = "FULL_OBJECT"
	ChecksumTypeComposite  = "COMPOSITE"
)

type PartChecksums struct {
	ETag              string
	ChecksumCRC32     *string
	ChecksumCRC32C    *string
	ChecksumCRC64NVME *string
	ChecksumSHA1      *string
	ChecksumSHA256    *string
	Size              int64
}

func CalculateMultipartChecksums(parts []PartChecksums, checksumType string) (ChecksumValues, error) {
	etagMd5Hash := md5.New()

	skipCrc32 := false
	skipCrc32c := false
	skipCrc64Nvme := false
	skipSha1 := false
	skipSha256 := false

	// -- The following variables are only used by checksumType Composite
	crc32Hash := crc32.NewIEEE()
	crc32cHash := crc32.New(crc32.MakeTable(crc32.Castagnoli))
	sha1Hash := sha1.New()
	sha256Hash := sha256.New()
	// --

	// -- The following variables are only used by checksumType FullObject
	var crc32Combined *[]byte = nil
	var crc32cCombined *[]byte = nil
	var crc64NvmeCombined *[]byte = nil
	// --

	for _, part := range parts {
		// ETag calculation (MD5 of ETags)
		etag := part.ETag
		if len(etag) >= 2 && etag[0] == '"' && etag[len(etag)-1] == '"' {
			etag = etag[1 : len(etag)-1]
		}
		etagBytes, err := hex.DecodeString(etag)
		if err != nil {
			return ChecksumValues{}, fmt.Errorf("failed to decode part etag: %v", err)
		}
		etagMd5Hash.Write(etagBytes)

		switch checksumType {
		case ChecksumTypeComposite:
			if part.ChecksumCRC32 != nil {
				data, err := base64.StdEncoding.DecodeString(*part.ChecksumCRC32)
				if err != nil {
					return ChecksumValues{}, err
				}
				_, err = crc32Hash.Write(data)
				if err != nil {
					return ChecksumValues{}, err
				}
			} else {
				skipCrc32 = true
			}

			if part.ChecksumCRC32C != nil {
				data, err := base64.StdEncoding.DecodeString(*part.ChecksumCRC32C)
				if err != nil {
					return ChecksumValues{}, err
				}
				_, err = crc32cHash.Write(data)
				if err != nil {
					return ChecksumValues{}, err
				}
			} else {
				skipCrc32c = true
			}

			// not supported for checksumType Composite
			skipCrc64Nvme = true

			if part.ChecksumSHA1 != nil {
				data, err := base64.StdEncoding.DecodeString(*part.ChecksumSHA1)
				if err != nil {
					return ChecksumValues{}, err
				}
				_, err = sha1Hash.Write(data)
				if err != nil {
					return ChecksumValues{}, err
				}
			} else {
				skipSha1 = true
			}

			if part.ChecksumSHA256 != nil {
				data, err := base64.StdEncoding.DecodeString(*part.ChecksumSHA256)
				if err != nil {
					return ChecksumValues{}, err
				}
				_, err = sha256Hash.Write(data)
				if err != nil {
					return ChecksumValues{}, err
				}
			} else {
				skipSha256 = true
			}
		case ChecksumTypeFullObject:
			if part.ChecksumCRC32 != nil {
				data, err := base64.StdEncoding.DecodeString(*part.ChecksumCRC32)
				if err != nil {
					return ChecksumValues{}, err
				}
				if crc32Combined == nil {
					crc32Combined = &data
				} else {
					combined := CombineCrc32(*crc32Combined, data, part.Size)
					crc32Combined = &combined
				}
			} else {
				skipCrc32 = true
			}

			if part.ChecksumCRC32C != nil {
				data, err := base64.StdEncoding.DecodeString(*part.ChecksumCRC32C)
				if err != nil {
					return ChecksumValues{}, err
				}
				if crc32cCombined == nil {
					crc32cCombined = &data
				} else {
					combined := CombineCrc32c(*crc32cCombined, data, part.Size)
					crc32cCombined = &combined
				}
			} else {
				skipCrc32c = true
			}

			if part.ChecksumCRC64NVME != nil {
				data, err := base64.StdEncoding.DecodeString(*part.ChecksumCRC64NVME)
				if err != nil {
					return ChecksumValues{}, err
				}
				if crc64NvmeCombined == nil {
					crc64NvmeCombined = &data
				} else {
					combined := CombineCrc64Nvme(*crc64NvmeCombined, data, part.Size)
					crc64NvmeCombined = &combined
				}
			} else {
				skipCrc64Nvme = true
			}

			skipSha1 = true
			skipSha256 = true
		}
	}

	calculatedChecksums := ChecksumValues{}
	etagStr := "\"" + hex.EncodeToString(etagMd5Hash.Sum(nil)) + "-" + strconv.Itoa(len(parts)) + "\""
	calculatedChecksums.ETag = &etagStr

	switch checksumType {
	case ChecksumTypeComposite:
		if !skipCrc32 {
			val := base64.StdEncoding.EncodeToString(crc32Hash.Sum(nil)) + "-" + strconv.Itoa(len(parts))
			calculatedChecksums.ChecksumCRC32 = &val
		}
		if !skipCrc32c {
			val := base64.StdEncoding.EncodeToString(crc32cHash.Sum(nil)) + "-" + strconv.Itoa(len(parts))
			calculatedChecksums.ChecksumCRC32C = &val
		}
		if !skipSha1 {
			val := base64.StdEncoding.EncodeToString(sha1Hash.Sum(nil)) + "-" + strconv.Itoa(len(parts))
			calculatedChecksums.ChecksumSHA1 = &val
		}
		if !skipSha256 {
			val := base64.StdEncoding.EncodeToString(sha256Hash.Sum(nil)) + "-" + strconv.Itoa(len(parts))
			calculatedChecksums.ChecksumSHA256 = &val
		}
	case ChecksumTypeFullObject:
		if !skipCrc32 && crc32Combined != nil {
			val := base64.StdEncoding.EncodeToString(*crc32Combined)
			calculatedChecksums.ChecksumCRC32 = &val
		}
		if !skipCrc32c && crc32cCombined != nil {
			val := base64.StdEncoding.EncodeToString(*crc32cCombined)
			calculatedChecksums.ChecksumCRC32C = &val
		}
		if !skipCrc64Nvme && crc64NvmeCombined != nil {
			val := base64.StdEncoding.EncodeToString(*crc64NvmeCombined)
			calculatedChecksums.ChecksumCRC64NVME = &val
		}
	}

	return calculatedChecksums, nil
}
