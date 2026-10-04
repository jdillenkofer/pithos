package metadatastore

import "errors"

var ErrInvalidChecksumConfiguration = errors.New("InvalidChecksumConfiguration")

// ResolveMultipartChecksumType validates the initiation settings and selects a
// type supported by the chosen algorithm. Without an algorithm, preserve the
// legacy FULL_OBJECT default; unknown legacy algorithms are never inferred.
func ResolveMultipartChecksumType(algorithm, requestedType *string) (*string, error) {
	effective := ChecksumTypeFullObject
	if algorithm != nil {
		switch *algorithm {
		case "CRC32", "CRC32C", "SHA1", "SHA256":
			effective = ChecksumTypeComposite
		case "CRC64NVME":
		default:
			return nil, ErrInvalidChecksumConfiguration
		}
	}
	if requestedType != nil {
		effective = *requestedType
	}
	if effective != ChecksumTypeFullObject && effective != ChecksumTypeComposite {
		return nil, ErrInvalidChecksumConfiguration
	}
	if algorithm != nil {
		if (*algorithm == "SHA1" || *algorithm == "SHA256") && effective != ChecksumTypeComposite {
			return nil, ErrInvalidChecksumConfiguration
		}
		if *algorithm == "CRC64NVME" && effective != ChecksumTypeFullObject {
			return nil, ErrInvalidChecksumConfiguration
		}
	}
	return &effective, nil
}
