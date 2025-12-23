package auditlog

import (
	"crypto/sha512"
)

// CalculateMerkleRoot computes the Merkle Root of a slice of hashes using SHA-512.
// It handles odd node counts by duplicating the last node at each level.
func CalculateMerkleRoot(hashes [][]byte) []byte {
	if len(hashes) == 0 {
		return nil
	}
	if len(hashes) == 1 {
		return hashes[0]
	}

	// Copy hashes to avoid modifying the input slice
	current := make([][]byte, len(hashes))
	copy(current, hashes)

	for len(current) > 1 {
		var next [][]byte
		for i := 0; i < len(current); i += 2 {
			if i+1 == len(current) {
				// Odd number of nodes: hash(current[i] + current[i])
				h := sha512.New()
				h.Write(current[i])
				h.Write(current[i])
				next = append(next, h.Sum(nil))
			} else {
				// Even number: hash(current[i] + current[i+1])
				h := sha512.New()
				h.Write(current[i])
				h.Write(current[i+1])
				next = append(next, h.Sum(nil))
			}
		}
		current = next
	}

	return current[0]
}
