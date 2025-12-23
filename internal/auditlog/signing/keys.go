package signing

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"strings"

	"github.com/cloudflare/circl/sign/mldsa/mldsa65"
)

func LoadEd25519PublicKey(input string) (ed25519.PublicKey, error) {
	data, err := loadRawData(input)
	if err != nil {
		return nil, err
	}

	if len(data) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid Ed25519 public key size: expected %d, got %d", ed25519.PublicKeySize, len(data))
	}

	return ed25519.PublicKey(data), nil
}

func LoadEd25519PrivateKey(input string) (ed25519.PrivateKey, error) {
	data, err := loadRawData(input)
	if err != nil {
		return nil, err
	}

	if len(data) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid Ed25519 private key size: expected %d, got %d", ed25519.PrivateKeySize, len(data))
	}

	return ed25519.PrivateKey(data), nil
}

func LoadMlDsaPublicKey(input string) (*mldsa65.PublicKey, error) {
	data, err := loadRawData(input)
	if err != nil {
		return nil, err
	}

	pub := &mldsa65.PublicKey{}
	if err := pub.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ML-DSA public key: %w", err)
	}
	return pub, nil
}

func LoadMlDsaPrivateKey(input string) (*mldsa65.PrivateKey, error) {
	data, err := loadRawData(input)
	if err != nil {
		return nil, err
	}

	priv := &mldsa65.PrivateKey{}
	if err := priv.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ML-DSA private key: %w", err)
	}
	return priv, nil
}

func loadRawData(input string) ([]byte, error) {
	// Try to read as file first
	data, err := os.ReadFile(input)
	if err == nil {
		data = bytes.TrimSpace(data)
		// Try raw bytes if size matches common key sizes (heuristically)
		// otherwise try base64
		if isLikelyRawKey(data) {
			return data, nil
		}
		
		decoded, err := base64.StdEncoding.DecodeString(string(data))
		if err == nil {
			return decoded, nil
		}
		// If it was a file but not valid base64 and not raw, we might have issues, 
		// but we'll fall through to treat the original input string as base64.
	}

	// Try as base64 string
	input = strings.TrimSpace(input)
	decoded, err := base64.StdEncoding.DecodeString(input)
	if err != nil {
		return nil, fmt.Errorf("failed to decode base64: %w", err)
	}
	return decoded, nil
}

func isLikelyRawKey(data []byte) bool {
	l := len(data)
	return l == ed25519.PublicKeySize || l == ed25519.PrivateKeySize || l == mldsa65.PublicKeySize || l == mldsa65.PrivateKeySize
}

func GenerateEd25519KeyPair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(nil)
}

func GenerateMlDsaKeyPair() ([]byte, []byte, error) {
	pub, priv, err := mldsa65.GenerateKey(nil)
	if err != nil {
		return nil, nil, err
	}
	pubBytes, _ := pub.MarshalBinary()
	privBytes, _ := priv.MarshalBinary()
	return pubBytes, privBytes, nil
}
