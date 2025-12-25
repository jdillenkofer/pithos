package signing

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"os"

	"github.com/cloudflare/circl/sign/mldsa/mldsa87"
)

func LoadEd25519PublicKey(input string) (ed25519.PublicKey, error) {
	data, err := loadKeyData(input)
	if err != nil {
		return nil, err
	}

	if len(data) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid Ed25519 public key size: expected %d, got %d", ed25519.PublicKeySize, len(data))
	}

	return ed25519.PublicKey(data), nil
}

func LoadEd25519PrivateKey(input string) (ed25519.PrivateKey, error) {
	data, err := loadKeyData(input)
	if err != nil {
		return nil, err
	}

	if len(data) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid Ed25519 private key size: expected %d, got %d", ed25519.PrivateKeySize, len(data))
	}

	return ed25519.PrivateKey(data), nil
}

func LoadMlDsa87PublicKey(input string) (*mldsa87.PublicKey, error) {
	data, err := loadKeyData(input)
	if err != nil {
		return nil, err
	}

	pub := &mldsa87.PublicKey{}
	if err := pub.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ML-DSA public key: %w", err)
	}
	return pub, nil
}

func LoadMlDsa87PrivateKey(input string) (*mldsa87.PrivateKey, error) {
	data, err := loadKeyData(input)
	if err != nil {
		return nil, err
	}

	priv := &mldsa87.PrivateKey{}
	if err := priv.UnmarshalBinary(data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal ML-DSA private key: %w", err)
	}
	return priv, nil
}

func loadKeyData(input string) ([]byte, error) {
	// Try to read as file first
	data, err := os.ReadFile(input)
	if err == nil {
		return decodeKey(data)
	}

	// Treat input string as the key data itself
	return decodeKey([]byte(input))
}

func decodeKey(data []byte) ([]byte, error) {
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, fmt.Errorf("empty key data")
	}

	// 1. Try PEM
	block, _ := pem.Decode(data)
	if block != nil {
		return block.Bytes, nil
	}

	// 2. Try Base64
	decoded, err := base64.StdEncoding.DecodeString(string(data))
	if err == nil {
		return decoded, nil
	}

	return nil, fmt.Errorf("failed to decode key: must be PEM or Base64 encoded")
}

func GenerateEd25519KeyPair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	return ed25519.GenerateKey(nil)
}

func GenerateMlDsa87KeyPair() ([]byte, []byte, error) {
	pub, priv, err := mldsa87.GenerateKey(nil)
	if err != nil {
		return nil, nil, err
	}
	pubBytes, _ := pub.MarshalBinary()
	privBytes, _ := priv.MarshalBinary()
	return pubBytes, privBytes, nil
}