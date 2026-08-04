package signing

import (
	"bytes"
	"crypto/mldsa"
	"encoding/base64"
	"strings"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

func TestMlDsa87KeyRoundTrip(t *testing.T) {
	testutils.SkipIfIntegration(t)

	pubBytes, privBytes, err := GenerateMlDsa87KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	if len(pubBytes) != mldsa.MLDSA87PublicKeySize {
		t.Fatalf("unexpected public key size: got %d, want %d", len(pubBytes), mldsa.MLDSA87PublicKeySize)
	}
	if len(privBytes) != mldsa.PrivateKeySize {
		t.Fatalf("unexpected private key size: got %d, want %d", len(privBytes), mldsa.PrivateKeySize)
	}

	pub, err := LoadMlDsa87PublicKey(base64.StdEncoding.EncodeToString(pubBytes))
	if err != nil {
		t.Fatal(err)
	}
	priv, err := LoadMlDsa87PrivateKey(base64.StdEncoding.EncodeToString(privBytes))
	if err != nil {
		t.Fatal(err)
	}

	message := []byte("audit grounding root")
	signature, err := NewMlDsa87Signer(priv).Sign(message)
	if err != nil {
		t.Fatal(err)
	}
	if len(signature) != mldsa.MLDSA87SignatureSize {
		t.Fatalf("unexpected signature size: got %d, want %d", len(signature), mldsa.MLDSA87SignatureSize)
	}

	verifier := NewMlDsa87Verifier(pub)
	if !verifier.Verify(message, signature) {
		t.Fatal("signature verification failed")
	}
	signature[0] ^= 1
	if verifier.Verify(message, signature) {
		t.Fatal("tampered signature unexpectedly verified")
	}
}

func TestLoadMlDsa87PrivateKeyExplainsLegacyKeyMigration(t *testing.T) {
	testutils.SkipIfIntegration(t)

	legacyKey := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0}, 4896))
	_, err := LoadMlDsa87PrivateKey(legacyKey)
	if err == nil {
		t.Fatal("expected legacy private key to be rejected")
	}
	if !strings.Contains(err.Error(), "legacy CIRCL keys must be regenerated") {
		t.Fatalf("migration guidance missing from error: %v", err)
	}
}
