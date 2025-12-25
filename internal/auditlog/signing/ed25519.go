package signing

import (
	"crypto/ed25519"
)

type Ed25519Signer struct {
	priv ed25519.PrivateKey
}

func NewEd25519Signer(priv ed25519.PrivateKey) *Ed25519Signer {
	return &Ed25519Signer{priv: priv}
}

func (s *Ed25519Signer) Sign(data []byte) ([]byte, error) {
	return ed25519.Sign(s.priv, data), nil
}

type Ed25519Verifier struct {
	pub ed25519.PublicKey
}

func NewEd25519Verifier(pub ed25519.PublicKey) *Ed25519Verifier {
	return &Ed25519Verifier{pub: pub}
}

func (v *Ed25519Verifier) Verify(data, signature []byte) bool {
	return ed25519.Verify(v.pub, data, signature)
}
