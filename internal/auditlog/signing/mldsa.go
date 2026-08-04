package signing

import (
	"crypto/mldsa"
)

type MlDsa87Signer struct {
	priv *mldsa.PrivateKey
}

func NewMlDsa87Signer(priv *mldsa.PrivateKey) *MlDsa87Signer {
	return &MlDsa87Signer{priv: priv}
}

func (s *MlDsa87Signer) Sign(data []byte) ([]byte, error) {
	// PrivateKey.Sign produces a randomized, direct ML-DSA-87 signature.
	return s.priv.Sign(nil, data, nil)
}

type MlDsa87Verifier struct {
	pub *mldsa.PublicKey
}

func NewMlDsa87Verifier(pub *mldsa.PublicKey) *MlDsa87Verifier {
	return &MlDsa87Verifier{pub: pub}
}

func (v *MlDsa87Verifier) Verify(data, signature []byte) bool {
	return mldsa.Verify(v.pub, data, signature, nil) == nil
}
