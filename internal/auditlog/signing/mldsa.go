package signing

import (
	"github.com/cloudflare/circl/sign/mldsa/mldsa87"
)

type MlDsa87Signer struct {
	priv *mldsa87.PrivateKey
}

func NewMlDsa87Signer(priv *mldsa87.PrivateKey) *MlDsa87Signer {
	return &MlDsa87Signer{priv: priv}
}

func (s *MlDsa87Signer) Sign(data []byte) ([]byte, error) {
	// ML-DSA-87 Sign implementation using SignTo. 
	// We use randomized=true for security as per recommended practices.
	sig := make([]byte, mldsa87.SignatureSize)
	err := mldsa87.SignTo(s.priv, data, nil, true, sig)
	if err != nil {
		return nil, err
	}
	return sig, nil
}

type MlDsa87Verifier struct {
	pub *mldsa87.PublicKey
}

func NewMlDsa87Verifier(pub *mldsa87.PublicKey) *MlDsa87Verifier {
	return &MlDsa87Verifier{pub: pub}
}

func (v *MlDsa87Verifier) Verify(data, signature []byte) bool {
	// ML-DSA-87 Verify implementation.
	// Context is usually empty in standard use cases unless specified.
	return mldsa87.Verify(v.pub, data, nil, signature)
}