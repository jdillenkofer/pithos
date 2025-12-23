package signing

import (
	"github.com/cloudflare/circl/sign/mldsa/mldsa65"
)

type MlDsaSigner struct {
	priv *mldsa65.PrivateKey
}

func NewMlDsaSigner(priv *mldsa65.PrivateKey) *MlDsaSigner {
	return &MlDsaSigner{priv: priv}
}

func (s *MlDsaSigner) Sign(data []byte) ([]byte, error) {
	// ML-DSA-65 Sign implementation using SignTo. 
	// We use randomized=true for security as per recommended practices.
	sig := make([]byte, mldsa65.SignatureSize)
	err := mldsa65.SignTo(s.priv, data, nil, true, sig)
	if err != nil {
		return nil, err
	}
	return sig, nil
}

type MlDsaVerifier struct {
	pub *mldsa65.PublicKey
}

func NewMlDsaVerifier(pub *mldsa65.PublicKey) *MlDsaVerifier {
	return &MlDsaVerifier{pub: pub}
}

func (v *MlDsaVerifier) Verify(data, signature []byte) bool {
	// ML-DSA-65 Verify implementation.
	// Context is usually empty in standard use cases unless specified.
	return mldsa65.Verify(v.pub, data, nil, signature)
}
