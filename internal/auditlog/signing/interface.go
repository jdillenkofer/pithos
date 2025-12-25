package signing

type Signer interface {
	Sign(data []byte) ([]byte, error)
}

type Verifier interface {
	Verify(data, signature []byte) bool
}
