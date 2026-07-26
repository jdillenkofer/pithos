package pkcs11

import miekgpkcs11 "github.com/miekg/pkcs11"

type cryptokiContext interface {
	Initialize(opts ...miekgpkcs11.InitializeOption) error
	Finalize() error
	Destroy()
	GetSlotList(tokenPresent bool) ([]uint, error)
	GetTokenInfo(slotID uint) (miekgpkcs11.TokenInfo, error)
	OpenSession(slotID uint, flags uint) (miekgpkcs11.SessionHandle, error)
	CloseSession(session miekgpkcs11.SessionHandle) error
	Login(session miekgpkcs11.SessionHandle, userType uint, pin string) error
	Logout(session miekgpkcs11.SessionHandle) error
	FindObjectsInit(session miekgpkcs11.SessionHandle, template []*miekgpkcs11.Attribute) error
	FindObjects(session miekgpkcs11.SessionHandle, max int) ([]miekgpkcs11.ObjectHandle, bool, error)
	FindObjectsFinal(session miekgpkcs11.SessionHandle) error
	EncryptInit(session miekgpkcs11.SessionHandle, mechanisms []*miekgpkcs11.Mechanism, key miekgpkcs11.ObjectHandle) error
	Encrypt(session miekgpkcs11.SessionHandle, plaintext []byte) ([]byte, error)
	DecryptInit(session miekgpkcs11.SessionHandle, mechanisms []*miekgpkcs11.Mechanism, key miekgpkcs11.ObjectHandle) error
	Decrypt(session miekgpkcs11.SessionHandle, ciphertext []byte) ([]byte, error)
}

func loadCryptokiContext(modulePath string) cryptokiContext {
	ctx := miekgpkcs11.New(modulePath)
	if ctx == nil {
		return nil
	}
	return ctx
}
