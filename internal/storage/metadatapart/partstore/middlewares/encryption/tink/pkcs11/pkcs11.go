package pkcs11

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sync"

	"github.com/miekg/pkcs11"
)

const (
	// aesGCMIVSize is the IV size for AES-GCM (12 bytes as per NIST recommendation)
	aesGCMIVSize = 12
	// aesGCMTagSize is the authentication tag size for AES-GCM (16 bytes)
	aesGCMTagSize = 16
)

// AEAD implements tink.AEAD interface using PKCS#11 for key operations.
// The master key never leaves the HSM.
type AEAD struct {
	mu         sync.Mutex // Protects concurrent access to PKCS#11 operations
	ctx        *pkcs11.Ctx
	session    pkcs11.SessionHandle
	keyHandle  pkcs11.ObjectHandle
	modulePath string
	tokenLabel string
	slotID     uint
	loggedIn   bool
}

// NewAEAD creates a new PKCS#11 AEAD.
// modulePath: path to the PKCS#11 library (e.g., "/usr/lib64/pkcs11/libsofthsm2.so")
// tokenLabel: label of the token containing the key
// pin: PIN to access the token
// keyLabel: label of the AES key to use for encryption/decryption
func NewAEAD(modulePath, tokenLabel, pin, keyLabel string) (*AEAD, error) {
	if modulePath == "" {
		return nil, errors.New("modulePath is required")
	}
	if tokenLabel == "" {
		return nil, errors.New("tokenLabel is required")
	}
	if pin == "" {
		return nil, errors.New("pin is required")
	}
	if keyLabel == "" {
		return nil, errors.New("keyLabel is required")
	}

	// Initialize PKCS#11 context
	ctx := pkcs11.New(modulePath)
	if ctx == nil {
		return nil, fmt.Errorf("failed to load PKCS#11 module: %s", modulePath)
	}

	if err := ctx.Initialize(); err != nil {
		ctx.Destroy()
		return nil, fmt.Errorf("failed to initialize PKCS#11: %w", err)
	}

	// Find the slot with the matching token label
	slotID, err := findSlotByTokenLabel(ctx, tokenLabel)
	if err != nil {
		ctx.Finalize()
		ctx.Destroy()
		return nil, err
	}

	// Open a session
	session, err := ctx.OpenSession(slotID, pkcs11.CKF_SERIAL_SESSION|pkcs11.CKF_RW_SESSION)
	if err != nil {
		ctx.Finalize()
		ctx.Destroy()
		return nil, fmt.Errorf("failed to open PKCS#11 session: %w", err)
	}

	// Login with UserPIN
	if err := ctx.Login(session, pkcs11.CKU_USER, pin); err != nil {
		ctx.CloseSession(session)
		ctx.Finalize()
		ctx.Destroy()
		return nil, fmt.Errorf("failed to login to PKCS#11 token: %w", err)
	}

	// Find the AES key by label
	keyHandle, err := findKeyByLabel(ctx, session, keyLabel)
	if err != nil {
		ctx.Logout(session)
		ctx.CloseSession(session)
		ctx.Finalize()
		ctx.Destroy()
		return nil, err
	}

	return &AEAD{
		ctx:        ctx,
		session:    session,
		keyHandle:  keyHandle,
		modulePath: modulePath,
		tokenLabel: tokenLabel,
		slotID:     slotID,
		loggedIn:   true,
	}, nil
}

// findSlotByTokenLabel finds the slot ID for a token with the given label
func findSlotByTokenLabel(ctx *pkcs11.Ctx, tokenLabel string) (uint, error) {
	slots, err := ctx.GetSlotList(true) // Only slots with tokens present
	if err != nil {
		return 0, fmt.Errorf("failed to get slot list: %w", err)
	}

	for _, slot := range slots {
		tokenInfo, err := ctx.GetTokenInfo(slot)
		if err != nil {
			continue
		}
		// Token labels are padded with spaces to 32 chars, so we need to trim
		if trimLabel(tokenInfo.Label) == tokenLabel {
			return slot, nil
		}
	}

	return 0, fmt.Errorf("token with label %q not found", tokenLabel)
}

// trimLabel removes trailing spaces from PKCS#11 labels
func trimLabel(label string) string {
	// Labels are fixed-size and padded with spaces
	for len(label) > 0 && label[len(label)-1] == ' ' {
		label = label[:len(label)-1]
	}
	return label
}

// findKeyByLabel finds an AES key by its label
func findKeyByLabel(ctx *pkcs11.Ctx, session pkcs11.SessionHandle, keyLabel string) (pkcs11.ObjectHandle, error) {
	template := []*pkcs11.Attribute{
		pkcs11.NewAttribute(pkcs11.CKA_CLASS, pkcs11.CKO_SECRET_KEY),
		pkcs11.NewAttribute(pkcs11.CKA_KEY_TYPE, pkcs11.CKK_AES),
		pkcs11.NewAttribute(pkcs11.CKA_LABEL, keyLabel),
	}

	if err := ctx.FindObjectsInit(session, template); err != nil {
		return 0, fmt.Errorf("failed to init find objects: %w", err)
	}
	defer ctx.FindObjectsFinal(session)

	handles, _, err := ctx.FindObjects(session, 1)
	if err != nil {
		return 0, fmt.Errorf("failed to find objects: %w", err)
	}

	if len(handles) == 0 {
		return 0, fmt.Errorf("AES key with label %q not found", keyLabel)
	}

	return handles[0], nil
}

// Encrypt encrypts plaintext with associatedData using AES-GCM via PKCS#11.
// This implements the tink.AEAD interface.
// Output format: [IV (12 bytes)] [ciphertext + tag]
func (a *AEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Generate random IV
	iv := make([]byte, aesGCMIVSize)
	if _, err := rand.Read(iv); err != nil {
		return nil, fmt.Errorf("failed to generate IV: %w", err)
	}

	// Setup GCM mechanism with IV and AAD
	gcmParams := pkcs11.NewGCMParams(iv, associatedData, aesGCMTagSize*8)
	mechanism := []*pkcs11.Mechanism{
		pkcs11.NewMechanism(pkcs11.CKM_AES_GCM, gcmParams),
	}

	if err := a.ctx.EncryptInit(a.session, mechanism, a.keyHandle); err != nil {
		return nil, fmt.Errorf("failed to init encryption: %w", err)
	}

	ciphertext, err := a.ctx.Encrypt(a.session, plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt: %w", err)
	}

	// Prepend IV to ciphertext
	result := make([]byte, len(iv)+len(ciphertext))
	copy(result[:len(iv)], iv)
	copy(result[len(iv):], ciphertext)

	return result, nil
}

// Decrypt decrypts ciphertext with associatedData using AES-GCM via PKCS#11.
// This implements the tink.AEAD interface.
// Input format: [IV (12 bytes)] [ciphertext + tag]
func (a *AEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(ciphertext) < aesGCMIVSize+aesGCMTagSize {
		return nil, errors.New("ciphertext too short")
	}

	// Extract IV and actual ciphertext
	iv := ciphertext[:aesGCMIVSize]
	actualCiphertext := ciphertext[aesGCMIVSize:]

	// Setup GCM mechanism with IV and AAD
	gcmParams := pkcs11.NewGCMParams(iv, associatedData, aesGCMTagSize*8)
	mechanism := []*pkcs11.Mechanism{
		pkcs11.NewMechanism(pkcs11.CKM_AES_GCM, gcmParams),
	}

	if err := a.ctx.DecryptInit(a.session, mechanism, a.keyHandle); err != nil {
		return nil, fmt.Errorf("failed to init decryption: %w", err)
	}

	plaintext, err := a.ctx.Decrypt(a.session, actualCiphertext)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}

// Close releases all PKCS#11 resources
func (a *AEAD) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var errs []error

	if a.loggedIn {
		if err := a.ctx.Logout(a.session); err != nil {
			errs = append(errs, fmt.Errorf("failed to logout: %w", err))
		}
		a.loggedIn = false
	}

	if err := a.ctx.CloseSession(a.session); err != nil {
		errs = append(errs, fmt.Errorf("failed to close session: %w", err))
	}

	if err := a.ctx.Finalize(); err != nil {
		errs = append(errs, fmt.Errorf("failed to finalize: %w", err))
	}

	a.ctx.Destroy()

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
