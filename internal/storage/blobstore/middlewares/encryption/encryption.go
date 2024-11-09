package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"errors"
	"io"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/blobstore/middlewares/encryption/pkcs7padding"
	"golang.org/x/crypto/scrypt"
)

type encryptionBlobStoreMiddleware struct {
	key            []byte
	innerBlobStore blobstore.BlobStore
}

const hmacSize = 32

var (
	ErrDataTooShortForHMACValidation     = errors.New("data too short for HMAC validation")
	ErrInvalidHMAC                       = errors.New("invalid HMAC")
	ErrCiphertextTooShort                = errors.New("ciphertext too short")
	ErrCiphertextNotAMultipleOfBlockSize = errors.New("ciphertext is not a multiple of the block size")
	ErrPlaintextNotAMultipleOfBlockSize  = errors.New("plaintext is not a multiple of the block size")
)

func New(password string, innerBlobStore blobstore.BlobStore) (blobstore.BlobStore, error) {
	key, err := scrypt.Key([]byte(password), []byte("pithos"), 1<<16, 8, 1, 32)
	if err != nil {
		return nil, err
	}
	ebsm := &encryptionBlobStoreMiddleware{
		key:            key,
		innerBlobStore: innerBlobStore,
	}
	return ebsm, nil
}

func (ebsm *encryptionBlobStoreMiddleware) Start(ctx context.Context) error {
	return ebsm.innerBlobStore.Start(ctx)
}

func (ebsm *encryptionBlobStoreMiddleware) Stop(ctx context.Context) error {
	return ebsm.innerBlobStore.Stop(ctx)
}

func (ebsm *encryptionBlobStoreMiddleware) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) (*blobstore.PutBlobResult, error) {
	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}

	// Recalculate etag on original data
	originalSize := int64(len(data))
	originalReadSeekCloser := ioutils.NewByteReadSeekCloser(data)
	etag, err := blobstore.CalculateETag(originalReadSeekCloser)
	if err != nil {
		return nil, err
	}

	paddedData, err := pkcs7padding.Pad(data, aes.BlockSize)
	if err != nil {
		return nil, err
	}

	if len(paddedData)%aes.BlockSize != 0 {
		return nil, ErrPlaintextNotAMultipleOfBlockSize
	}

	block, err := aes.NewCipher(ebsm.key)
	if err != nil {
		return nil, err
	}

	ciphertext := make([]byte, aes.BlockSize+len(paddedData))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, err
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext[aes.BlockSize:], paddedData)

	mac := hmac.New(sha256.New, ebsm.key)
	mac.Write(ciphertext)
	ciphertextWithMAC := mac.Sum(ciphertext)

	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(ciphertextWithMAC)
	putBlobResult, err := ebsm.innerBlobStore.PutBlob(ctx, tx, blobId, byteReadSeekCloser)
	if err != nil {
		return nil, err
	}

	putBlobResult.Size = originalSize
	putBlobResult.ETag = *etag
	return putBlobResult, nil
}

func validHMAC(message, messageMAC, key []byte) bool {
	mac := hmac.New(sha256.New, key)
	mac.Write(message)
	expectedMAC := mac.Sum(nil)
	return hmac.Equal(messageMAC, expectedMAC)
}

func (ebsm *encryptionBlobStoreMiddleware) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadSeekCloser, error) {
	readSeekCloser, err := ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(readSeekCloser)
	if err != nil {
		return nil, err
	}

	key := ebsm.key
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	if len(data) < hmacSize {
		return nil, ErrDataTooShortForHMACValidation
	}

	// Verify HMAC before decryption
	ciphertext := data[:len(data)-hmacSize]
	ciphertextMAC := data[len(data)-hmacSize:]
	if !validHMAC(ciphertext, ciphertextMAC, key) {
		return nil, ErrInvalidHMAC
	}

	if len(ciphertext) < aes.BlockSize {
		return nil, ErrCiphertextTooShort
	}

	iv := ciphertext[:aes.BlockSize]
	ciphertext = ciphertext[aes.BlockSize:]

	// CBC mode always works in whole blocks.
	if len(ciphertext)%aes.BlockSize != 0 {
		return nil, ErrCiphertextNotAMultipleOfBlockSize
	}

	mode := cipher.NewCBCDecrypter(block, iv)

	mode.CryptBlocks(ciphertext, ciphertext)

	unpaddedData, err := pkcs7padding.Unpad(ciphertext, aes.BlockSize)
	if err != nil {
		return nil, err
	}

	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(unpaddedData)
	return byteReadSeekCloser, nil
}

func (ebsm *encryptionBlobStoreMiddleware) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	return ebsm.innerBlobStore.GetBlobIds(ctx, tx)
}

func (ebsm *encryptionBlobStoreMiddleware) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	return ebsm.innerBlobStore.DeleteBlob(ctx, tx, blobId)
}
