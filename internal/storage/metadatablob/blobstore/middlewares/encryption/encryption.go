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
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/encryption/pkcs7padding"
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

func (ebsm *encryptionBlobStoreMiddleware) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	// @TODO: cache reader on disk
	data, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	paddedData, err := pkcs7padding.Pad(data, aes.BlockSize)
	if err != nil {
		return err
	}

	if len(paddedData)%aes.BlockSize != 0 {
		return ErrPlaintextNotAMultipleOfBlockSize
	}

	block, err := aes.NewCipher(ebsm.key)
	if err != nil {
		return err
	}

	ciphertext := make([]byte, aes.BlockSize+len(paddedData))
	iv := ciphertext[:aes.BlockSize]
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return err
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	mode.CryptBlocks(ciphertext[aes.BlockSize:], paddedData)

	mac := hmac.New(sha256.New, ebsm.key)
	mac.Write(ciphertext)
	ciphertextWithMAC := mac.Sum(ciphertext)

	byteReadSeekCloser := ioutils.NewByteReadSeekCloser(ciphertextWithMAC)
	err = ebsm.innerBlobStore.PutBlob(ctx, tx, blobId, byteReadSeekCloser)
	if err != nil {
		return err
	}

	return nil
}

func validHMAC(message io.ReadSeekCloser, messageMAC, key []byte) (*bool, error) {
	mac := hmac.New(sha256.New, key)
	_, err := io.Copy(mac, message)
	if err != nil {
		return nil, err
	}
	expectedMAC := mac.Sum(nil)
	valid := hmac.Equal(messageMAC, expectedMAC)
	return &valid, nil
}

func (ebsm *encryptionBlobStoreMiddleware) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadSeekCloser, error) {
	readSeekCloser, err := ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}

	endOffset, err := readSeekCloser.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	hmacOffset := endOffset - hmacSize
	if hmacOffset < aes.BlockSize {
		return nil, ErrCiphertextTooShort
	}

	_, err = readSeekCloser.Seek(hmacOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}
	ciphertextMAC, err := io.ReadAll(readSeekCloser)
	if err != nil {
		return nil, err
	}
	_, err = readSeekCloser.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	key := ebsm.key
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	// Verify HMAC before decryption
	ciphertext := ioutils.NewLimitedEndReadSeekCloser(readSeekCloser, hmacOffset)
	valid, err := validHMAC(ciphertext, ciphertextMAC, key)
	if err != nil {
		return nil, err
	}
	if !*valid {
		return nil, ErrInvalidHMAC
	}

	_, err = ciphertext.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	iv := make([]byte, aes.BlockSize)
	_, err = io.ReadFull(ciphertext, iv)
	if err != nil {
		return nil, err
	}

	_, err = ciphertext.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	ciphertext = ioutils.NewLimitedStartReadSeekCloser(ciphertext, aes.BlockSize)

	ciphertextEnd, err := ciphertext.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, err
	}

	// CBC mode always works in whole blocks.
	if ciphertextEnd%aes.BlockSize != 0 {
		return nil, ErrCiphertextNotAMultipleOfBlockSize
	}

	_, err = ciphertext.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	mode := cipher.NewCBCDecrypter(block, iv)

	ciphertextBytes, err := io.ReadAll(ciphertext)
	if err != nil {
		return nil, err
	}

	mode.CryptBlocks(ciphertextBytes, ciphertextBytes)

	unpaddedData, err := pkcs7padding.Unpad(ciphertextBytes, aes.BlockSize)
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
