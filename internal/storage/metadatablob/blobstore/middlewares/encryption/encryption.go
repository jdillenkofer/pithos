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

func validHMAC(message io.ReadCloser, messageMAC, key []byte) (*bool, error) {
	mac := hmac.New(sha256.New, key)
	_, err := io.Copy(mac, message)
	if err != nil {
		return nil, err
	}
	expectedMAC := mac.Sum(nil)
	valid := hmac.Equal(messageMAC, expectedMAC)
	return &valid, nil
}

type cbcDecrypterReader struct {
	mode                        cipher.BlockMode
	innerReadCloser             io.ReadCloser
	internalSrc                 []byte
	internalDst                 []byte
	bytesRemainingInInternalDst int64
	innerReaderEndOffset        int64
}

func newCBCDecrypterReader(key []byte, iv []byte, innerReaderEndOffset int64, innerReadCloser io.ReadCloser) (io.ReadCloser, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	mode := cipher.NewCBCDecrypter(block, iv)
	return &cbcDecrypterReader{
		mode:                        mode,
		innerReadCloser:             innerReadCloser,
		internalSrc:                 make([]byte, aes.BlockSize*512),
		internalDst:                 make([]byte, aes.BlockSize*512),
		bytesRemainingInInternalDst: 0,
		innerReaderEndOffset:        innerReaderEndOffset,
	}, nil
}

func (dr *cbcDecrypterReader) Read(p []byte) (int, error) {
	if dr.bytesRemainingInInternalDst > 0 {
		n := copy(p, dr.internalDst[:dr.bytesRemainingInInternalDst])
		copy(dr.internalDst, dr.internalDst[n:dr.bytesRemainingInInternalDst])
		dr.bytesRemainingInInternalDst -= int64(n)
		return n, nil
	}
	// Make sure that no more data can be read when reaching endOffset
	if dr.innerReaderEndOffset == 0 {
		return 0, io.EOF
	}

	var bytesRead int
	var err error
	// Read until the entire internalSrc buffer is full (optimization)
	if dr.innerReaderEndOffset > int64(len(dr.internalSrc)) {
		bytesRead, err = io.ReadFull(dr.innerReadCloser, dr.internalSrc)
		if err != nil {
			return bytesRead, err
		}
	} else {
		// Read at least aes.BlockSize bytes
		bytesRead, err = io.ReadAtLeast(dr.innerReadCloser, dr.internalSrc, aes.BlockSize)
		if err != nil {
			return bytesRead, err
		}

		// Always round up to the next full blockSize
		remainingBytesToReadForFullBlock := bytesRead % aes.BlockSize
		if remainingBytesToReadForFullBlock > 0 {
			buf := make([]byte, remainingBytesToReadForFullBlock)
			bytesRead2, err := io.ReadFull(dr.innerReadCloser, buf)
			if err != nil {
				return bytesRead2, err
			}
			copy(dr.internalSrc[bytesRead:], buf)
			bytesRead += bytesRead2
		}
	}
	dr.innerReaderEndOffset -= int64(bytesRead)
	dr.mode.CryptBlocks(dr.internalDst[:bytesRead], dr.internalSrc[:bytesRead])
	dr.bytesRemainingInInternalDst += int64(bytesRead)

	if dr.innerReaderEndOffset == 0 {
		unpaddedData, err := pkcs7padding.Unpad(dr.internalDst[:dr.bytesRemainingInInternalDst], aes.BlockSize)
		if err != nil {
			return -1, err
		}
		dr.bytesRemainingInInternalDst = int64(copy(dr.internalDst, unpaddedData))
		n := copy(p, dr.internalDst[:dr.bytesRemainingInInternalDst])
		copy(dr.internalDst, dr.internalDst[n:])
		dr.bytesRemainingInInternalDst -= int64(n)
		return n, nil
	}
	n := copy(p, dr.internalDst[:dr.bytesRemainingInInternalDst])
	copy(dr.internalDst, dr.internalDst[n:dr.bytesRemainingInInternalDst])
	dr.bytesRemainingInInternalDst -= int64(n)
	return n, nil
}

func (dr *cbcDecrypterReader) Close() error {
	return dr.innerReadCloser.Close()
}

func (ebsm *encryptionBlobStoreMiddleware) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadCloser, error) {
	readCloser, err := ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}

	var endOffset int64
	if readSeekCloser, ok := readCloser.(io.ReadSeekCloser); ok {
		endOffset, err = readSeekCloser.Seek(0, io.SeekEnd)
		if err != nil {
			return nil, err
		}
		_, err := readSeekCloser.Seek(0, io.SeekStart)
		if err != nil {
			return nil, err
		}
	} else {
		{
			defer readCloser.Close()
			endOffset, err = ioutils.SkipAllBytes(readCloser)
			if err != nil {
				return nil, err
			}
		}
		readCloser, err = ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
		if err != nil {
			return nil, err
		}
	}

	hmacOffset := endOffset - hmacSize
	if hmacOffset < aes.BlockSize {
		return nil, ErrCiphertextTooShort
	}
	/*

		var ciphertextMAC []byte
		if readSeekCloser, ok := readCloser.(io.ReadSeekCloser); ok {
			readSeekCloser.Seek(hmacOffset, io.SeekStart)
			ciphertextMAC, err = io.ReadAll(readCloser)
			if err != nil {
				return nil, err
			}
			readSeekCloser.Seek(0, io.SeekStart)
		} else {
			{
				defer readCloser.Close()
				_, err = ioutils.SkipNBytes(readCloser, hmacOffset)
				if err != nil {
					return nil, err
				}
				ciphertextMAC, err = io.ReadAll(readCloser)
				if err != nil {
					return nil, err
				}
			}
			readCloser, err = ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
			if err != nil {
				return nil, err
			}
		}

		// Verify HMAC before decryption
		{
			readCloser, err := ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
			if err != nil {
				return nil, err
			}
			ciphertext := ioutils.NewLimitedEndReadCloser(readCloser, hmacOffset)
			defer ciphertext.Close()

			valid, err := validHMAC(ciphertext, ciphertextMAC, ebsm.key)
			if err != nil {
				return nil, err
			}
			if !*valid {
				return nil, ErrInvalidHMAC
			}
		}
	*/

	readCloser, err = ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}

	lazyReadCloser := ioutils.NewLazyReadCloser(func() (io.ReadCloser, error) {
		cipherStream := ioutils.NewLimitedEndReadCloser(readCloser, hmacOffset)

		iv := make([]byte, aes.BlockSize)
		_, err = io.ReadFull(cipherStream, iv)
		if err != nil {
			return nil, err
		}

		// CBC mode always works in whole blocks.
		if hmacOffset%aes.BlockSize != 0 {
			return nil, ErrCiphertextNotAMultipleOfBlockSize
		}

		decryptedStream, err := newCBCDecrypterReader(ebsm.key, iv, hmacOffset-int64(len(iv)), cipherStream)
		if err != nil {
			return nil, err
		}
		return decryptedStream, nil
	})

	return lazyReadCloser, nil
}

func (ebsm *encryptionBlobStoreMiddleware) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	return ebsm.innerBlobStore.GetBlobIds(ctx, tx)
}

func (ebsm *encryptionBlobStoreMiddleware) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	return ebsm.innerBlobStore.DeleteBlob(ctx, tx, blobId)
}
