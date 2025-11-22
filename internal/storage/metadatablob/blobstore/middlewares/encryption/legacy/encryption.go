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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/lifecycle"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore"
	"github.com/jdillenkofer/pithos/internal/storage/metadatablob/blobstore/middlewares/encryption/pkcs7padding"
	"golang.org/x/crypto/scrypt"
)

type legacyEncryptionBlobStoreMiddleware struct {
	*lifecycle.ValidatedLifecycle
	key            []byte
	innerBlobStore blobstore.BlobStore
	tracer         trace.Tracer
}

// Compile-time check to ensure legacyEncryptionBlobStoreMiddleware implements blobstore.BlobStore
var _ blobstore.BlobStore = (*legacyEncryptionBlobStoreMiddleware)(nil)

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
	lifecycle, err := lifecycle.NewValidatedLifecycle("LegacyEncryptionBlobStoreMiddleware")
	if err != nil {
		return nil, err
	}
	ebsm := &legacyEncryptionBlobStoreMiddleware{
		ValidatedLifecycle: lifecycle,
		key:                key,
		innerBlobStore:     innerBlobStore,
		tracer:             otel.Tracer("internal/storage/metadatablob/blobstore/middlewares/encryption/legacy"),
	}
	return ebsm, nil
}

func (ebsm *legacyEncryptionBlobStoreMiddleware) Start(ctx context.Context) error {
	if err := ebsm.ValidatedLifecycle.Start(ctx); err != nil {
		return err
	}
	return ebsm.innerBlobStore.Start(ctx)
}

func (ebsm *legacyEncryptionBlobStoreMiddleware) Stop(ctx context.Context) error {
	if err := ebsm.ValidatedLifecycle.Stop(ctx); err != nil {
		return err
	}
	return ebsm.innerBlobStore.Stop(ctx)
}

type cbcEncrypterReader struct {
	mode                        cipher.BlockMode
	innerReadCloser             io.ReadCloser
	internalSrc                 []byte
	internalDst                 []byte
	bytesRemainingInInternalDst int64
	reachedEOF                  bool
}

func newCBCEncrypterReader(key []byte, iv []byte, innerReadCloser io.ReadCloser) (io.ReadCloser, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	mode := cipher.NewCBCEncrypter(block, iv)
	// @Note(ExtraBlocks): We add two extraBlocks for padding and buf extension in second ReadFull call
	const extraBlocks = 2
	return &cbcEncrypterReader{
		mode:                        mode,
		innerReadCloser:             innerReadCloser,
		internalSrc:                 make([]byte, aes.BlockSize*512),
		internalDst:                 make([]byte, aes.BlockSize*(512+extraBlocks)),
		bytesRemainingInInternalDst: 0,
		reachedEOF:                  false,
	}, nil
}

func (dr *cbcEncrypterReader) Read(p []byte) (int, error) {
	if dr.bytesRemainingInInternalDst > 0 {
		n := copy(p, dr.internalDst[:dr.bytesRemainingInInternalDst])
		copy(dr.internalDst, dr.internalDst[n:dr.bytesRemainingInInternalDst])
		dr.bytesRemainingInInternalDst -= int64(n)
		return n, nil
	}
	// Make sure that no more data can be read when reaching endOffset
	if dr.reachedEOF {
		return 0, io.EOF
	}

	var bytesRead int
	var err error
	// Read at least aes.BlockSize bytes
	bytesRead, err = io.ReadAtLeast(dr.innerReadCloser, dr.internalSrc, aes.BlockSize)
	if err == io.ErrUnexpectedEOF || err == io.EOF {
		paddedData, err := pkcs7padding.Pad(dr.internalSrc[:bytesRead], aes.BlockSize)
		if err != nil {
			return -1, err
		}
		dr.reachedEOF = true
		// @Note(ExtraBlocks): After this internalSrc may be bigger than 512*aes.Blocksize
		// We make sure that there is enough room in internalDst by adding two extra aes.Blocksize many bytes
		dr.internalSrc = paddedData
		bytesRead = len(paddedData)
	} else if err != nil {
		return bytesRead, err
	}

	// Always round up to the next full blockSize
	remainingBytesToReadForFullBlock := bytesRead % aes.BlockSize
	if remainingBytesToReadForFullBlock > 0 {
		if dr.reachedEOF {
			return -1, ErrPlaintextNotAMultipleOfBlockSize
		}
		buf := make([]byte, remainingBytesToReadForFullBlock)
		bytesRead2, err := io.ReadFull(dr.innerReadCloser, buf)
		if err == io.ErrUnexpectedEOF || err == io.EOF {
			bytesCopied := copy(dr.internalSrc[bytesRead:], buf[:bytesRead2])
			if bytesRead2 != bytesCopied {
				panic("cbcEncrypterReader(1): Copy did not copy all bytes")
			}
			bytesRead += bytesRead2

			// @Note(ExtraBlocks): After this internalSrc may be bigger than 512*aes.Blocksize
			// We make sure that there is enough room in internalDst by adding two extra aes.Blocksize many bytes
			paddedData, err := pkcs7padding.Pad(dr.internalSrc[:bytesRead], aes.BlockSize)
			if err != nil {
				return -1, err
			}
			dr.reachedEOF = true
			dr.internalSrc = paddedData
			bytesRead = len(paddedData)
		} else if err != nil {
			return bytesRead2, err
		} else {
			bytesCopied := copy(dr.internalSrc[bytesRead:], buf[:bytesRead2])
			if bytesRead2 != bytesCopied {
				panic("cbcEncrypterReader(2): Copy did not copy all bytes")
			}
			bytesRead += bytesRead2
		}
	}

	dr.mode.CryptBlocks(dr.internalDst[:bytesRead], dr.internalSrc[:bytesRead])
	dr.bytesRemainingInInternalDst += int64(bytesRead)

	n := copy(p, dr.internalDst[:dr.bytesRemainingInInternalDst])
	copy(dr.internalDst, dr.internalDst[n:dr.bytesRemainingInInternalDst])
	dr.bytesRemainingInInternalDst -= int64(n)
	return n, nil
}

func (dr *cbcEncrypterReader) Close() error {
	return dr.innerReadCloser.Close()
}

func (ebsm *legacyEncryptionBlobStoreMiddleware) PutBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId, reader io.Reader) error {
	ctx, span := ebsm.tracer.Start(ctx, "legacyEncryptionBlobStoreMiddleware.PutBlob")
	defer span.End()

	// Generate a random initialization vector
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return err
	}

	ciphertextReadCloser, err := newCBCEncrypterReader(ebsm.key, iv, io.NopCloser(reader))
	if err != nil {
		return err
	}

	ivReadCloser := ioutils.NewByteReadSeekCloser(iv)
	var ivAndCiphertextReadCloser io.ReadCloser = ioutils.NewMultiReadCloser(ivReadCloser, ciphertextReadCloser)

	{
		readers, writer, closer := ioutils.PipeWriterIntoMultipleReaders(2)

		doneChan := make(chan struct{}, 1)
		errChan := make(chan error, 1)
		go func() {
			defer closer.Close()
			_, err := io.Copy(writer, ivAndCiphertextReadCloser)
			if err != nil {
				errChan <- err
				return
			}
			doneChan <- struct{}{}
		}()

		signatureReaderChan := make(chan io.ReadCloser, 1)
		errChan2 := make(chan error, 1)
		go func() {
			mac := hmac.New(sha256.New, ebsm.key)
			_, err := io.Copy(mac, readers[0])
			if err != nil {
				errChan2 <- err
				return
			}
			signature := mac.Sum([]byte{})
			signatureReaderChan <- ioutils.NewByteReadSeekCloser(signature)
		}()

		signatureReadCloser := ioutils.NewLazyReadCloser(func() (io.ReadCloser, error) {
			var readCloser io.ReadCloser
			select {
			case readCloser = <-signatureReaderChan:
			case err := <-errChan2:
				if err != nil {
					return nil, err
				}
			}
			return readCloser, nil
		})

		err = ebsm.innerBlobStore.PutBlob(ctx, tx, blobId, io.MultiReader(readers[1], signatureReadCloser))
		if err != nil {
			return err
		}

		select {
		case <-doneChan:
		case err := <-errChan:
			if err != nil {
				return err
			}
		}
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
			bytesCopied := copy(dr.internalSrc[bytesRead:], buf)
			if len(buf) != bytesCopied {
				panic("cbcDecrypterReader: Copy did not copy all bytes")
			}
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

func (ebsm *legacyEncryptionBlobStoreMiddleware) GetBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) (io.ReadCloser, error) {
	ctx, span := ebsm.tracer.Start(ctx, "legacyEncryptionBlobStoreMiddleware.GetBlob")
	defer span.End()

	var readCloser io.ReadCloser
	readCloser, err := ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
	if err != nil {
		return nil, err
	}
	var readCloser2 io.ReadCloser
	var readCloser3 io.ReadCloser
	var readCloser4 io.ReadCloser
	if _, ok := readCloser.(io.ReadSeekCloser); !ok {
		readCloser2, err = ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
		if err != nil {
			readCloser.Close()
			return nil, err
		}
		readCloser3, err = ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
		if err != nil {
			readCloser.Close()
			readCloser2.Close()
			return nil, err
		}
		readCloser4, err = ebsm.innerBlobStore.GetBlob(ctx, tx, blobId)
		if err != nil {
			readCloser.Close()
			readCloser2.Close()
			readCloser3.Close()
			return nil, err
		}
	}

	lazyReadCloser := ioutils.NewLazyReadCloser(func() (io.ReadCloser, error) {
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
			endOffset, err = ioutils.SkipAllBytes(readCloser2)
			if err != nil {
				return nil, err
			}
		}

		hmacOffset := endOffset - hmacSize
		if hmacOffset < aes.BlockSize {
			return nil, ErrCiphertextTooShort
		}

		var ciphertextMAC []byte
		if readSeekCloser, ok := readCloser.(io.ReadSeekCloser); ok {
			_, err = readSeekCloser.Seek(hmacOffset, io.SeekStart)
			if err != nil {
				return nil, err
			}
			ciphertextMAC, err = io.ReadAll(readSeekCloser)
			if err != nil {
				return nil, err
			}
			_, err = readSeekCloser.Seek(0, io.SeekStart)
			if err != nil {
				return nil, err
			}

			ciphertext := ioutils.NewLimitedEndReadCloser(readSeekCloser, hmacOffset)

			valid, err := validHMAC(ciphertext, ciphertextMAC, ebsm.key)
			if err != nil {
				return nil, err
			}
			if !*valid {
				return nil, ErrInvalidHMAC
			}

			_, err = readSeekCloser.Seek(0, io.SeekStart)
			if err != nil {
				return nil, err
			}
		} else {
			_, err = ioutils.SkipNBytes(readCloser3, hmacOffset)
			if err != nil {
				return nil, err
			}
			ciphertextMAC, err = io.ReadAll(readCloser3)
			if err != nil {
				return nil, err
			}

			ciphertext := ioutils.NewLimitedEndReadCloser(readCloser4, hmacOffset)

			valid, err := validHMAC(ciphertext, ciphertextMAC, ebsm.key)
			if err != nil {
				return nil, err
			}
			if !*valid {
				return nil, ErrInvalidHMAC
			}
		}
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

	return ioutils.NewReadAndCallbackCloser(lazyReadCloser, func() error {
		if readCloser4 != nil {
			err := readCloser4.Close()
			if err != nil {
				return err
			}
		}
		if readCloser3 != nil {
			err := readCloser3.Close()
			if err != nil {
				return err
			}
		}
		if readCloser2 != nil {
			err := readCloser2.Close()
			if err != nil {
				return err
			}
		}
		if readCloser != nil {
			err := readCloser.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}), nil
}

func (ebsm *legacyEncryptionBlobStoreMiddleware) GetBlobIds(ctx context.Context, tx *sql.Tx) ([]blobstore.BlobId, error) {
	ctx, span := ebsm.tracer.Start(ctx, "legacyEncryptionBlobStoreMiddleware.GetBlobIds")
	defer span.End()

	return ebsm.innerBlobStore.GetBlobIds(ctx, tx)
}

func (ebsm *legacyEncryptionBlobStoreMiddleware) DeleteBlob(ctx context.Context, tx *sql.Tx, blobId blobstore.BlobId) error {
	ctx, span := ebsm.tracer.Start(ctx, "legacyEncryptionBlobStoreMiddleware.DeleteBlob")
	defer span.End()

	return ebsm.innerBlobStore.DeleteBlob(ctx, tx, blobId)
}
