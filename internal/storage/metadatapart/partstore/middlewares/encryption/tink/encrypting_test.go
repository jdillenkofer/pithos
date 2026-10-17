package tink

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"testing"
	"testing/iotest"

	streamingaeadsubtle "github.com/google/tink/go/streamingaead/subtle"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEncryptingReaderMatchesTinkFormat locks in that ciphertext produced by
// encryptingReader decrypts with tink-go's own reader and with the seekable
// reader, and has exactly the length tink-go's writer would produce, around
// every segment boundary.
func TestEncryptingReaderMatchesTinkFormat(t *testing.T) {
	testutils.SkipIfIntegration(t)

	mainKey := make([]byte, 32)
	_, err := rand.Read(mainKey)
	require.NoError(t, err)
	aad := []byte("part-id")
	prefix := []byte("prefix")

	for _, css := range []int{LegacySegmentSize, DefaultSegmentSize} {
		tinkHeaderLen := 1 + tinkKeySize + tinkNoncePrefixSize
		pss := css - tinkTagSize
		firstPss := pss - tinkHeaderLen
		sizes := []int{0, 1, firstPss - 1, firstPss, firstPss + 1, firstPss + pss, firstPss + pss + 1, firstPss + 3*pss - 1, 3*css + 777}

		aead, err := streamingaeadsubtle.NewAESGCMHKDF(mainKey, "SHA256", tinkKeySize, css, 0)
		require.NoError(t, err)

		for _, size := range sizes {
			for _, oneByte := range []bool{false, true} {
				t.Run(fmt.Sprintf("css=%d/size=%d/oneByte=%v", css, size, oneByte), func(t *testing.T) {
					plaintext := make([]byte, size)
					_, err := rand.Read(plaintext)
					require.NoError(t, err)

					var src io.Reader = bytes.NewReader(plaintext)
					if oneByte {
						src = iotest.OneByteReader(src)
					}
					r, err := newEncryptingReader(src, prefix, mainKey, aad, css)
					require.NoError(t, err)
					var out []byte
					if oneByte {
						out, err = io.ReadAll(iotest.OneByteReader(r))
					} else {
						var buf bytes.Buffer
						_, err = io.Copy(&buf, r)
						out = buf.Bytes()
					}
					require.NoError(t, err)
					require.True(t, bytes.HasPrefix(out, prefix))
					ciphertext := out[len(prefix):]

					var tinkOut bytes.Buffer
					w, err := aead.NewEncryptingWriter(&tinkOut, aad)
					require.NoError(t, err)
					_, err = w.Write(plaintext)
					require.NoError(t, err)
					require.NoError(t, w.Close())
					assert.Equal(t, tinkOut.Len(), len(ciphertext), "ciphertext length must match tink-go's writer")

					dr, err := aead.NewDecryptingReader(bytes.NewReader(ciphertext), aad)
					require.NoError(t, err)
					decrypted, err := io.ReadAll(dr)
					require.NoError(t, err)
					assert.True(t, bytes.Equal(plaintext, decrypted), "tink-go reader must decrypt")

					sr, err := newSeekableDecryptingReader(bytes.NewReader(out), int64(len(prefix)), mainKey, aad, css)
					require.NoError(t, err)
					decrypted, err = io.ReadAll(sr)
					require.NoError(t, err)
					assert.True(t, bytes.Equal(plaintext, decrypted), "seekable reader must decrypt")
				})
			}
		}
	}
}

func TestEncryptingReaderPropagatesSourceErrors(t *testing.T) {
	testutils.SkipIfIntegration(t)

	mainKey := make([]byte, 32)
	srcErr := errors.New("source failed")
	src := io.MultiReader(bytes.NewReader(make([]byte, 1000)), iotest.ErrReader(srcErr))
	r, err := newEncryptingReader(src, nil, mainKey, nil, DefaultSegmentSize)
	require.NoError(t, err)
	_, err = io.ReadAll(r)
	assert.ErrorIs(t, err, srcErr)
	_, err = r.Read(make([]byte, 10))
	assert.ErrorIs(t, err, srcErr)
}
