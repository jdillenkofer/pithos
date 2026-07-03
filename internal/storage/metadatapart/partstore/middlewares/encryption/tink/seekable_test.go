package tink

import (
	"bytes"
	"crypto/rand"
	"io"
	mathrand "math/rand"
	"testing"

	streamingaeadsubtle "github.com/google/tink/go/streamingaead/subtle"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// encryptWithTink produces ciphertext exactly as the middleware's PutPart
// does: via tink-go's streaming AEAD writer.
func encryptWithTink(t *testing.T, key, aad, plaintext []byte, segmentSize int) []byte {
	t.Helper()
	aead, err := streamingaeadsubtle.NewAESGCMHKDF(key, "SHA256", 32, segmentSize, 0)
	require.NoError(t, err)
	var buf bytes.Buffer
	w, err := aead.NewEncryptingWriter(&buf, aad)
	require.NoError(t, err)
	_, err = w.Write(plaintext)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

func TestSeekableDecryptingReaderMatchesTink(t *testing.T) {
	testutils.SkipIfIntegration(t)

	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	aad := []byte("part-id-aad")

	segmentSizes := []int{LegacySegmentSize, 128 * 1024, DefaultSegmentSize}
	pss := func(css int) int { return css - tinkTagSize }

	for _, css := range segmentSizes {
		firstPss := pss(css) - 40
		plaintextSizes := []int{
			0, 1, 100,
			firstPss - 1, firstPss, firstPss + 1,
			firstPss + pss(css) - 1, firstPss + pss(css), firstPss + pss(css) + 1,
			3*pss(css) + 17,
		}
		for _, size := range plaintextSizes {
			plaintext := make([]byte, size)
			_, err := rand.Read(plaintext)
			require.NoError(t, err)

			ciphertext := encryptWithTink(t, key, aad, plaintext, css)

			r, err := newSeekableDecryptingReader(bytes.NewReader(ciphertext), 0, key, aad, css)
			require.NoError(t, err, "css=%d size=%d", css, size)
			assert.Equal(t, int64(size), r.plaintextLen, "css=%d size=%d", css, size)

			// Full sequential read must match.
			got, err := io.ReadAll(r)
			require.NoError(t, err, "css=%d size=%d", css, size)
			assert.True(t, bytes.Equal(plaintext, got), "full read mismatch css=%d size=%d", css, size)

			// Random seeks must match the corresponding plaintext slice.
			rng := mathrand.New(mathrand.NewSource(int64(css) + int64(size)))
			for i := 0; i < 20 && size > 0; i++ {
				off := rng.Int63n(int64(size))
				_, err := r.Seek(off, io.SeekStart)
				require.NoError(t, err)
				want := plaintext[off:min(int(off)+1024, size)]
				got := make([]byte, len(want))
				_, err = io.ReadFull(r, got)
				require.NoError(t, err, "css=%d size=%d off=%d", css, size, off)
				assert.True(t, bytes.Equal(want, got), "seek read mismatch css=%d size=%d off=%d", css, size, off)
			}
		}
	}
}

func TestSeekableDecryptingReaderNonZeroBase(t *testing.T) {
	testutils.SkipIfIntegration(t)

	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	aad := []byte("aad")
	plaintext := make([]byte, 3*4096+123)
	_, err = rand.Read(plaintext)
	require.NoError(t, err)

	ciphertext := encryptWithTink(t, key, aad, plaintext, LegacySegmentSize)
	prefix := []byte("json-part-header-prefix")
	stream := append(append([]byte(nil), prefix...), ciphertext...)

	r, err := newSeekableDecryptingReader(bytes.NewReader(stream), int64(len(prefix)), key, aad, LegacySegmentSize)
	require.NoError(t, err)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(plaintext, got))

	// Seek relative to end.
	_, err = r.Seek(-100, io.SeekEnd)
	require.NoError(t, err)
	tail, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(plaintext[len(plaintext)-100:], tail))
}

func TestSeekableDecryptingReaderRejectsTamperedSegment(t *testing.T) {
	testutils.SkipIfIntegration(t)

	key := make([]byte, 32)
	_, err := rand.Read(key)
	require.NoError(t, err)
	aad := []byte("aad")
	plaintext := make([]byte, 2*4096)
	_, err = rand.Read(plaintext)
	require.NoError(t, err)

	ciphertext := encryptWithTink(t, key, aad, plaintext, LegacySegmentSize)
	// Flip a bit in the second segment.
	ciphertext[LegacySegmentSize+10] ^= 0x01

	r, err := newSeekableDecryptingReader(bytes.NewReader(ciphertext), 0, key, aad, LegacySegmentSize)
	require.NoError(t, err)
	_, err = io.ReadAll(r)
	assert.Error(t, err)
}
