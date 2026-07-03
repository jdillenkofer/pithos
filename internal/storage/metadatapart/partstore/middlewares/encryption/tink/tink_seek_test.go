package tink

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/jdillenkofer/pithos/internal/ioutils"
	"github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore"
	filesystemPartStore "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/filesystem"
	compressionPartStoreMiddleware "github.com/jdillenkofer/pithos/internal/storage/metadatapart/partstore/middlewares/compression"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetPartSeeksOverSeekableInnerStore verifies the middleware's GetPart
// returns a seekable reader when the inner store supports it, and that
// seeking mid-part yields the same bytes as a sequential read+skip.
func TestGetPartSeeksOverSeekableInnerStore(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()

	storagePath := t.TempDir()
	inner, err := filesystemPartStore.New(storagePath)
	require.NoError(t, err)

	middleware, err := NewWithLocalKMS("test-password", inner, nil)
	require.NoError(t, err)
	require.NoError(t, middleware.Start(ctx))
	defer middleware.Stop(ctx)

	// Spans several segments at the default segment size.
	content := make([]byte, 3*DefaultSegmentSize+12345)
	_, err = rand.Read(content)
	require.NoError(t, err)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, middleware.PutPart(ctx, nil, *partId, bytes.NewReader(content)))

	rc, err := middleware.GetPart(ctx, nil, *partId)
	require.NoError(t, err)
	defer rc.Close()

	_, isSeeker := rc.(io.Seeker)
	require.True(t, isSeeker, "GetPart over a seekable inner store must return a seekable reader")

	// SkipNBytes should take the Seek path; verify the bytes afterwards match.
	skip := int64(2*DefaultSegmentSize + 777)
	_, err = ioutils.SkipNBytes(rc, skip)
	require.NoError(t, err)
	rest, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(content[skip:], rest), "post-seek bytes must match plaintext")

	// A fresh reader must still stream the whole part correctly.
	rc2, err := middleware.GetPart(ctx, nil, *partId)
	require.NoError(t, err)
	defer rc2.Close()
	all, err := io.ReadAll(rc2)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(content, all))
}

// TestGetPartSeeksOverCompressionMiddleware covers the composition used by
// the integration tests (tink over compression over filesystem). Tink
// ciphertext is incompressible, so the compression middleware stores it
// verbatim and its GetPart returns the raw part file positioned past the
// compression header — a seekable stream that does NOT begin at file offset
// 0. The seekable decrypting reader must honor that starting position.
func TestGetPartSeeksOverCompressionMiddleware(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()

	storagePath := t.TempDir()
	fs, err := filesystemPartStore.New(storagePath)
	require.NoError(t, err)
	inner, err := compressionPartStoreMiddleware.New(fs)
	require.NoError(t, err)

	middleware, err := NewWithLocalKMS("test-password", inner, nil)
	require.NoError(t, err)
	require.NoError(t, middleware.Start(ctx))
	defer middleware.Stop(ctx)

	content := make([]byte, 2*DefaultSegmentSize+4321)
	_, err = rand.Read(content)
	require.NoError(t, err)

	partId, err := partstore.NewRandomPartId()
	require.NoError(t, err)
	require.NoError(t, middleware.PutPart(ctx, nil, *partId, bytes.NewReader(content)))

	// Sequential read of the whole part.
	rc, err := middleware.GetPart(ctx, nil, *partId)
	require.NoError(t, err)
	all, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.True(t, bytes.Equal(content, all), "full read must match plaintext")

	// Range read: skip into a later segment, as UploadPartCopy and ranged
	// GETs do.
	rc2, err := middleware.GetPart(ctx, nil, *partId)
	require.NoError(t, err)
	defer rc2.Close()
	skip := int64(DefaultSegmentSize + 99)
	_, err = ioutils.SkipNBytes(rc2, skip)
	require.NoError(t, err)
	rest, err := io.ReadAll(rc2)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(content[skip:], rest), "post-seek bytes must match plaintext")
}
