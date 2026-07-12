package simulator

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jdillenkofer/pithos/internal/tape"
	"github.com/jdillenkofer/pithos/internal/tape/tapetest"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

func openTestDevice(t *testing.T, path string, opts Options) *Device {
	t.Helper()
	dev, err := Open(context.Background(), path, opts)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, dev.Close())
	})
	return dev
}

func TestSimulatorDeviceSuite(t *testing.T) {
	testutils.SkipIfIntegration(t)
	tapetest.RunDeviceSuite(t, func(t *testing.T) tape.Device {
		return openTestDevice(t, filepath.Join(t.TempDir(), "tape.sim"), Options{})
	})
}

func TestReopenRebuildsIndex(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tape.sim")
	recordA := bytes.Repeat([]byte{'a'}, 100)
	recordB := bytes.Repeat([]byte{'b'}, 200)

	dev, err := Open(ctx, path, Options{})
	require.NoError(t, err)
	require.NoError(t, dev.WriteRecord(ctx, recordA))
	require.NoError(t, dev.WriteFilemarks(ctx, 1))
	require.NoError(t, dev.WriteRecord(ctx, recordB))
	require.NoError(t, dev.Close())

	dev = openTestDevice(t, path, Options{})
	status, err := dev.Status(ctx)
	require.NoError(t, err)
	require.True(t, status.AtBOT)

	buf := make([]byte, 1024)
	n, err := dev.ReadRecord(ctx, buf)
	require.NoError(t, err)
	require.Equal(t, recordA, buf[:n])
	_, err = dev.ReadRecord(ctx, buf)
	require.ErrorIs(t, err, tape.ErrFilemark)
	n, err = dev.ReadRecord(ctx, buf)
	require.NoError(t, err)
	require.Equal(t, recordB, buf[:n])
	_, err = dev.ReadRecord(ctx, buf)
	require.ErrorIs(t, err, tape.ErrEndOfData)
}

func TestTornTailIsTreatedAsEndOfData(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tape.sim")
	recordA := bytes.Repeat([]byte{'a'}, 100)

	dev, err := Open(ctx, path, Options{})
	require.NoError(t, err)
	require.NoError(t, dev.WriteRecord(ctx, recordA))
	require.NoError(t, dev.Close())

	// Simulate a torn write: a record frame that announces more payload
	// than the file contains.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = f.Write([]byte{0x40, 0x00, 0x00, 0x00, 'x', 'y'})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	dev = openTestDevice(t, path, Options{})
	buf := make([]byte, 1024)
	n, err := dev.ReadRecord(ctx, buf)
	require.NoError(t, err)
	require.Equal(t, recordA, buf[:n])
	_, err = dev.ReadRecord(ctx, buf)
	require.ErrorIs(t, err, tape.ErrEndOfData)

	// Writing at end-of-data replaces the torn tail with the new record.
	recordB := bytes.Repeat([]byte{'b'}, 50)
	require.NoError(t, dev.WriteRecord(ctx, recordB))
	require.NoError(t, dev.Rewind(ctx))
	n, err = dev.ReadRecord(ctx, buf)
	require.NoError(t, err)
	require.Equal(t, recordA, buf[:n])
	n, err = dev.ReadRecord(ctx, buf)
	require.NoError(t, err)
	require.Equal(t, recordB, buf[:n])
}

func TestCapacityExhaustionReturnsEndOfTape(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	dev := openTestDevice(t, filepath.Join(t.TempDir(), "tape.sim"), Options{Capacity: 1000})
	require.Equal(t, int64(1000), dev.Capacity())

	require.NoError(t, dev.WriteRecord(ctx, make([]byte, 600)))
	err := dev.WriteRecord(ctx, make([]byte, 600))
	require.ErrorIs(t, err, tape.ErrEndOfTape)
	require.NoError(t, dev.WriteRecord(ctx, make([]byte, 400)))

	// Overwriting from the beginning frees the erased payload.
	require.NoError(t, dev.Rewind(ctx))
	require.NoError(t, dev.WriteRecord(ctx, make([]byte, 900)))
}

func TestCapacityIsAPropertyOfTheMedium(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tape.sim")

	dev, err := Open(ctx, path, Options{Capacity: 1000})
	require.NoError(t, err)
	require.NoError(t, dev.Close())

	// Reopening without a capacity keeps the one stored in the header.
	dev = openTestDevice(t, path, Options{})
	require.Equal(t, int64(1000), dev.Capacity())
}

func TestReadOnlyBehavesLikeWriteProtectedCartridge(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "tape.sim")
	recordA := bytes.Repeat([]byte{'a'}, 100)

	dev, err := Open(ctx, path, Options{})
	require.NoError(t, err)
	require.NoError(t, dev.WriteRecord(ctx, recordA))
	require.NoError(t, dev.Close())

	dev = openTestDevice(t, path, Options{ReadOnly: true})
	status, err := dev.Status(ctx)
	require.NoError(t, err)
	require.True(t, status.WriteProtected)

	require.ErrorIs(t, dev.WriteRecord(ctx, recordA), tape.ErrWriteProtected)
	require.ErrorIs(t, dev.WriteFilemarks(ctx, 1), tape.ErrWriteProtected)

	buf := make([]byte, 1024)
	n, err := dev.ReadRecord(ctx, buf)
	require.NoError(t, err)
	require.Equal(t, recordA, buf[:n])
}

func TestZeroLengthWriteWritesNoRecord(t *testing.T) {
	testutils.SkipIfIntegration(t)
	ctx := context.Background()
	dev := openTestDevice(t, filepath.Join(t.TempDir(), "tape.sim"), Options{})

	require.NoError(t, dev.WriteRecord(ctx, nil))
	pos, err := dev.Tell(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(0), pos.Block)
}
