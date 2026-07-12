// Package tapetest provides a behavior test suite that any tape.Device
// implementation must pass.
package tapetest

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/jdillenkofer/pithos/internal/tape"
)

const readBufferSize = 1 << 20

// RunDeviceSuite runs the shared tape.Device behavior suite. open must
// return a device positioned at the beginning of the tape; the medium
// content beyond that is unspecified, every subtest writes its own layout
// from there (writing erases everything after the written data, so the
// suite never depends on a blank medium).
func RunDeviceSuite(t *testing.T, open func(t *testing.T) tape.Device) {
	ctx := context.Background()

	record := func(size int, fill byte) []byte {
		return bytes.Repeat([]byte{fill}, size)
	}

	writeRecords := func(t *testing.T, dev tape.Device, records ...[]byte) {
		t.Helper()
		for _, r := range records {
			require.NoError(t, dev.WriteRecord(ctx, r))
		}
	}

	readRecord := func(t *testing.T, dev tape.Device) []byte {
		t.Helper()
		buf := make([]byte, readBufferSize)
		n, err := dev.ReadRecord(ctx, buf)
		require.NoError(t, err)
		return buf[:n]
	}

	requireBlock := func(t *testing.T, dev tape.Device, block uint64) {
		t.Helper()
		pos, err := dev.Tell(ctx)
		require.NoError(t, err)
		require.Equal(t, block, pos.Block)
	}

	t.Run("WriteReadRoundtrip", func(t *testing.T) {
		dev := open(t)
		records := [][]byte{record(1, 'a'), record(100, 'b'), record(64<<10, 'c')}
		writeRecords(t, dev, records...)
		require.NoError(t, dev.Rewind(ctx))
		for _, expected := range records {
			require.Equal(t, expected, readRecord(t, dev))
		}
		_, err := dev.ReadRecord(ctx, make([]byte, readBufferSize))
		require.ErrorIs(t, err, tape.ErrEndOfData)
	})

	t.Run("FilemarkSemantics", func(t *testing.T) {
		dev := open(t)
		recordA := record(10, 'a')
		recordB := record(20, 'b')
		writeRecords(t, dev, recordA)
		require.NoError(t, dev.WriteFilemarks(ctx, 1))
		writeRecords(t, dev, recordB)
		require.NoError(t, dev.WriteFilemarks(ctx, 2))
		require.NoError(t, dev.Rewind(ctx))

		buf := make([]byte, readBufferSize)
		require.Equal(t, recordA, readRecord(t, dev))
		_, err := dev.ReadRecord(ctx, buf)
		require.ErrorIs(t, err, tape.ErrFilemark)
		require.Equal(t, recordB, readRecord(t, dev))
		_, err = dev.ReadRecord(ctx, buf)
		require.ErrorIs(t, err, tape.ErrFilemark)
		_, err = dev.ReadRecord(ctx, buf)
		require.ErrorIs(t, err, tape.ErrFilemark)
		_, err = dev.ReadRecord(ctx, buf)
		require.ErrorIs(t, err, tape.ErrEndOfData)
	})

	t.Run("OverwriteErasesTail", func(t *testing.T) {
		dev := open(t)
		recordA := record(10, 'a')
		recordD := record(40, 'd')
		writeRecords(t, dev, recordA, record(20, 'b'), record(30, 'c'))
		require.NoError(t, dev.LocateBlock(ctx, 1))
		writeRecords(t, dev, recordD)
		requireBlock(t, dev, 2)

		require.NoError(t, dev.Rewind(ctx))
		require.Equal(t, recordA, readRecord(t, dev))
		require.Equal(t, recordD, readRecord(t, dev))
		_, err := dev.ReadRecord(ctx, make([]byte, readBufferSize))
		require.ErrorIs(t, err, tape.ErrEndOfData)
	})

	t.Run("LocateAndTell", func(t *testing.T) {
		dev := open(t)
		records := [][]byte{record(10, 'a'), record(20, 'b'), record(30, 'c'), record(40, 'd')}
		writeRecords(t, dev, records...)
		requireBlock(t, dev, 4)

		require.NoError(t, dev.LocateBlock(ctx, 2))
		requireBlock(t, dev, 2)
		require.Equal(t, records[2], readRecord(t, dev))

		err := dev.LocateBlock(ctx, 10)
		require.ErrorIs(t, err, tape.ErrEndOfData)
	})

	t.Run("SpaceRecords", func(t *testing.T) {
		dev := open(t)
		recordC := record(30, 'c')
		writeRecords(t, dev, record(10, 'a'), record(20, 'b'))
		require.NoError(t, dev.WriteFilemarks(ctx, 1))
		writeRecords(t, dev, recordC)
		require.NoError(t, dev.Rewind(ctx))

		require.NoError(t, dev.SpaceRecords(ctx, 2))
		requireBlock(t, dev, 2)

		// Crossing the filemark stops just past it.
		err := dev.SpaceRecords(ctx, 2)
		require.ErrorIs(t, err, tape.ErrFilemark)
		requireBlock(t, dev, 3)
		require.Equal(t, recordC, readRecord(t, dev))

		// Backward across the record, stopping just past the filemark.
		require.NoError(t, dev.SpaceRecords(ctx, -1))
		requireBlock(t, dev, 3)
		err = dev.SpaceRecords(ctx, -1)
		require.ErrorIs(t, err, tape.ErrFilemark)
		requireBlock(t, dev, 2)

		err = dev.SpaceRecords(ctx, -5)
		require.ErrorIs(t, err, tape.ErrBeginningOfTape)
		requireBlock(t, dev, 0)
	})

	t.Run("SpaceFilemarks", func(t *testing.T) {
		dev := open(t)
		recordB := record(20, 'b')
		writeRecords(t, dev, record(10, 'a'))
		require.NoError(t, dev.WriteFilemarks(ctx, 1))
		writeRecords(t, dev, recordB)
		require.NoError(t, dev.WriteFilemarks(ctx, 1))
		writeRecords(t, dev, record(30, 'c'))
		require.NoError(t, dev.Rewind(ctx))

		require.NoError(t, dev.SpaceFilemarks(ctx, 1))
		requireBlock(t, dev, 2)
		require.Equal(t, recordB, readRecord(t, dev))

		require.NoError(t, dev.SpaceFilemarks(ctx, 1))
		requireBlock(t, dev, 4)

		err := dev.SpaceFilemarks(ctx, 1)
		require.ErrorIs(t, err, tape.ErrEndOfData)
		requireBlock(t, dev, 5)

		// Backward stops on the near side of the filemark: reading next
		// returns the filemark again.
		require.NoError(t, dev.SpaceFilemarks(ctx, -2))
		requireBlock(t, dev, 1)
		_, err = dev.ReadRecord(ctx, make([]byte, readBufferSize))
		require.ErrorIs(t, err, tape.ErrFilemark)
		require.Equal(t, recordB, readRecord(t, dev))

		err = dev.SpaceFilemarks(ctx, -2)
		require.ErrorIs(t, err, tape.ErrBeginningOfTape)
		requireBlock(t, dev, 0)
	})

	t.Run("SeekToEODAndStatus", func(t *testing.T) {
		dev := open(t)
		writeRecords(t, dev, record(10, 'a'))
		require.NoError(t, dev.WriteFilemarks(ctx, 1))
		writeRecords(t, dev, record(20, 'b'))
		require.NoError(t, dev.Rewind(ctx))

		status, err := dev.Status(ctx)
		require.NoError(t, err)
		require.True(t, status.Online)
		require.True(t, status.AtBOT)
		require.Equal(t, int64(0), status.FileNumber)

		require.NoError(t, dev.SeekToEOD(ctx))
		requireBlock(t, dev, 3)
		status, err = dev.Status(ctx)
		require.NoError(t, err)
		require.True(t, status.AtEOD)
		require.False(t, status.AtBOT)
		require.Equal(t, int64(1), status.FileNumber)

		// Appending at end-of-data preserves what is already on the tape.
		writeRecords(t, dev, record(30, 'c'))
		requireBlock(t, dev, 4)
	})

	t.Run("ShortBuffer", func(t *testing.T) {
		dev := open(t)
		recordA := record(100, 'a')
		writeRecords(t, dev, recordA)
		require.NoError(t, dev.Rewind(ctx))

		_, err := dev.ReadRecord(ctx, make([]byte, 10))
		require.ErrorIs(t, err, io.ErrShortBuffer)

		// The position after a short read is implementation-defined;
		// re-locate before reading again.
		require.NoError(t, dev.LocateBlock(ctx, 0))
		require.Equal(t, recordA, readRecord(t, dev))
	})

	t.Run("ClosedDevice", func(t *testing.T) {
		dev := open(t)
		require.NoError(t, dev.Close())
		require.ErrorIs(t, dev.WriteRecord(ctx, record(1, 'a')), tape.ErrClosed)
		_, err := dev.ReadRecord(ctx, make([]byte, 1))
		require.ErrorIs(t, err, tape.ErrClosed)
		require.ErrorIs(t, dev.Rewind(ctx), tape.ErrClosed)
		_, err = dev.Status(ctx)
		require.ErrorIs(t, err, tape.ErrClosed)
	})
}
