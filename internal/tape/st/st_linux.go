//go:build linux && (amd64 || arm64)

// Package st implements tape.Device for Linux SCSI tape devices driven by
// the st driver (e.g. /dev/nst0). Use the non-rewinding device node: the
// rewinding nodes (/dev/st0) rewind on close, which silently repositions
// the head.
package st

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"
	"unsafe"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sys/unix"

	"github.com/jdillenkofer/pithos/internal/tape"
)

type stDevice struct {
	mu       sync.Mutex
	fd       int
	path     string
	readOnly bool
	closed   bool
	tracer   trace.Tracer
}

// Compile-time check to ensure stDevice implements tape.Device
var _ tape.Device = (*stDevice)(nil)

// Open opens the tape device at path (e.g. /dev/nst0) and puts it into
// variable block mode. A write-protected medium is opened read-only.
func Open(path string) (tape.Device, error) {
	fd, err := unix.Open(path, unix.O_RDWR, 0)
	readOnly := false
	if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EROFS) {
		fd, err = unix.Open(path, unix.O_RDONLY, 0)
		readOnly = true
	}
	if err != nil {
		return nil, fmt.Errorf("opening tape device %s: %w", path, err)
	}
	d := &stDevice{
		fd:       fd,
		path:     path,
		readOnly: readOnly,
		tracer:   otel.Tracer("internal/tape/st"),
	}
	if err := d.ioctlOp(mtSETBLK, 0); err != nil {
		unix.Close(fd)
		return nil, fmt.Errorf("setting variable block mode on %s: %w", path, err)
	}
	return d, nil
}

func (d *stDevice) ioctlOp(op int16, count int32) error {
	arg := mtOp{Op: op, Count: count}
	return ioctlPtr(d.fd, mtiocTop, unsafe.Pointer(&arg))
}

func ioctlPtr(fd int, req uintptr, arg unsafe.Pointer) error {
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), req, uintptr(arg))
	if errno != 0 {
		return errno
	}
	return nil
}

func (d *stDevice) checkOpen(ctx context.Context) error {
	if d.closed {
		return tape.ErrClosed
	}
	return ctx.Err()
}

// gstat returns the generalized status bits, or 0 if they cannot be read.
func (d *stDevice) gstat() int64 {
	var g mtGet
	if err := ioctlPtr(d.fd, mtiocGet, unsafe.Pointer(&g)); err != nil {
		return 0
	}
	return g.Gstat
}

func (d *stDevice) WriteRecord(ctx context.Context, p []byte) error {
	_, span := d.tracer.Start(ctx, "stDevice.WriteRecord")
	defer span.End()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkOpen(ctx); err != nil {
		return err
	}
	if d.readOnly {
		return tape.ErrWriteProtected
	}
	if len(p) == 0 {
		// write(2) of zero bytes writes no record.
		return nil
	}
	n, err := unix.Write(d.fd, p)
	if err != nil {
		switch {
		case errors.Is(err, unix.ENOSPC):
			return tape.ErrEndOfTape
		case errors.Is(err, unix.EACCES), errors.Is(err, unix.EROFS):
			return tape.ErrWriteProtected
		}
		return fmt.Errorf("writing record to %s: %w", d.path, err)
	}
	if n != len(p) {
		return fmt.Errorf("short record write to %s: %d of %d bytes", d.path, n, len(p))
	}
	return nil
}

func (d *stDevice) ReadRecord(ctx context.Context, p []byte) (int, error) {
	_, span := d.tracer.Start(ctx, "stDevice.ReadRecord")
	defer span.End()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkOpen(ctx); err != nil {
		return 0, err
	}
	if len(p) == 0 {
		return 0, io.ErrShortBuffer
	}
	n, err := unix.Read(d.fd, p)
	if err != nil {
		switch {
		case errors.Is(err, unix.EOVERFLOW), errors.Is(err, unix.ENOMEM):
			// The record was larger than the buffer; the head has moved
			// past it.
			return 0, io.ErrShortBuffer
		case errors.Is(err, unix.EIO) && d.gstat()&gmtEOD != 0:
			return 0, tape.ErrEndOfData
		}
		return 0, fmt.Errorf("reading record from %s: %w", d.path, err)
	}
	if n == 0 {
		// A zero-length read means a filemark was crossed, unless the
		// status says we are in the blank region after the data.
		gstat := d.gstat()
		if gstat&gmtEOF != 0 {
			return 0, tape.ErrFilemark
		}
		return 0, tape.ErrEndOfData
	}
	return n, nil
}

func (d *stDevice) WriteFilemarks(ctx context.Context, count int) error {
	_, span := d.tracer.Start(ctx, "stDevice.WriteFilemarks")
	defer span.End()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkOpen(ctx); err != nil {
		return err
	}
	if d.readOnly {
		return tape.ErrWriteProtected
	}
	if count < 0 {
		return fmt.Errorf("negative filemark count %d", count)
	}
	if count == 0 {
		return nil
	}
	if count > math.MaxInt32 {
		return fmt.Errorf("filemark count %d out of range", count)
	}
	if err := d.ioctlOp(mtWEOF, int32(count)); err != nil {
		return fmt.Errorf("writing %d filemarks to %s: %w", count, d.path, err)
	}
	return nil
}

func (d *stDevice) Rewind(ctx context.Context) error {
	_, span := d.tracer.Start(ctx, "stDevice.Rewind")
	defer span.End()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkOpen(ctx); err != nil {
		return err
	}
	if err := d.ioctlOp(mtREW, 1); err != nil {
		return fmt.Errorf("rewinding %s: %w", d.path, err)
	}
	return nil
}

func (d *stDevice) LocateBlock(ctx context.Context, block uint64) error {
	_, span := d.tracer.Start(ctx, "stDevice.LocateBlock")
	defer span.End()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkOpen(ctx); err != nil {
		return err
	}
	if block > math.MaxInt32 {
		return fmt.Errorf("block number %d out of range for MTSEEK", block)
	}
	if err := d.ioctlOp(mtSEEK, int32(block)); err != nil {
		if errors.Is(err, unix.EIO) && d.gstat()&gmtEOD != 0 {
			return tape.ErrEndOfData
		}
		return fmt.Errorf("locating block %d on %s: %w", block, d.path, err)
	}
	return nil
}

func (d *stDevice) Tell(ctx context.Context) (tape.Position, error) {
	_, span := d.tracer.Start(ctx, "stDevice.Tell")
	defer span.End()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkOpen(ctx); err != nil {
		return tape.Position{}, err
	}
	var pos mtPos
	if err := ioctlPtr(d.fd, mtiocPos, unsafe.Pointer(&pos)); err != nil {
		return tape.Position{}, fmt.Errorf("reading position of %s: %w", d.path, err)
	}
	return tape.Position{Block: uint64(pos.BlkNo)}, nil
}

func (d *stDevice) SpaceRecords(ctx context.Context, count int) error {
	_, span := d.tracer.Start(ctx, "stDevice.SpaceRecords")
	defer span.End()
	return d.space(ctx, mtFSR, mtBSR, count)
}

func (d *stDevice) SpaceFilemarks(ctx context.Context, count int) error {
	_, span := d.tracer.Start(ctx, "stDevice.SpaceFilemarks")
	defer span.End()
	return d.space(ctx, mtFSF, mtBSF, count)
}

func (d *stDevice) space(ctx context.Context, forwardOp, backwardOp int16, count int) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkOpen(ctx); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	op := forwardOp
	if count < 0 {
		op = backwardOp
		count = -count
	}
	if count > math.MaxInt32 {
		return fmt.Errorf("space count %d out of range", count)
	}
	if err := d.ioctlOp(op, int32(count)); err != nil {
		if errors.Is(err, unix.EIO) {
			gstat := d.gstat()
			switch {
			case gstat&gmtEOF != 0:
				return tape.ErrFilemark
			case gstat&gmtEOD != 0, gstat&gmtEOT != 0:
				return tape.ErrEndOfData
			case gstat&gmtBOT != 0:
				return tape.ErrBeginningOfTape
			}
		}
		return fmt.Errorf("spacing on %s: %w", d.path, err)
	}
	return nil
}

func (d *stDevice) SeekToEOD(ctx context.Context) error {
	_, span := d.tracer.Start(ctx, "stDevice.SeekToEOD")
	defer span.End()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkOpen(ctx); err != nil {
		return err
	}
	if err := d.ioctlOp(mtEOM, 1); err != nil {
		return fmt.Errorf("seeking to end of data on %s: %w", d.path, err)
	}
	return nil
}

func (d *stDevice) Status(ctx context.Context) (*tape.Status, error) {
	_, span := d.tracer.Start(ctx, "stDevice.Status")
	defer span.End()
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.checkOpen(ctx); err != nil {
		return nil, err
	}
	var g mtGet
	if err := ioctlPtr(d.fd, mtiocGet, unsafe.Pointer(&g)); err != nil {
		return nil, fmt.Errorf("reading status of %s: %w", d.path, err)
	}
	return &tape.Status{
		Online:         g.Gstat&gmtOnline != 0,
		WriteProtected: d.readOnly || g.Gstat&gmtWrProt != 0,
		AtBOT:          g.Gstat&gmtBOT != 0,
		AtEOD:          g.Gstat&gmtEOD != 0,
		FileNumber:     int64(g.FileNo),
		BlockNumber:    int64(g.BlkNo),
	}, nil
}

func (d *stDevice) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	return unix.Close(d.fd)
}
