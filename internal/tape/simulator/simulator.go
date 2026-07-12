// Package simulator provides a file-backed tape.Device implementation that
// behaves like a real tape drive: strictly sequential records and filemarks,
// writes that erase everything after the current position, and optionally
// simulated drive latency.
package simulator

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/jdillenkofer/pithos/internal/tape"
)

// Options configures a simulated tape device.
type Options struct {
	// Capacity is the payload capacity in bytes of a newly created tape
	// file; 0 means unlimited. For existing files the capacity stored in
	// the file header is used (capacity is a property of the medium).
	// WriteRecord returns tape.ErrEndOfTape once the capacity is exceeded.
	Capacity int64
	// Latency simulates drive timing; see LatencyProfile. The zero value
	// disables all simulated latency.
	Latency LatencyProfile
	// ReadOnly opens the tape file read-only, like a write-protected
	// cartridge; writes return tape.ErrWriteProtected.
	ReadOnly bool
}

// Device is a file-backed tape.Device. Operations serialize on an internal
// mutex, including simulated latency: concurrent callers queue behind the
// single head, as they would on a real drive.
type Device struct {
	f            *os.File
	index        []entry
	cursor       int
	fileSize     int64
	payloadBytes int64
	capacity     int64 // payload byte limit; 0 = unlimited
	readOnly     bool
	closed       bool
	latency      LatencyProfile
	// scaleCapacity is the denominator for distance-based latency scaling.
	scaleCapacity int64
	sleep         sleepFunc
	tracer        trace.Tracer
	sem           chan struct{}
}

// Compile-time check to ensure Device implements tape.Device
var _ tape.Device = (*Device)(nil)

// Open opens or creates the simulated tape file at path. The head is
// positioned at the beginning of the tape, and the profile's load time is
// charged.
func Open(ctx context.Context, path string, opts Options) (*Device, error) {
	return open(ctx, path, opts, contextSleep)
}

func open(ctx context.Context, path string, opts Options, sleep sleepFunc) (*Device, error) {
	flags := os.O_RDWR | os.O_CREATE
	if opts.ReadOnly {
		flags = os.O_RDONLY
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening tape file: %w", err)
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("stating tape file: %w", err)
	}
	fileSize := info.Size()

	var capacity int64
	var index []entry
	var payloadBytes int64
	if fileSize == 0 {
		if opts.Capacity < 0 {
			f.Close()
			return nil, fmt.Errorf("negative tape capacity %d", opts.Capacity)
		}
		if err := writeHeader(f, uint64(opts.Capacity)); err != nil {
			f.Close()
			return nil, fmt.Errorf("writing tape file header: %w", err)
		}
		capacity = opts.Capacity
		fileSize = headerSize
	} else {
		headerCapacity, err := readHeader(f)
		if err != nil {
			f.Close()
			return nil, err
		}
		capacity = int64(headerCapacity)
		index, payloadBytes = scanIndex(f, fileSize)
	}

	scaleCapacity := capacity
	if scaleCapacity == 0 {
		scaleCapacity = opts.Latency.NativeCapacity
		if scaleCapacity == 0 {
			scaleCapacity = defaultScaleCapacity
		}
	}
	d := &Device{
		f:             f,
		index:         index,
		cursor:        0,
		fileSize:      fileSize,
		payloadBytes:  payloadBytes,
		capacity:      capacity,
		readOnly:      opts.ReadOnly,
		latency:       opts.Latency,
		scaleCapacity: scaleCapacity,
		sleep:         sleep,
		tracer:        otel.Tracer("internal/tape/simulator"),
		sem:           make(chan struct{}, 1),
	}
	if err := d.sleep(ctx, d.latency.LoadTime); err != nil {
		f.Close()
		return nil, err
	}
	return d, nil
}

// lock acquires the device mutex, giving up when ctx is canceled (latency
// sleeps of queued operations can hold it for a long time).
func (d *Device) lock(ctx context.Context) error {
	select {
	case d.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *Device) unlock() {
	<-d.sem
}

func (d *Device) checkOpen() error {
	if d.closed {
		return tape.ErrClosed
	}
	return nil
}

// offsetOf returns the file offset corresponding to the given block index;
// block len(index) is the end-of-data position.
func (d *Device) offsetOf(block int) int64 {
	if block < len(d.index) {
		return d.index[block].offset
	}
	if len(d.index) == 0 {
		return headerSize
	}
	last := d.index[len(d.index)-1]
	return last.offset + last.byteLen()
}

// distanceFromBOT returns the head's physical distance from the beginning
// of the tape, used for latency scaling.
func (d *Device) distanceFromBOT() int64 {
	return d.offsetOf(d.cursor) - headerSize
}

// truncateAtCursor drops everything at and after the current position,
// including any torn tail beyond the last valid entry.
func (d *Device) truncateAtCursor() error {
	target := d.offsetOf(d.cursor)
	if d.fileSize == target && len(d.index) == d.cursor {
		return nil
	}
	if err := d.f.Truncate(target); err != nil {
		return fmt.Errorf("truncating tape file: %w", err)
	}
	for _, e := range d.index[d.cursor:] {
		if e.kind == entryRecord {
			d.payloadBytes -= int64(e.size)
		}
	}
	d.index = d.index[:d.cursor]
	d.fileSize = target
	return nil
}

// erasedPayload returns how many payload bytes a write at the current
// position would erase.
func (d *Device) erasedPayload() int64 {
	var erased int64
	for _, e := range d.index[d.cursor:] {
		if e.kind == entryRecord {
			erased += int64(e.size)
		}
	}
	return erased
}

func (d *Device) WriteRecord(ctx context.Context, p []byte) error {
	ctx, span := d.tracer.Start(ctx, "simulatorDevice.WriteRecord")
	defer span.End()
	if err := d.lock(ctx); err != nil {
		return err
	}
	defer d.unlock()
	if err := d.checkOpen(); err != nil {
		return err
	}
	if d.readOnly {
		return tape.ErrWriteProtected
	}
	if len(p) > maxRecordSize {
		return fmt.Errorf("record size %d exceeds maximum %d", len(p), maxRecordSize)
	}
	if len(p) == 0 {
		// Like write(2) on a tape device, a zero-length write writes no record.
		return nil
	}
	if d.capacity > 0 && d.payloadBytes-d.erasedPayload()+int64(len(p)) > d.capacity {
		return tape.ErrEndOfTape
	}
	if err := d.sleep(ctx, d.latency.transferCost(int64(len(p)), d.latency.WriteThroughput)); err != nil {
		return err
	}
	if err := d.truncateAtCursor(); err != nil {
		return err
	}
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(p)))
	offset := d.fileSize
	if _, err := d.f.WriteAt(lenBuf[:], offset); err != nil {
		return fmt.Errorf("writing record frame: %w", err)
	}
	if _, err := d.f.WriteAt(p, offset+4); err != nil {
		return fmt.Errorf("writing record payload: %w", err)
	}
	if _, err := d.f.WriteAt(lenBuf[:], offset+4+int64(len(p))); err != nil {
		return fmt.Errorf("writing record frame: %w", err)
	}
	d.index = append(d.index, entry{offset: offset, size: uint32(len(p)), kind: entryRecord})
	d.fileSize = offset + 8 + int64(len(p))
	d.payloadBytes += int64(len(p))
	d.cursor = len(d.index)
	return nil
}

func (d *Device) ReadRecord(ctx context.Context, p []byte) (int, error) {
	ctx, span := d.tracer.Start(ctx, "simulatorDevice.ReadRecord")
	defer span.End()
	if err := d.lock(ctx); err != nil {
		return 0, err
	}
	defer d.unlock()
	if err := d.checkOpen(); err != nil {
		return 0, err
	}
	if d.cursor >= len(d.index) {
		return 0, tape.ErrEndOfData
	}
	e := d.index[d.cursor]
	if e.kind == entryFilemark {
		d.cursor++
		return 0, tape.ErrFilemark
	}
	if int64(e.size) > int64(len(p)) {
		// The drive has already read the oversized record; the head moves
		// past it (st driver behavior in variable-block mode).
		d.cursor++
		return 0, io.ErrShortBuffer
	}
	if err := d.sleep(ctx, d.latency.transferCost(int64(e.size), d.latency.ReadThroughput)); err != nil {
		return 0, err
	}
	if _, err := d.f.ReadAt(p[:e.size], e.offset+4); err != nil {
		return 0, fmt.Errorf("reading record payload: %w", err)
	}
	d.cursor++
	return int(e.size), nil
}

func (d *Device) WriteFilemarks(ctx context.Context, count int) error {
	ctx, span := d.tracer.Start(ctx, "simulatorDevice.WriteFilemarks")
	defer span.End()
	if err := d.lock(ctx); err != nil {
		return err
	}
	defer d.unlock()
	if err := d.checkOpen(); err != nil {
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
	if err := d.sleep(ctx, d.latency.filemarkCost(count)); err != nil {
		return err
	}
	if err := d.truncateAtCursor(); err != nil {
		return err
	}
	buf := make([]byte, 8*count)
	for i := range buf {
		buf[i] = 0xFF
	}
	offset := d.fileSize
	if _, err := d.f.WriteAt(buf, offset); err != nil {
		return fmt.Errorf("writing filemarks: %w", err)
	}
	for i := range count {
		d.index = append(d.index, entry{offset: offset + int64(8*i), kind: entryFilemark})
	}
	d.fileSize = offset + int64(8*count)
	d.cursor = len(d.index)
	return nil
}

func (d *Device) Rewind(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "simulatorDevice.Rewind")
	defer span.End()
	if err := d.lock(ctx); err != nil {
		return err
	}
	defer d.unlock()
	if err := d.checkOpen(); err != nil {
		return err
	}
	if err := d.sleep(ctx, d.latency.rewindCost(d.distanceFromBOT(), d.scaleCapacity)); err != nil {
		return err
	}
	d.cursor = 0
	return nil
}

func (d *Device) LocateBlock(ctx context.Context, block uint64) error {
	ctx, span := d.tracer.Start(ctx, "simulatorDevice.LocateBlock")
	defer span.End()
	if err := d.lock(ctx); err != nil {
		return err
	}
	defer d.unlock()
	if err := d.checkOpen(); err != nil {
		return err
	}
	target := int(block)
	var resultErr error
	if block > uint64(len(d.index)) {
		// Locating past the recorded data stops at end-of-data.
		target = len(d.index)
		resultErr = tape.ErrEndOfData
	}
	if err := d.moveTo(ctx, target); err != nil {
		return err
	}
	return resultErr
}

func (d *Device) Tell(ctx context.Context) (tape.Position, error) {
	_, span := d.tracer.Start(ctx, "simulatorDevice.Tell")
	defer span.End()
	if err := d.lock(ctx); err != nil {
		return tape.Position{}, err
	}
	defer d.unlock()
	if err := d.checkOpen(); err != nil {
		return tape.Position{}, err
	}
	return tape.Position{Block: uint64(d.cursor)}, nil
}

func (d *Device) SpaceRecords(ctx context.Context, count int) error {
	ctx, span := d.tracer.Start(ctx, "simulatorDevice.SpaceRecords")
	defer span.End()
	if err := d.lock(ctx); err != nil {
		return err
	}
	defer d.unlock()
	if err := d.checkOpen(); err != nil {
		return err
	}
	target, resultErr := d.spaceRecordsTarget(count)
	if err := d.spaceTo(ctx, target); err != nil {
		return err
	}
	return resultErr
}

func (d *Device) spaceRecordsTarget(count int) (int, error) {
	c := d.cursor
	for count > 0 {
		if c >= len(d.index) {
			return c, tape.ErrEndOfData
		}
		crossed := d.index[c]
		c++
		count--
		if crossed.kind == entryFilemark {
			return c, tape.ErrFilemark
		}
	}
	for count < 0 {
		if c == 0 {
			return c, tape.ErrBeginningOfTape
		}
		c--
		count++
		if d.index[c].kind == entryFilemark {
			return c, tape.ErrFilemark
		}
	}
	return c, nil
}

func (d *Device) SpaceFilemarks(ctx context.Context, count int) error {
	ctx, span := d.tracer.Start(ctx, "simulatorDevice.SpaceFilemarks")
	defer span.End()
	if err := d.lock(ctx); err != nil {
		return err
	}
	defer d.unlock()
	if err := d.checkOpen(); err != nil {
		return err
	}
	target, resultErr := d.spaceFilemarksTarget(count)
	if err := d.spaceTo(ctx, target); err != nil {
		return err
	}
	return resultErr
}

func (d *Device) spaceFilemarksTarget(count int) (int, error) {
	c := d.cursor
	for count > 0 {
		if c >= len(d.index) {
			return c, tape.ErrEndOfData
		}
		crossed := d.index[c]
		c++
		if crossed.kind == entryFilemark {
			count--
		}
	}
	for count < 0 {
		if c == 0 {
			return c, tape.ErrBeginningOfTape
		}
		c--
		if d.index[c].kind == entryFilemark {
			count++
		}
	}
	return c, nil
}

func (d *Device) SeekToEOD(ctx context.Context) error {
	ctx, span := d.tracer.Start(ctx, "simulatorDevice.SeekToEOD")
	defer span.End()
	if err := d.lock(ctx); err != nil {
		return err
	}
	defer d.unlock()
	if err := d.checkOpen(); err != nil {
		return err
	}
	return d.moveTo(ctx, len(d.index))
}

// moveTo repositions the head to the given block, charging the seek cost.
func (d *Device) moveTo(ctx context.Context, block int) error {
	cost := d.latency.seekCost(d.offsetOf(d.cursor), d.offsetOf(block), d.scaleCapacity)
	if err := d.sleep(ctx, cost); err != nil {
		return err
	}
	d.cursor = block
	return nil
}

// spaceTo advances or backs over records without treating each spacing
// command as a fresh random seek.
func (d *Device) spaceTo(ctx context.Context, block int) error {
	cost := d.latency.spaceCost(d.offsetOf(d.cursor), d.offsetOf(block), d.scaleCapacity)
	if err := d.sleep(ctx, cost); err != nil {
		return err
	}
	d.cursor = block
	return nil
}

func (d *Device) Status(ctx context.Context) (*tape.Status, error) {
	_, span := d.tracer.Start(ctx, "simulatorDevice.Status")
	defer span.End()
	if err := d.lock(ctx); err != nil {
		return nil, err
	}
	defer d.unlock()
	if err := d.checkOpen(); err != nil {
		return nil, err
	}
	fileNumber := int64(0)
	blockNumber := int64(0)
	for _, e := range d.index[:d.cursor] {
		if e.kind == entryFilemark {
			fileNumber++
			blockNumber = 0
		} else {
			blockNumber++
		}
	}
	return &tape.Status{
		Online:         true,
		WriteProtected: d.readOnly,
		AtBOT:          d.cursor == 0,
		AtEOD:          d.cursor == len(d.index),
		FileNumber:     fileNumber,
		BlockNumber:    blockNumber,
	}, nil
}

// Capacity returns the payload capacity of the medium in bytes; 0 means
// unlimited. This is a property of the simulated medium and intentionally
// not part of tape.Device (real drives cannot report it).
func (d *Device) Capacity() int64 {
	return d.capacity
}

func (d *Device) Close() error {
	d.sem <- struct{}{}
	defer d.unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	return d.f.Close()
}
