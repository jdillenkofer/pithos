// Package tape provides an abstraction for sequential-access tape devices
// operating in variable-block mode: every WriteRecord produces exactly one
// tape record and every ReadRecord consumes exactly one.
//
// Records and filemarks each occupy one logical block position (SCSI logical
// object addressing), so a tape written as
//
//	record record filemark record filemark
//
// has blocks 0..4 with filemarks at block 2 and 4 and end-of-data at block 5.
//
// Every method maps directly to a capability of the Linux st driver; nothing
// in this interface is specific to simulated devices.
package tape

import (
	"context"
	"errors"
	"io"
)

var (
	// ErrFilemark is returned by ReadRecord when the head is at a filemark.
	// The position advances past the filemark (st driver behavior).
	ErrFilemark = errors.New("tape: filemark encountered")
	// ErrEndOfData is returned when reading or spacing in the blank region
	// after the last written data.
	ErrEndOfData = errors.New("tape: end of recorded data")
	// ErrBeginningOfTape is returned when spacing backward hits the
	// beginning of the tape before the requested count is reached.
	ErrBeginningOfTape = errors.New("tape: beginning of tape")
	// ErrEndOfTape is returned when a write hits the physical end of the medium.
	ErrEndOfTape = errors.New("tape: end of tape")
	// ErrClosed is returned for operations on a closed device.
	ErrClosed = errors.New("tape: device closed")
	// ErrWriteProtected is returned for writes to a write-protected medium.
	ErrWriteProtected = errors.New("tape: medium is write-protected")
	// ErrUnsupportedPlatform is returned by st.Open on platforms without
	// tape device support.
	ErrUnsupportedPlatform = errors.New("tape: unsupported platform")
)

// Position is a logical object position on the tape. Records and filemarks
// each occupy one block position.
type Position struct {
	Block uint64
}

// Status reports the drive and medium state. All fields are available from
// the st driver via MTIOCGET.
type Status struct {
	// Online reports whether a medium is loaded and ready.
	Online bool
	// WriteProtected reports whether the medium is write-protected.
	WriteProtected bool
	// AtBOT reports whether the head is at the beginning of the tape.
	AtBOT bool
	// AtEOD reports whether the head is at the end of recorded data.
	AtEOD bool
	// FileNumber is the number of filemarks before the head, or -1 if unknown.
	FileNumber int64
	// BlockNumber is the record number within the current file, or -1 if unknown.
	BlockNumber int64
}

// Device models a sequential-access tape device in variable-block mode.
// Implementations are safe for concurrent use, but operations serialize
// (a tape has a single head); callers should treat the device as single-user.
type Device interface {
	io.Closer

	// WriteRecord writes p as exactly one tape record. Writing anywhere but
	// end-of-data logically erases everything after the current position.
	// Returns ErrEndOfTape when the medium is full and ErrWriteProtected on
	// read-only media.
	WriteRecord(ctx context.Context, p []byte) error
	// ReadRecord reads exactly one record into p and returns its length.
	// Returns ErrFilemark at a filemark (the position moves past it),
	// ErrEndOfData past the last written data, and io.ErrShortBuffer if the
	// record is larger than len(p).
	ReadRecord(ctx context.Context, p []byte) (int, error)
	// WriteFilemarks writes count filemarks at the current position.
	WriteFilemarks(ctx context.Context, count int) error

	// Rewind positions the head at the beginning of the tape.
	Rewind(ctx context.Context) error
	// LocateBlock positions the head at the given logical block.
	LocateBlock(ctx context.Context, block uint64) error
	// Tell returns the current logical position.
	Tell(ctx context.Context) (Position, error)
	// SpaceRecords moves count records forward (positive) or backward
	// (negative). If a filemark is hit, movement stops just past it in the
	// direction of movement and ErrFilemark is returned. Returns
	// ErrEndOfData when hitting the end of recorded data and
	// ErrBeginningOfTape when hitting the beginning of the tape.
	SpaceRecords(ctx context.Context, count int) error
	// SpaceFilemarks moves past count filemarks forward (positive) or
	// backward (negative), stopping just past the last one in the direction
	// of movement. Returns ErrEndOfData (forward) or ErrBeginningOfTape
	// (backward) if there are not enough filemarks.
	SpaceFilemarks(ctx context.Context, count int) error
	// SeekToEOD positions the head after the last written data.
	SeekToEOD(ctx context.Context) error

	// Status returns the current drive and medium state.
	Status(ctx context.Context) (*Status, error)
}
