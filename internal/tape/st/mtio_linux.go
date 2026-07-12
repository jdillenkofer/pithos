//go:build linux && (amd64 || arm64)

package st

import "unsafe"

// Definitions from <linux/mtio.h>, which golang.org/x/sys/unix does not
// provide. The struct layouts assume 64-bit longs, hence the amd64/arm64
// build constraint.

// mtOp is struct mtop: the argument of the MTIOCTOP ioctl.
type mtOp struct {
	Op    int16
	_     [2]byte
	Count int32
}

// mtGet is struct mtget: the result of the MTIOCGET ioctl.
type mtGet struct {
	Type   int64
	Resid  int64
	Dsreg  int64
	Gstat  int64
	Erreg  int64
	FileNo int32
	BlkNo  int32
}

// mtPos is struct mtpos: the result of the MTIOCPOS ioctl.
type mtPos struct {
	BlkNo int64
}

// Tape operations for mtOp.Op.
const (
	mtWEOF   = 0  // write count filemarks
	mtFSF    = 1  // forward space over count filemarks
	mtBSF    = 2  // backward space over count filemarks
	mtFSR    = 3  // forward space count records
	mtBSR    = 4  // backward space count records
	mtREW    = 6  // rewind
	mtEOM    = 12 // space to end of recorded media
	mtSETBLK = 20 // set block size (0 = variable block mode)
	mtSEEK   = 22 // seek to block
)

// Generalized status bits in mtGet.Gstat.
const (
	gmtEOF    = 0x80000000 // at a filemark
	gmtBOT    = 0x40000000 // at beginning of tape
	gmtEOT    = 0x20000000 // at physical end of tape
	gmtEOD    = 0x08000000 // at end of recorded data
	gmtWrProt = 0x04000000 // medium is write-protected
	gmtOnline = 0x01000000 // medium loaded and ready
)

// ioctl request numbers, encoded with the asm-generic _IOR/_IOW scheme
// (valid on amd64 and arm64): dir<<30 | size<<16 | 'm'<<8 | nr.
const (
	iocRead  = 2
	iocWrite = 1
)

func ioc(dir, nr, size uintptr) uintptr {
	return dir<<30 | size<<16 | 'm'<<8 | nr
}

var (
	mtiocTop = ioc(iocWrite, 1, unsafe.Sizeof(mtOp{}))
	mtiocGet = ioc(iocRead, 2, unsafe.Sizeof(mtGet{}))
	mtiocPos = ioc(iocRead, 3, unsafe.Sizeof(mtPos{}))
)
