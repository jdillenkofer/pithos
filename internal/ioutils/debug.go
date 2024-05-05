package ioutils

import (
	"io"
	"log"
)

type DebugReadSeekCloser struct {
	name                string
	innerReadSeekCloser io.ReadSeekCloser
}

func NewDebugReadSeekCloser(name string, innerReadSeekCloser io.ReadSeekCloser) *DebugReadSeekCloser {
	return &DebugReadSeekCloser{
		name:                name,
		innerReadSeekCloser: innerReadSeekCloser,
	}
}

func (d *DebugReadSeekCloser) Read(p []byte) (int, error) {
	log.Println("DebugReadSeekCloser", d.name, "Read(", p, ")")
	n, err := d.innerReadSeekCloser.Read(p)
	log.Println("DebugReadSeekCloser", d.name, "Read(", p, ") = (n: ", n, ", err:", err, "); p:", "\""+string(p[0:n])+"\"")
	return n, err
}

func (d *DebugReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	log.Println("DebugReadSeekCloser", d.name, "Seek(", offset, ",", whence, ")")
	n, err := d.innerReadSeekCloser.Seek(offset, whence)
	log.Println("DebugReadSeekCloser", d.name, "Seek(", offset, ",", whence, ") = (n: ", n, ", err:", err, ")")
	return n, err
}

func (d *DebugReadSeekCloser) Close() error {
	log.Println("DebugReadSeekCloser", d.name, "Close()")
	err := d.innerReadSeekCloser.Close()
	log.Println("DebugReadSeekCloser", d.name, "Close() = err: ", err)
	return err
}
