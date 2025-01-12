package ioutils

import (
	"io"
	"log"
)

type debugReadCloser struct {
	name            string
	innerReadCloser io.ReadCloser
}

func NewDebugReadCloser(name string, innerReadCloser io.ReadCloser) io.ReadCloser {
	if innerReadSeekCloser, ok := innerReadCloser.(io.ReadSeekCloser); ok {
		return &debugReadSeekCloser{
			name:                name,
			innerReadSeekCloser: innerReadSeekCloser,
		}
	}
	return &debugReadCloser{
		name:            name,
		innerReadCloser: innerReadCloser,
	}
}

func (d *debugReadCloser) Read(p []byte) (int, error) {
	log.Println("DebugReadSeekCloser", d.name, "Read(", p, ")")
	n, err := d.innerReadCloser.Read(p)
	log.Println("DebugReadSeekCloser", d.name, "Read(", p, ") = (n: ", n, ", err:", err, ")")
	return n, err
}

func (d *debugReadCloser) Close() error {
	log.Println("DebugReadCloser", d.name, "Close()")
	err := d.innerReadCloser.Close()
	log.Println("DebugReadCloser", d.name, "Close() = err: ", err)
	return err
}

type debugReadSeekCloser struct {
	name                string
	innerReadSeekCloser io.ReadSeekCloser
}

func (d *debugReadSeekCloser) Read(p []byte) (int, error) {
	log.Println("DebugReadSeekCloser", d.name, "Read(", p, ")")
	n, err := d.innerReadSeekCloser.Read(p)
	log.Println("DebugReadSeekCloser", d.name, "Read(", p, ") = (n: ", n, ", err:", err, ")")
	return n, err
}

func (d *debugReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	log.Println("DebugReadSeekCloser", d.name, "Seek(", offset, ",", whence, ")")
	n, err := d.innerReadSeekCloser.Seek(offset, whence)
	log.Println("DebugReadSeekCloser", d.name, "Seek(", offset, ",", whence, ") = (n: ", n, ", err:", err, ")")
	return n, err
}

func (d *debugReadSeekCloser) Close() error {
	log.Println("DebugReadSeekCloser", d.name, "Close()")
	err := d.innerReadSeekCloser.Close()
	log.Println("DebugReadSeekCloser", d.name, "Close() = err: ", err)
	return err
}
