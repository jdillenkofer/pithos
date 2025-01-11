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
	return &debugReadCloser{
		name:            name,
		innerReadCloser: innerReadCloser,
	}
}

func (d *debugReadCloser) Read(p []byte) (int, error) {
	log.Println("DebugReadCloser", d.name, "Read(", p, ")")
	n, err := d.innerReadCloser.Read(p)
	log.Println("DebugReadCloser", d.name, "Read(", p, ") = (n: ", n, ", err:", err, "); p:", "\""+string(p[0:n])+"\"")
	return n, err
}

func (d *debugReadCloser) Close() error {
	log.Println("DebugReadCloser", d.name, "Close()")
	err := d.innerReadCloser.Close()
	log.Println("DebugReadCloser", d.name, "Close() = err: ", err)
	return err
}
