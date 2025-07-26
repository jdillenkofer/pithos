package ioutils

import (
	"fmt"
	"io"
	"log/slog"
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
	slog.Debug(fmt.Sprint("DebugReadSeekCloser", d.name, "Read(", "len(p):", len(p), ")"))
	n, err := d.innerReadCloser.Read(p)
	slog.Debug(fmt.Sprint("DebugReadSeekCloser", d.name, "Read(", "len(p):", len(p), ") = (n: ", n, ", err:", err, ")"))
	return n, err
}

func (d *debugReadCloser) Close() error {
	slog.Debug(fmt.Sprint("DebugReadCloser", d.name, "Close()"))
	err := d.innerReadCloser.Close()
	slog.Debug(fmt.Sprint("DebugReadCloser", d.name, "Close() = err: ", err))
	return err
}

type debugReadSeekCloser struct {
	name                string
	innerReadSeekCloser io.ReadSeekCloser
}

func (d *debugReadSeekCloser) Read(p []byte) (int, error) {
	slog.Debug(fmt.Sprint("DebugReadSeekCloser", d.name, "Read(", "len(p):", len(p), ")"))
	n, err := d.innerReadSeekCloser.Read(p)
	slog.Debug(fmt.Sprint("DebugReadSeekCloser", d.name, "Read(", "len(p):", len(p), ") = (n: ", n, ", err:", err, ")"))
	return n, err
}

func (d *debugReadSeekCloser) Seek(offset int64, whence int) (int64, error) {
	slog.Debug(fmt.Sprint("DebugReadSeekCloser", d.name, "Seek(", offset, ",", whence, ")"))
	n, err := d.innerReadSeekCloser.Seek(offset, whence)
	slog.Debug(fmt.Sprint("DebugReadSeekCloser", d.name, "Seek(", offset, ",", whence, ") = (n: ", n, ", err:", err, ")"))
	return n, err
}

func (d *debugReadSeekCloser) Close() error {
	slog.Debug(fmt.Sprint("DebugReadSeekCloser", d.name, "Close()"))
	err := d.innerReadSeekCloser.Close()
	slog.Debug(fmt.Sprint("DebugReadSeekCloser", d.name, "Close() = err: ", err))
	return err
}
