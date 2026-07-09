//go:build wasip1 && wasm

package main

import (
	"unsafe"

	"github.com/jdillenkofer/pithos/examples/wasm-authorizer/go/pithos"
)

var allocations = map[uint32][]byte{}

//go:wasmexport pithos_alloc
func pithosAlloc(size uint32) uint32 {
	if size == 0 {
		return 0
	}
	buf := make([]byte, size)
	ptr := uint32(uintptr(unsafe.Pointer(&buf[0])))
	allocations[ptr] = buf
	return ptr
}

//go:wasmexport pithos_free
func pithosFree(ptr uint32, len uint32) {
	delete(allocations, ptr)
}

//go:wasmexport pithos_evaluate
func pithosEvaluate(ptr uint32, len uint32) uint64 {
	inputBytes := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(ptr))), int(len))

	input, ok := pithos.ParseInput(inputBytes)
	if !ok {
		return writeDecision(false)
	}
	return writeDecision(allow(input))
}

func allow(input pithos.Input) bool {
	switch input.Hook {
	case "request":
		return allowRequest(input.Request)
	case "list-object":
		return hasPrefix(input.Resource.Key, "public/")
	default:
		return false
	}
}

func allowRequest(request pithos.Request) bool {
	publicRead := request.Operation == "GetObject" &&
		request.Bucket == "public-assets" &&
		request.Key == "public/index.html"
	adminWrite := request.Authorization.AccessKeyID == "admin-access-key-id" &&
		!request.IsReadOnly
	return publicRead || adminWrite
}

func writeDecision(allow bool) uint64 {
	output := pithos.MarshalDecision(allow)
	outPtr := pithosAlloc(uint32(len(output)))
	outBytes := unsafe.Slice((*byte)(unsafe.Pointer(uintptr(outPtr))), len(output))
	copy(outBytes, output)
	return uint64(outPtr)<<32 | uint64(len(output))
}

func hasPrefix(value string, prefix string) bool {
	return len(value) >= len(prefix) && value[:len(prefix)] == prefix
}

func main() {}
