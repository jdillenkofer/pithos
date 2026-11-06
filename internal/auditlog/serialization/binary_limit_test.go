package serialization

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBoundedBinaryDecoderRejectsOversizedFieldBeforeAllocation(t *testing.T) {
	var input bytes.Buffer
	require.NoError(t, binary.Write(&input, binary.BigEndian, uint16(6)))
	require.NoError(t, binary.Write(&input, binary.BigEndian, int64(0)))
	require.NoError(t, binary.Write(&input, binary.BigEndian, uint32(0xffffffff)))
	_, err := (&BinarySerializer{MaxFieldBytes: 1 << 20}).NewDecoder(&input).Decode()
	require.ErrorContains(t, err, "exceeds decoding limit")
}
