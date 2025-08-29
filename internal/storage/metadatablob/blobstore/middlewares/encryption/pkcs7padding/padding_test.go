package pkcs7padding

import (
	"fmt"
	"testing"

	testutils "github.com/jdillenkofer/pithos/internal/testing"
)

func TestPaddingForDifferentLengths(t *testing.T) {
	testutils.SkipIfIntegration(t)
	var lengths = []int{1, 5, 10, 25, 50, 100, 150, 200}
	for _, length := range lengths {
		testName := fmt.Sprintf("TestPadding with length %d", length)
		t.Run(testName, CurrySubtestWithLength(length))
	}
}

func CurrySubtestWithLength(length int) func(*testing.T) {
	return func(t *testing.T) {
		t.Parallel()
		bytes := make([]byte, length)
		for i := 0; i < length; i++ {
			bytes[i] = byte(i)
		}
		blockSize := 128 / 8
		bytes, err := Pad(bytes, blockSize)
		if err != nil {
			t.Error("Error while padding byte array")
		}
		if len(bytes)%blockSize != 0 {
			t.Errorf("Byte array was not padded to modulo blockSize: %d %% %d != 0", len(bytes), blockSize)
		}
		bytes, err = Unpad(bytes, blockSize)
		if err != nil {
			t.Error("Error while unpadding byte array")
		}
		if len(bytes) != length {
			t.Errorf("Byte array length is not %d after unpad.", length)
		}
		for i := 0; i < length; i++ {
			if bytes[i] != byte(i) {
				t.Error("Output byte array was modified.")
			}
		}
	}
}
