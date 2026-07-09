package wasm

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	"github.com/stretchr/testify/require"
)

func TestRustExampleAuthorizer(t *testing.T) {
	wasmBytes := buildRustExample(t)
	assertExamplePolicy(t, wasmBytes)
}

func TestGoExampleAuthorizer(t *testing.T) {
	wasmBytes := buildGoExample(t)
	assertExamplePolicy(t, wasmBytes)
}

func assertExamplePolicy(t *testing.T, wasmBytes []byte) {
	t.Helper()

	authorizer, err := NewWasmAuthorizer(wasmBytes)
	require.NoError(t, err)
	defer authorizer.Close(context.Background())

	allowed, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationGetObject,
		Bucket:    ptrutils.ToPtr("public-assets"),
		Key:       ptrutils.ToPtr("public/index.html"),
	})
	require.NoError(t, err)
	require.True(t, allowed, "public object read should be allowed")

	allowed, err = authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationGetObject,
		Bucket:    ptrutils.ToPtr("public-assets"),
		Key:       ptrutils.ToPtr("private/index.html"),
	})
	require.NoError(t, err)
	require.False(t, allowed, "private object read should be denied")

	allowed, err = authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationPutObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("admin-access-key-id"),
		},
		Bucket: ptrutils.ToPtr("public-assets"),
		Key:    ptrutils.ToPtr("private/index.html"),
	})
	require.NoError(t, err)
	require.True(t, allowed, "admin write should be allowed")

	listRequest := &authorization.Request{
		Operation: authorization.OperationListObjects,
		Bucket:    ptrutils.ToPtr("public-assets"),
	}
	allowed, err = authorizer.AuthorizeListObject(context.Background(), listRequest, "public/index.html")
	require.NoError(t, err)
	require.True(t, allowed, "public list entry should be visible")

	allowed, err = authorizer.AuthorizeListObject(context.Background(), listRequest, "private/index.html")
	require.NoError(t, err)
	require.False(t, allowed, "private list entry should be hidden")
}

func buildRustExample(t *testing.T) []byte {
	t.Helper()

	cargo, err := exec.LookPath("cargo")
	if err != nil {
		t.Skip("cargo not installed; skipping Rust authorizer example test")
	}
	if !rustTargetInstalled(t, "wasm32-wasip1") {
		t.Skip("Rust target wasm32-wasip1 not installed; run `rustup target add wasm32-wasip1`")
	}

	targetDir := filepath.Join(t.TempDir(), "target")
	manifestPath := filepath.Join(repoRoot(t), "examples", "wasm-authorizer", "rust", "Cargo.toml")
	cmd := exec.Command(cargo, "build", "--release", "--target", "wasm32-wasip1", "--manifest-path", manifestPath)
	cmd.Env = append(os.Environ(), "CARGO_TARGET_DIR="+targetDir)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	wasmPath := filepath.Join(targetDir, "wasm32-wasip1", "release", "pithos_authorizer_rust.wasm")
	wasmBytes, err := os.ReadFile(wasmPath)
	require.NoError(t, err)
	return wasmBytes
}

func buildGoExample(t *testing.T) []byte {
	t.Helper()

	goBin, err := exec.LookPath("go")
	if err != nil {
		t.Skip("go not installed; skipping Go authorizer example test")
	}

	outputPath := filepath.Join(t.TempDir(), "authorizer.wasm")
	sourceDir := filepath.Join(repoRoot(t), "examples", "wasm-authorizer", "go")
	cmd := exec.Command(goBin, "build", "-buildmode=c-shared", "-o", outputPath, ".")
	cmd.Dir = sourceDir
	cmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))

	wasmBytes, err := os.ReadFile(outputPath)
	require.NoError(t, err)
	return wasmBytes
}

func rustTargetInstalled(t *testing.T, target string) bool {
	t.Helper()

	rustup, err := exec.LookPath("rustup")
	if err != nil {
		return false
	}
	output, err := exec.Command(rustup, "target", "list", "--installed").CombinedOutput()
	require.NoError(t, err, string(output))
	return bytes.Contains(output, []byte(target))
}

func repoRoot(t *testing.T) string {
	t.Helper()

	_, filename, _, ok := runtime.Caller(0)
	require.True(t, ok)
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", "..", "..", "..", ".."))
}
