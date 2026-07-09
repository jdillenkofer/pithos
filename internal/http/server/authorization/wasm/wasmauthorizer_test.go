package wasm

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jdillenkofer/pithos/internal/http/server/authorization"
	"github.com/jdillenkofer/pithos/internal/ptrutils"
	testutils "github.com/jdillenkofer/pithos/internal/testing"
	"github.com/stretchr/testify/require"
)

func TestWasmAuthorizerAllowsRequest(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authorizer, err := NewWasmAuthorizer(testAuthorizerWasm(`{"allow":true}`))
	require.NoError(t, err)
	defer authorizer.Close(context.Background())

	allowed, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationGetObject,
	})
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestWasmAuthorizerResourceHooksUseEvaluate(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authorizer, err := NewWasmAuthorizer(testAuthorizerWasm(`{"allow":true}`))
	require.NoError(t, err)
	defer authorizer.Close(context.Background())

	request := &authorization.Request{Operation: authorization.OperationListObjects}
	allowed, err := authorizer.AuthorizeListObject(context.Background(), request, "key")
	require.NoError(t, err)
	require.True(t, allowed)

	allowed, err = authorizer.AuthorizeListPart(context.Background(), request, 1)
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestWasmAuthorizerHandlesConcurrentRequests(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authorizer, err := NewWasmAuthorizer(testAuthorizerWasm(`{"allow":true}`))
	require.NoError(t, err)
	defer authorizer.Close(context.Background())

	const callCount = 32
	var wg sync.WaitGroup
	for i := 0; i < callCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			allowed, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
				Operation: authorization.OperationGetObject,
			})
			require.NoError(t, err)
			require.True(t, allowed)
		}()
	}
	wg.Wait()
}

func TestWasmAuthorizerCanDisableInstancePool(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authorizer, err := NewWasmAuthorizerWithOptions(testAuthorizerWasm(`{"allow":true}`), Options{
		InstancePoolSize: -1,
	})
	require.NoError(t, err)
	defer authorizer.Close(context.Background())

	allowed, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationGetObject,
	})
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestWasmAuthorizerDoesNotResolveObjectTagsUnlessGuestRequestsThem(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authorizer, err := NewWasmAuthorizer(testAuthorizerWasm(`{"allow":true}`))
	require.NoError(t, err)
	defer authorizer.Close(context.Background())

	callCount := 0
	allowed, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationGetObject,
		ResolveExistingObjectTags: func(ctx context.Context) (map[string]string, error) {
			callCount++
			return map[string]string{"env": "prod"}, nil
		},
	})
	require.NoError(t, err)
	require.True(t, allowed)
	require.Equal(t, 0, callCount)
}

func TestWasmAuthorizerDeniesOnHostImportedTagResolverError(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authorizer, err := NewWasmAuthorizer(tagImportAuthorizerWasm(t))
	require.NoError(t, err)
	defer authorizer.Close(context.Background())

	allowed, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationGetObject,
		ResolveExistingObjectTags: func(ctx context.Context) (map[string]string, error) {
			return nil, errors.New("storage unavailable")
		},
	})
	require.Error(t, err)
	require.False(t, allowed)
}

func BenchmarkWasmAuthorizerAuthorizeRequest(b *testing.B) {
	authorizer, err := NewWasmAuthorizer(testAuthorizerWasm(`{"allow":true}`))
	require.NoError(b, err)
	defer authorizer.Close(context.Background())

	request := &authorization.Request{
		Operation: authorization.OperationGetObject,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		allowed, err := authorizer.AuthorizeRequest(context.Background(), request)
		if err != nil {
			b.Fatal(err)
		}
		if !allowed {
			b.Fatal("request denied")
		}
	}
}

func TestWasmAuthorizerDeniesOnMalformedDecision(t *testing.T) {
	testutils.SkipIfIntegration(t)
	_, err := NewWasmAuthorizer(testAuthorizerWasm(`not-json`))
	require.Error(t, err)
}

func TestWasmAuthorizerDeniesOnOversizedDecision(t *testing.T) {
	testutils.SkipIfIntegration(t)
	_, err := NewWasmAuthorizerWithOptions(testAuthorizerWasm(`{"allow":true,"reason":"too large"}`), Options{
		MaxDecisionBytes: 8,
	})
	require.Error(t, err)
}

func TestWasmAuthorizerRoundTripsRequestInputThroughGuestMemory(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authorizer := newRoundTripAuthorizer(t, []string{
		`"abiVersion":1`,
		`"hook":"request"`,
		`"operation":"GetObject"`,
		`"accessKeyId":"AKIAIOSFODNN7EXAMPLE"`,
		`"bucket":"photos"`,
		`"key":"public/cat.jpg"`,
		`"sourceBucket":"archive"`,
		`"sourceKey":"cat-source.jpg"`,
		`"method":"GET"`,
		`"path":"/photos/public/cat.jpg"`,
		`"query":"versionId=abc123"`,
		`"name":"versionId"`,
		`"values":["abc123"]`,
		`"name":"X-Test-Header"`,
		`"values":["roundtrip"]`,
		`"host":"s3.localhost"`,
		`"proto":"HTTP/1.1"`,
		`"contentLength":42`,
		`"remoteAddr":"10.0.0.1:12345"`,
		`"remoteIP":"10.0.0.1"`,
		`"clientIP":"203.0.113.10"`,
		`"scheme":"https"`,
		`"isReadOnly":true`,
		`"requestObjectTags":{"team":"storage"}`,
	})
	defer authorizer.Close(context.Background())

	contentLength := 42
	allowed, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationGetObject,
		Authorization: authorization.Authorization{
			AccessKeyId: ptrutils.ToPtr("AKIAIOSFODNN7EXAMPLE"),
		},
		Bucket:       ptrutils.ToPtr("photos"),
		Key:          ptrutils.ToPtr("public/cat.jpg"),
		SourceBucket: ptrutils.ToPtr("archive"),
		SourceKey:    ptrutils.ToPtr("cat-source.jpg"),
		HttpRequest: authorization.HTTPRequest{
			Method:        "GET",
			Path:          "/photos/public/cat.jpg",
			Query:         "versionId=abc123",
			QueryParams:   map[string][]string{"versionId": {"abc123"}},
			Headers:       map[string][]string{"X-Test-Header": {"roundtrip"}, "X-Forwarded-For": {"203.0.113.10"}, "X-Forwarded-Proto": {"https"}},
			Host:          "s3.localhost",
			Proto:         "HTTP/1.1",
			ContentLength: &contentLength,
			RemoteAddr:    "10.0.0.1:12345",
			RemoteIP:      ptrutils.ToPtr("10.0.0.1"),
		},
		RequestObjectTags: map[string]string{"team": "storage"},
		ResolveExistingObjectTags: func(ctx context.Context) (map[string]string, error) {
			return map[string]string{"env": "prod"}, nil
		},
		ResolveExistingSourceObjectTags: func(ctx context.Context) (map[string]string, error) {
			return map[string]string{"source": "true"}, nil
		},
	})
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestWasmAuthorizerRoundTripsResourceHookInputThroughGuestMemory(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authorizer := newRoundTripAuthorizer(t, []string{
		`"hook":"list-multipart-upload"`,
		`"operation":"ListMultipartUploads"`,
		`"bucket":"photos"`,
		`"resource":{"key":"uploads/cat.jpg","uploadId":"upload-123"}`,
	})
	defer authorizer.Close(context.Background())

	allowed, err := authorizer.AuthorizeListMultipartUpload(context.Background(), &authorization.Request{
		Operation: authorization.OperationListMultipartUploads,
		Bucket:    ptrutils.ToPtr("photos"),
	}, "uploads/cat.jpg", "upload-123")
	require.NoError(t, err)
	require.True(t, allowed)
}

func TestWasmAuthorizerRoundTripGuestCanDenyWhenInputDoesNotMatch(t *testing.T) {
	testutils.SkipIfIntegration(t)
	authorizer := newRoundTripAuthorizer(t, []string{
		`"operation":"PutObject"`,
	})
	defer authorizer.Close(context.Background())

	allowed, err := authorizer.AuthorizeRequest(context.Background(), &authorization.Request{
		Operation: authorization.OperationGetObject,
	})
	require.NoError(t, err)
	require.False(t, allowed)
}

func newRoundTripAuthorizer(t *testing.T, needles []string) *WasmAuthorizer {
	t.Helper()

	wasmBytes := compileWAT(t, roundTripWAT(needles))
	authorizer, err := NewWasmAuthorizerWithOptions(wasmBytes, Options{
		TrustForwardedHeaders: true,
		TrustedProxyCIDRs:     []string{"10.0.0.0/8"},
	})
	require.NoError(t, err)
	return authorizer
}

func tagImportAuthorizerWasm(t *testing.T) []byte {
	t.Helper()

	return compileWAT(t, `(module
  (import "pithos" "object_tags_len" (func $object_tags_len (result i32)))
  (memory (export "memory") 1)
  (func (export "pithos_alloc") (param $size i32) (result i32)
    i32.const 1024)
  (func (export "pithos_free") (param $ptr i32) (param $len i32))
  (func (export "pithos_evaluate") (param $ptr i32) (param $len i32) (result i64)
    call $object_tags_len
    drop
    i64.const 8796093022222)
  (data (i32.const 2048) "{\"allow\":true}"))`)
}

func compileWAT(t *testing.T, wat string) []byte {
	t.Helper()

	wat2wasm, err := exec.LookPath("wat2wasm")
	if err != nil {
		t.Skip("wat2wasm not installed; skipping Wasm roundtrip test")
	}

	tmpDir := t.TempDir()
	watPath := filepath.Join(tmpDir, "authorizer.wat")
	wasmPath := filepath.Join(tmpDir, "authorizer.wasm")
	require.NoError(t, os.WriteFile(watPath, []byte(wat), 0o600))

	output, err := exec.Command(wat2wasm, watPath, "-o", wasmPath).CombinedOutput()
	require.NoError(t, err, string(output))

	wasmBytes, err := os.ReadFile(wasmPath)
	require.NoError(t, err)
	return wasmBytes
}

func roundTripWAT(needles []string) string {
	const (
		allowPtr = 4096
		denyPtr  = 8192
	)
	allow := `{"allow":true}`
	deny := `{"allow":false}`
	allowPacked := uint64(allowPtr)<<32 | uint64(len(allow))
	denyPacked := uint64(denyPtr)<<32 | uint64(len(deny))

	wat := `(module
  (memory (export "memory") 1)
  (func (export "pithos_alloc") (param $size i32) (result i32)
    i32.const 1024)
  (func (export "pithos_free") (param $ptr i32) (param $len i32))
  (func $contains (param $ptr i32) (param $len i32) (param $needle i32) (param $needle_len i32) (result i32)
    (local $i i32)
    (local $j i32)
    (local $limit i32)
    local.get $needle_len
    i32.eqz
    if
      i32.const 1
      return
    end
    local.get $len
    local.get $needle_len
    i32.lt_u
    if
      i32.const 0
      return
    end
    local.get $len
    local.get $needle_len
    i32.sub
    local.set $limit
    loop $outer
      i32.const 0
      local.set $j
      block $after_inner
        loop $inner
          local.get $j
          local.get $needle_len
          i32.eq
          if
            i32.const 1
            return
          end
          local.get $ptr
          local.get $i
          i32.add
          local.get $j
          i32.add
          i32.load8_u
          local.get $needle
          local.get $j
          i32.add
          i32.load8_u
          i32.ne
          if
            br $after_inner
          end
          local.get $j
          i32.const 1
          i32.add
          local.set $j
          br $inner
        end
      end
      local.get $i
      local.get $limit
      i32.eq
      if
        i32.const 0
        return
      end
      local.get $i
      i32.const 1
      i32.add
      local.set $i
      br $outer
    end
    i32.const 0)
  (func (export "pithos_evaluate") (param $ptr i32) (param $len i32) (result i64)
`
	for i, needle := range needles {
		offset := 12288 + i*512
		wat += `    local.get $ptr
    local.get $len
    i32.const ` + intString(offset) + `
    i32.const ` + intString(len(needle)) + `
    call $contains
    i32.eqz
    if
      i64.const ` + uintString(denyPacked) + `
      return
    end
`
	}
	wat += `    i64.const ` + uintString(allowPacked) + `
  )
  (data (i32.const ` + intString(allowPtr) + `) ` + watString(allow) + `)
  (data (i32.const ` + intString(denyPtr) + `) ` + watString(deny) + `)
`
	for i, needle := range needles {
		wat += `  (data (i32.const ` + intString(12288+i*512) + `) ` + watString(needle) + `)
`
	}
	wat += `)`
	return wat
}

func watString(value string) string {
	result := `"`
	for _, b := range []byte(value) {
		switch b {
		case '"', '\\':
			result += `\` + string(b)
		case '\n':
			result += `\0a`
		default:
			result += string(b)
		}
	}
	result += `"`
	return result
}

func intString(value int) string {
	return uintString(uint64(value))
}

func uintString(value uint64) string {
	if value == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for value > 0 {
		i--
		digits[i] = byte('0' + value%10)
		value /= 10
	}
	return string(digits[i:])
}

func testAuthorizerWasm(decision string) []byte {
	const inputPtr = 1024
	const outputPtr = 2048
	output := []byte(decision)
	packedOutput := uint64(outputPtr)<<32 | uint64(len(output))

	var wasm []byte
	wasm = append(wasm, []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}...)
	wasm = appendSection(wasm, 1, concat(
		u32(3),
		[]byte{0x60, 0x01, 0x7f, 0x01, 0x7f},
		[]byte{0x60, 0x02, 0x7f, 0x7f, 0x00},
		[]byte{0x60, 0x02, 0x7f, 0x7f, 0x01, 0x7e},
	))
	wasm = appendSection(wasm, 3, concat(u32(3), u32(0), u32(1), u32(2)))
	wasm = appendSection(wasm, 5, []byte{0x01, 0x00, 0x01})
	wasm = appendSection(wasm, 7, concat(
		u32(4),
		export("memory", 0x02, 0),
		export(exportAlloc, 0x00, 0),
		export(exportFree, 0x00, 1),
		export(exportEvaluate, 0x00, 2),
	))
	wasm = appendSection(wasm, 10, concat(
		u32(3),
		body(concat([]byte{0x41}, i32(inputPtr), []byte{0x0b})),
		body([]byte{0x0b}),
		body(concat([]byte{0x42}, i64(packedOutput), []byte{0x0b})),
	))
	wasm = appendSection(wasm, 11, concat(
		u32(1),
		[]byte{0x00, 0x41},
		i32(outputPtr),
		[]byte{0x0b},
		u32(uint32(len(output))),
		output,
	))
	return wasm
}

func appendSection(wasm []byte, id byte, contents []byte) []byte {
	wasm = append(wasm, id)
	wasm = append(wasm, u32(uint32(len(contents)))...)
	wasm = append(wasm, contents...)
	return wasm
}

func export(name string, kind byte, index uint32) []byte {
	return concat(u32(uint32(len(name))), []byte(name), []byte{kind}, u32(index))
}

func body(code []byte) []byte {
	return concat(u32(uint32(1+len(code))), []byte{0x00}, code)
}

func concat(parts ...[]byte) []byte {
	var result []byte
	for _, part := range parts {
		result = append(result, part...)
	}
	return result
}

func u32(v uint32) []byte {
	var out []byte
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		out = append(out, b)
		if v == 0 {
			return out
		}
	}
}

func i32(v int32) []byte {
	var out []byte
	for {
		b := byte(v & 0x7f)
		v >>= 7
		done := (v == 0 && b&0x40 == 0) || (v == -1 && b&0x40 != 0)
		if !done {
			b |= 0x80
		}
		out = append(out, b)
		if done {
			return out
		}
	}
}

func i64(v uint64) []byte {
	var out []byte
	for {
		b := byte(v & 0x7f)
		v >>= 7
		done := v == 0 && b&0x40 == 0
		if !done {
			b |= 0x80
		}
		out = append(out, b)
		if done {
			return out
		}
	}
}
