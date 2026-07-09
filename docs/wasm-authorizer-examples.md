# Wasm Authorizer Examples

The example policies live under [`../examples/wasm-authorizer`](../examples/wasm-authorizer). They implement the v1 core-Wasm ABI used by Pithos:

- export `memory`
- export `pithos_alloc(size) -> ptr`
- export `pithos_free(ptr, len)`
- export `pithos_evaluate(ptr, len) -> (ptr << 32) | len`

`pithos_evaluate` receives UTF-8 JSON matching the shape documented in [Configuration](configuration.md#wasm-authorizer-module) and returns a UTF-8 JSON decision such as `{"allow":true}`.

## Policy Behavior

Both examples parse the input JSON into typed structures and implement the same policy:

- allow `GetObject` for bucket `public-assets` and key `public/index.html`
- deny `GetObject` for `public-assets/private/index.html`
- allow writes for access key `admin-access-key-id`
- show listed objects only when the listed key starts with `public/`

These cases are covered by `TestRustExampleAuthorizer` and `TestGoExampleAuthorizer` in the Wasm authorizer test package. The tests compile the examples to `.wasm`, load them through Pithos' wazero adapter, and assert the policy behavior through the public authorization interfaces.

## Rust

Source: [`../examples/wasm-authorizer/rust/src/lib.rs`](../examples/wasm-authorizer/rust/src/lib.rs)

Build:

```sh
rustup target add wasm32-wasip1
cargo build --release \
  --target wasm32-wasip1 \
  --manifest-path examples/wasm-authorizer/rust/Cargo.toml
cp examples/wasm-authorizer/rust/target/wasm32-wasip1/release/pithos_authorizer_rust.wasm ./authorizer.wasm
```

If you set `CARGO_TARGET_DIR`, copy from that target directory instead.

## Go

Source: [`../examples/wasm-authorizer/go/main.go`](../examples/wasm-authorizer/go/main.go)

Build:

```sh
cd examples/wasm-authorizer/go
GOOS=wasip1 GOARCH=wasm go build \
  -buildmode=c-shared \
  -o authorizer.wasm \
  .
```

The standard Go example uses `//go:wasmexport`, so it requires a Go toolchain new enough to support exported Wasm functions. Pithos is currently using Go 1.26.x, which supports this.

## Run Pithos

```sh
PITHOS_AUTHORIZER_TYPE=wasm \
PITHOS_AUTHORIZER_PATH=./authorizer.wasm \
./pithos serve
```

## Notes

- The host owns the call boundary, but allocation happens in guest memory. That is why the guest exports `pithos_alloc` and `pithos_free`.
- A trap, invalid output JSON, timeout, or missing export denies the request.
- Lua allows missing resource hooks by default; Wasm policies should explicitly decide each `hook` they want to allow.
- `objectTags`, `sourceObjectTags`, and `requestObjectTags` are JSON objects in the current adapter.
