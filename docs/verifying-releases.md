# Verifying Releases

Pithos releases (Docker images and binaries) are signed using [Cosign](https://github.com/sigstore/cosign) (Sigstore). You can verify the authenticity of the artifacts using the following commands.

> **Note:** Releases are signed with Cosign v3, which uses the Sigstore
> protobuf bundle format. Install [Cosign v3 or later](https://github.com/sigstore/cosign/releases)
> to verify them.

## Verifying Docker Images

```sh
cosign verify jdillenkofer/pithos:latest \
  --certificate-identity-regexp "^https://github.com/jdillenkofer/pithos/\.github/workflows/release\.yml@.*$" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

## Verifying Binaries

Download the `checksums.txt` and `checksums.txt.bundle` files from the release page along with the binary you want to use.

### 1. Verify the signature of the checksums file

```sh
cosign verify-blob \
  --bundle checksums.txt.bundle \
  --certificate-identity-regexp "^https://github.com/jdillenkofer/pithos/\.github/workflows/release\.yml@.*$" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com" \
  checksums.txt
```

### 2. Verify the binary's checksum

```sh
sha256sum -c checksums.txt --ignore-missing
```
