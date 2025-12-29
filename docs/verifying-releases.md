# Verifying Releases

Pithos releases (Docker images and binaries) are signed using [Cosign](https://github.com/sigstore/cosign) (Sigstore). You can verify the authenticity of the artifacts using the following commands.

## Verifying Docker Images

```sh
cosign verify jdillenkofer/pithos:latest \
  --certificate-identity-regexp "https://github.com/jdillenkofer/pithos" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

## Verifying Binaries

Download the `checksums.txt`, `checksums.txt.pem`, and `checksums.txt.sig` files from the release page along with the binary you want to use.

### 1. Verify the signature of the checksums file

```sh
cosign verify-blob \
  --certificate checksums.txt.pem \
  --signature checksums.txt.sig \
  checksums.txt \
  --certificate-identity-regexp "https://github.com/jdillenkofer/pithos" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

### 2. Verify the binary's checksum

```sh
sha256sum -c checksums.txt --ignore-missing
```
