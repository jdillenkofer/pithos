# Verifying Releases

Pithos releases (container images and binaries) are signed using [Cosign](https://github.com/sigstore/cosign) (Sigstore). Container images are published to both GitHub Container Registry (`ghcr.io/jdillenkofer/pithos`) and Docker Hub (`jdillenkofer/pithos`). You can verify the authenticity of the artifacts using the following commands.

> **Note:** Binaries are signed with Cosign v3 using the Sigstore protobuf
> bundle format, so verifying them requires [Cosign v3 or later](https://github.com/sigstore/cosign/releases).
> The container images are signed with the legacy tag-based format (for Docker
> Hub compatibility) and can be verified with Cosign v2 or later. The GHCR
> image also has a GitHub build-provenance attestation created through OIDC.

## Verifying Container Images

Verify the GHCR image's keyless Cosign signature:

```sh
cosign verify ghcr.io/jdillenkofer/pithos:latest \
  --certificate-identity-regexp "^https://github.com/jdillenkofer/pithos/\.github/workflows/release\.yml@.*$" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

The Docker Hub copy is signed by the same release workflow and can be verified
separately:

```sh
cosign verify jdillenkofer/pithos:latest \
  --certificate-identity-regexp "^https://github.com/jdillenkofer/pithos/\.github/workflows/release\.yml@.*$" \
  --certificate-oidc-issuer "https://token.actions.githubusercontent.com"
```

### Verifying GitHub build provenance

The GHCR image also carries a GitHub artifact attestation containing SLSA build
provenance. Verify it with the GitHub CLI:

```sh
gh attestation verify oci://ghcr.io/jdillenkofer/pithos:latest \
  --repo jdillenkofer/pithos
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
