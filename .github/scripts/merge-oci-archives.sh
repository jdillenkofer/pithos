#!/usr/bin/env bash
# Merges single-platform OCI archives exported by buildx into one
# multi-platform OCI archive, without a registry. Image manifests and their
# provenance attestation manifests are carried over unchanged, so the
# per-platform digests stay the same.
#
# Prints the digest of the merged image index.
#
# Usage: merge-oci-archives.sh <output> <ref-name> <archive>...
set -euo pipefail

output="$1"
ref_name="$2"
shift 2
if (( $# == 0 )); then
  echo 'Error: No input archives' >&2
  exit 1
fi

index_media_type='application/vnd.oci.image.index.v1+json'
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
layout="$work/layout"
mkdir -p "$layout/blobs/sha256"
: > "$work/descriptors"

for archive in "$@"; do
  src="$work/src"
  rm -rf "$src"
  mkdir "$src"
  tar -xf "$archive" -C "$src"

  # buildx writes one named entry; with provenance it is always an index.
  if [[ "$(jq '.manifests | length' "$src/index.json")" != 1 ]]; then
    echo "Error: ${archive} must contain exactly one image" >&2
    exit 1
  fi
  digest="$(jq -r '.manifests[0].digest' "$src/index.json")"
  index_blob="$src/blobs/sha256/${digest#sha256:}"
  if [[ "sha256:$(sha256sum "$index_blob" | cut -d ' ' -f 1)" != "$digest" ]]; then
    echo "Error: ${archive} index does not match its digest" >&2
    exit 1
  fi
  if [[ "$(jq -r .mediaType "$index_blob")" != "$index_media_type" ]]; then
    echo "Error: ${archive} does not contain an image index" >&2
    exit 1
  fi

  # Blobs are content-addressed, so one copy of a shared blob is enough.
  for blob in "$src"/blobs/sha256/*; do
    if [[ ! -e "$layout/blobs/sha256/${blob##*/}" ]]; then
      cp "$blob" "$layout/blobs/sha256/"
    fi
  done
  jq -c '.manifests[]' "$index_blob" >> "$work/descriptors"
done

# Same order as a multi-platform buildx build: images, then attestations.
jq -cs --arg media_type "$index_media_type" '
  def attestation: .annotations["vnd.docker.reference.type"] == "attestation-manifest";
  def platform: "\(.platform.os)/\(.platform.architecture)/\(.platform.variant // "")";
  [.[] | select(attestation | not)] as $images
  | if ($images | map(platform) | unique | length) != ($images | length)
    then error("duplicate platform") else . end
  | {
      schemaVersion: 2,
      mediaType: $media_type,
      manifests: ($images + [.[] | select(attestation)])
    }
' "$work/descriptors" > "$work/index"

digest="sha256:$(sha256sum "$work/index" | cut -d ' ' -f 1)"
mv "$work/index" "$layout/blobs/sha256/${digest#sha256:}"
jq -cn \
  --arg media_type "$index_media_type" \
  --arg digest "$digest" \
  --argjson size "$(wc -c < "$layout/blobs/sha256/${digest#sha256:}")" \
  --arg ref_name "$ref_name" \
  '{
    schemaVersion: 2,
    mediaType: $media_type,
    manifests: [{
      mediaType: $media_type,
      digest: $digest,
      size: $size,
      annotations: {"org.opencontainers.image.ref.name": $ref_name}
    }]
  }' > "$layout/index.json"
printf '{"imageLayoutVersion":"1.0.0"}' > "$layout/oci-layout"

tar -C "$layout" -cf "$output" oci-layout index.json blobs
printf '%s\n' "$digest"
