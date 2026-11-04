#!/usr/bin/env bash
# Run after a full checkout with origin/main available. Fail closed for all events.
set -euo pipefail

case "${EVENT_NAME}:${RELEASE_REF}" in
  workflow_dispatch:refs/heads/main) ;;
  push:refs/tags/*)
    if [[ ! "$RELEASE_REF" =~ ^refs/tags/v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$ ]]; then
      echo 'Error: Release tags must be stable semver tags (vX.Y.Z)' >&2
      exit 1
    fi
    ;;
  *)
    echo 'Error: Releases and dry runs require manual main or a stable semver tag' >&2
    exit 1
    ;;
esac

# Dereference annotated tags and check the actual checked-out commit. Checking
# the event SHA too prevents validating a different commit from the build.
commit="$(git rev-parse --verify "${RELEASE_SHA}^{commit}")"
if [[ "$(git rev-parse HEAD)" != "$commit" ]]; then
  echo 'Error: Checkout does not match the release event commit' >&2
  exit 1
fi
if ! git merge-base --is-ancestor "$commit" refs/remotes/origin/main; then
  echo 'Error: Release commit must be on origin/main' >&2
  exit 1
fi
