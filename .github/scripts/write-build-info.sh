#!/usr/bin/env bash
# Records the environment that built a release binary. The C toolchains are
# not pinned, so this is what makes a release rebuildable later.
#
# With -trimpath, Go does not record -ldflags in the binary, so the caller
# passes them in.
#
# Usage: write-build-info.sh <binary> <output> <ldflags>
set -euo pipefail

binary="$1"
output="$2"
ldflags="$3"
cc="$(go env CC)"

{
  printf '== %s\n' "$(basename "$binary")"
  printf 'runner image: %s %s\n' "${ImageOS:-unknown}" "${ImageVersion:-unknown}"
  printf 'runner os: %s\n' "$(uname -srm)"
  printf 'c compiler: %s\n' "$cc"
  # CC may carry arguments (e.g. "zig cc -target ..."), so it is split on purpose.
  # shellcheck disable=SC2086
  printf 'c compiler version: %s\n' "$($cc --version 2>&1 | sed -n 1p)"
  printf 'ldflags: %s\n' "$ldflags"
  # Go version, build flags, GOOS/GOARCH, CGO settings, VCS revision and all
  # module versions.
  (cd "$(dirname "$binary")" && go version -m "$(basename "$binary")")
  printf '\n'
} > "$output"
