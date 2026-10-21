#!/usr/bin/env bash
# Adapted from Scryer's portable Linux entropy build preparation.
# Native Ubuntu musl builds need target UAPI headers for AWS-LC's ENOSYS fallback.
set -euo pipefail

compiler="${MUSL_CC:-musl-gcc}"
multiarch="$($compiler -print-multiarch)"
test -n "$multiarch"
headers="$(mktemp -d "${1:?temporary directory required}/musl-uapi.XXXXXXXX")"
# Do not add /usr/include: that would mix glibc and musl standard headers.
cp -R /usr/include/linux /usr/include/asm-generic "$headers/"
cp -R "/usr/include/$multiarch/asm" "$headers/"
printf '#include <linux/random.h>\nint main(void) { return 0; }\n' |
    "$compiler" -idirafter "$headers" -x c -c -o "$headers/probe.o" -
printf '%s\n' "$headers"
