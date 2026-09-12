#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 3 ]]; then
    echo 'Usage: package-release.sh TARGET TAG BINARY' >&2
    exit 2
fi

target=$1
tag=$2
binary=$3
case "$target" in
    x86_64-unknown-linux-gnu|aarch64-unknown-linux-gnu|i686-unknown-linux-gnu|armv7-unknown-linux-gnueabihf|x86_64-apple-darwin|aarch64-apple-darwin) ;;
    *) echo "Unsupported release target: $target" >&2; exit 2 ;;
esac
test -x "$binary"
# Tags can contain slashes and shell punctuation. Use a safe archive name while
# retaining the original tag as the GitHub Release identity.
safe_tag=$(printf '%s' "$tag" | LC_ALL=C tr -c 'A-Za-z0-9._-' '-')
name="iot-power-tui-${safe_tag}-${target}"
staging=$(mktemp -d)
trap 'rm -rf "$staging"' EXIT
mkdir -p "$staging/$name" dist
cp "$binary" "$staging/$name/iot-power-tui"
chmod 755 "$staging/$name/iot-power-tui"
cp README.md LICENSE "$staging/$name/"
cp -R docs "$staging/$name/docs"
tar -czf "dist/$name.tar.gz" -C "$staging" "$name"
printf 'Created dist/%s.tar.gz\n' "$name"
