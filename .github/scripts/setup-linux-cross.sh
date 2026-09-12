#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo 'Usage: setup-linux-cross.sh TARGET' >&2
    exit 2
fi
: "${GITHUB_ENV:?This setup script requires a GitHub Actions runner}"

case "$1" in
    i686-unknown-linux-gnu)
        sudo apt-get install --yes gcc-multilib libc6-dev-i386
        {
            echo 'CARGO_TARGET_I686_UNKNOWN_LINUX_GNU_LINKER=gcc'
            echo 'CC_i686_unknown_linux_gnu=gcc'
            echo 'CFLAGS_i686_unknown_linux_gnu=-m32'
            echo 'PKG_CONFIG_LIBDIR=/usr/lib/i386-linux-gnu/pkgconfig'
        } >> "$GITHUB_ENV"
        ;;
    armv7-unknown-linux-gnueabihf)
        sudo apt-get install --yes gcc-arm-linux-gnueabihf libc6-dev-armhf-cross qemu-user
        {
            echo 'CARGO_TARGET_ARMV7_UNKNOWN_LINUX_GNUEABIHF_LINKER=arm-linux-gnueabihf-gcc'
            echo 'CARGO_TARGET_ARMV7_UNKNOWN_LINUX_GNUEABIHF_RUNNER=qemu-arm -L /usr/arm-linux-gnueabihf'
            echo 'CC_armv7_unknown_linux_gnueabihf=arm-linux-gnueabihf-gcc'
            echo 'AR_armv7_unknown_linux_gnueabihf=arm-linux-gnueabihf-ar'
            echo 'PKG_CONFIG_LIBDIR=/usr/arm-linux-gnueabihf/lib/pkgconfig'
        } >> "$GITHUB_ENV"
        ;;
    *) echo "Unsupported 32-bit target: $1" >&2; exit 2 ;;
esac
# Do not accidentally link host x86_64 libraries into a 32-bit binary.
# With no target libudev .pc installed, vendored libusb uses its netlink backend.
{
    echo 'PKG_CONFIG_ALLOW_CROSS=1'
    echo 'PKG_CONFIG_PATH='
} >> "$GITHUB_ENV"
