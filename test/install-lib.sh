#!/bin/sh
set -eu

. ./subr.sh
micropython=$(find_micropython)
topdir=$(realpath ..)
libdir=$("$micropython" -c "import sys; print([x for x in sys.path if x.endswith('/lib')][0])")

copy() {
    local src="$1"
    local dst="$2"

    srcdir="$(realpath "$topdir/$src")"
    dstdir="$libdir/$dst"

    #printf "clear %s\n" "$dstdir"
    #rm -rf "$dstdir"

    printf "copy %s to %s\n" "$srcdir" "$libdir"
    mkdir -p "$dstdir"
    tar -c -C "$srcdir" -f - . | tar -x -C "$dstdir" -f -
}
copy src/sinetstream sinetstream
copy src/uavro uavro
copy plugins/broker/mqtt/src/sinetstreamplugin sinetstreamplugin

exit 0
