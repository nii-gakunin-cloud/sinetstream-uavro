#!/bin/sh
set -eu

./install-lib.sh

. ./subr.sh
micropython=$(find_micropython)

for X in test_*.py; do
    "$micropython" "$X"
done
