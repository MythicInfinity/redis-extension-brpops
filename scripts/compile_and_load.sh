#!/usr/bin/env bash
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ "$SOURCE" != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

set -euo pipefail

SRC_DIR="/opt/redis_brpops/src"
OUT_DIR="/tmp"

if [ ! -d "$SRC_DIR" ]; then
    echo "ERR: expected source directory at $SRC_DIR" >&2
    exit 1
fi

# apk add --no-cache gcc libc-dev

cd "$SRC_DIR"
gcc -W -Wall -fno-common -g -ggdb -std=c99 -O2 -fPIC -c -o "$OUT_DIR/brpops.o" redis_brpops.c
ld -o "$OUT_DIR/brpops.so" "$OUT_DIR/brpops.o" -shared

redis-cli module unload brpopall >/dev/null 2>&1 || true
redis-cli module load "$OUT_DIR/brpops.so"
