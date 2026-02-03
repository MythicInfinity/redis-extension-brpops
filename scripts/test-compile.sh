#!/usr/bin/env bash
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ "$SOURCE" != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

set -euo pipefail

ROOT_DIR="$DIR/.."
IMAGE_NAME="redis-brpops-test"
CONTAINER_NAME="redis-brpops-test-$$"

docker build -f "$ROOT_DIR/scripts/Dockerfile.redis" -t "$IMAGE_NAME" "$ROOT_DIR"
docker run -d --name "$CONTAINER_NAME" -p 6379:6379 "$IMAGE_NAME" >/dev/null

cleanup() {
    docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker exec "$CONTAINER_NAME" redis-cli brpopall a 1
