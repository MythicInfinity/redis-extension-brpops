#!/usr/bin/env bash
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  [[ "$SOURCE" != /* ]] && SOURCE="$DIR/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

# execute these on the host shell from the repo root dir to load the modules
docker cp forge/shared/redis_c_modules/redis_brpops.c hack-redis-1:/tmp/redis_brpops.c
docker cp forge/shared/redis_c_modules/redismodule.h hack-redis-1:/tmp/redismodule.h

docker exec -it hack-redis-1 apk add gcc libc-dev
docker exec -it hack-redis-1 ash -c 'cd /tmp && gcc -W -Wall -fno-common -g -ggdb -std=c99 -O2 -fPIC -c -o brpops.o redis_brpops.c'
docker exec -it hack-redis-1 ash -c 'cd /tmp && ld -o brpops.so brpops.o -shared'
docker exec -it hack-redis-1 redis-cli module load /tmp/brpops.so


## legacy stuff below

docker exec -it hack-redis-1 redis-cli

# recompile
docker cp forge/shared/redis/redis_brpops.c hack-redis-1:/tmp/redis_brpops.c
docker exec -it hack-redis-1 ash -c 'cd /tmp && gcc -W -Wall -fno-common -g -ggdb -std=c99 -O2 -fPIC -c -o brpops.o redis_brpops.c'
docker exec -it hack-redis-1 ash -c 'cd /tmp && ld -o brpops.so brpops.o -shared'
docker exec -it hack-redis-1 redis-cli module load /tmp/brpops.so


cd /tmp
apk add gcc libc-dev
gcc -W -Wall -fno-common -g -ggdb -std=c99 -O2 -fPIC -c -o brpopall.o redis_bpopall.c
ld -o brpopall.so brpopall.o -shared


redis-cli
MODULE LOAD /tmp/brpopall.so
