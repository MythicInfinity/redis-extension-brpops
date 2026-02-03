FROM redis:7.4-alpine AS builder

RUN apk add --no-cache gcc libc-dev git


ARG GIT_TAG=master

RUN mkdir -p /opt/redis-extension-brpops
WORKDIR /opt/redis-extension-brpops

# Support branches, tags, and commit hashes in GIT_TAG:
# 1. Initialize empty repo
# 2. Add remote
# 3. Fetch the specific tag/branch/hash with depth 1
# 4. Checkout that specific point (FETCH_HEAD)
RUN git init \
 && git remote add origin https://github.com/MythicInfinity/redis-extension-brpops.git \
 && git fetch --depth 1 origin "${GIT_TAG}" \
 && git checkout FETCH_HEAD

RUN cd /opt/redis-extension-brpops/src \
    && gcc -W -Wall -fno-common -g -ggdb -std=c99 -O2 -fPIC -c -o /tmp/brpops.o redis_brpops.c \
    && ld -o /tmp/brpops.so /tmp/brpops.o -shared

FROM redis:7.4-alpine

COPY --from=builder /tmp/brpops.so /tmp/brpops.so

CMD ["redis-server", "--loadmodule", "/tmp/brpops.so", "--loglevel", "warning"]
