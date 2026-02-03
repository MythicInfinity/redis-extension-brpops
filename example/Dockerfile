FROM redis:7.4-alpine AS builder

ARG GIT_TAG=main

RUN apk add --no-cache gcc libc-dev git

WORKDIR /opt
RUN git clone --depth 1 --branch "${GIT_TAG}" https://github.com/MythicInfinity/redis-extension-brpops.git

RUN cd /opt/redis_ext_brpops/src \
    && gcc -W -Wall -fno-common -g -ggdb -std=c99 -O2 -fPIC -c -o /tmp/brpops.o redis_brpops.c \
    && ld -o /tmp/brpops.so /tmp/brpops.o -shared

FROM redis:7.4-alpine

COPY --from=builder /tmp/brpops.so /tmp/brpops.so

CMD ["redis-server", "--loadmodule", "/tmp/brpops.so", "--loglevel", "warning"]
