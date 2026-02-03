# Redis brpops Extension

This repository provides a custom Redis module that adds blocking list pop commands:
- `brpopall` to block until a list has data, then pop and return all elements.
- `brpopbatch` to block until a list has data, then pop and return up to `count` elements.

The module is compiled as a shared object (`brpops.so`) and loaded into Redis to extend its command set.

##### Use Cases

These are similar to the built-in `brpop` command except that they allow returning multiple elements.

This is useful in producer-consumer scenarios where there are multiple consumers and you don't want to exhaust resources unnecessarily by polling, and you want the efficiency of getting multiple results per network round trip.

## Command Reference

- `BRPOPALL key [timeout-ms]`  
  Blocks until the list at `key` has data, then pops and returns all elements. A timeout of `0` blocks indefinitely.

- `BRPOPBATCH key count [timeout-ms]`  
  Blocks until the list at `key` has data, then pops and returns up to `count` elements. A timeout of `0` blocks indefinitely.

## Examples

The `example/` directory contains a Dockerfile that builds a custom Redis image with the module compiled and loaded.

### 1) Build the image

```bash
cd redis_ext_brpops/example
docker build -t redis-brpops --build-arg GIT_TAG=master .
```

Notes:
- `GIT_TAG` can be a tag, branch, or commit SHA in the `redis_ext_brpops` repository.
- For reproducible builds, prefer a commit SHA.
- Copying the Dockerfile to your source repository is recommended.

### 2) Run Redis with the module loaded

```bash
docker run --rm -p 6379:6379 redis-brpops
```

### 3) Redis CLI examples

```bash
$ redis-cli brpopall mylist 1
(nil)
$ redis-cli rpush mylist a b c
(integer) 3
$ redis-cli brpopbatch mylist 2 1000
1) "c"
2) "b"
```

## Minimal Python Example

Create a file named `example_client.py`:

```python
import redis
from typing import List

r = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

def brpopall(conn: redis.Redis, key: str, timeout_ms: int = 0) -> List[str]:
    result = conn.execute_command("brpopall", key, timeout_ms)
    if result is None:
        raise redis.TimeoutError(f"timed out waiting for key: {key}")
    return result

def brpopbatch(conn: redis.Redis, key: str, count: int, timeout_ms: int = 0) -> List[str]:
    result = conn.execute_command("brpopbatch", key, count, timeout_ms)
    if result is None:
        raise redis.TimeoutError(f"timed out waiting for key: {key}")
    return result

# Push some values, then pop them all (blocking up to 1s)
r.rpush("mylist", "a", "b", "c")
print(brpopall(r, "mylist", 1000))

# Push again, then pop a batch of 2
r.rpush("mylist", "d", "e", "f")
print(brpopbatch(r, "mylist", 2, 1000))
```

## Recommendations

- Pin `GIT_TAG` to a commit SHA for deterministic builds.
- Copy the [Dockerfile](examples/Dockerfile) to your source directory and adapt it to your needs.
- Keep your Redis base image version aligned with your production environment.
