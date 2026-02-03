# Redis brpops Example (Custom Image)

This example builds a custom Redis image that compiles and loads the `brpopall`/`brpopbatch` module at image build time.

## Build

```bash
docker build -t redis-brpops \
  --build-arg GIT_TAG=main \
  .
```

Notes:
- `GIT_TAG` should be a git tag, branch, or commit SHA from the `redis_ext_brpops` repository.
- For reproducible builds, prefer a commit SHA.

## Run

```bash
docker run --rm -p 6379:6379 redis-brpops
```

## Smoke Test

```bash
redis-cli brpopall mylist 1
```
