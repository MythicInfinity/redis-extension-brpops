import asyncio
import multiprocessing
import os
import random
import string
import time
import unittest

import pytest
import redis.asyncio as redis

def _set_isolated_test_rnd_seed() -> None:
    random.seed((os.getpid() * time.time_ns()) % (2**32))


def _get_redis_client() -> redis.Redis:
    return redis.Redis(
        host=os.environ.get("REDIS_HOST", "redis"),
        port=int(os.environ.get("REDIS_PORT", "6379")),
        decode_responses=False
    )


async def _push_values(key: str, *values) -> None:
    r = _get_redis_client()
    try:
        await r.rpush(key, *values)
    finally:
        await r.aclose()


async def _delete_key(key: str) -> None:
    r = _get_redis_client()
    try:
        await r.delete(key)
    finally:
        await r.aclose()


def _worker_brpopall(host: str, port: int, key: str, timeout_ms: int, result_q):
    async def _run():
        r = redis.Redis(host=host, port=port)
        try:
            result = await r.execute_command("brpopall", key, timeout_ms)
            result_q.put(result)
        except Exception as exc:
            result_q.put({"error": repr(exc)})
        finally:
            await r.aclose()

    asyncio.run(_run())


def _worker_brpopbatch(host: str, port: int, key: str, count: int, timeout_ms: int, result_q):
    async def _run():
        r = redis.Redis(host=host, port=port)
        try:
            result = await r.execute_command("brpopbatch", key, count, timeout_ms)
            result_q.put(result)
        except Exception as exc:
            result_q.put({"error": repr(exc)})
        finally:
            await r.aclose()

    asyncio.run(_run())


@pytest.mark.redis_ext_test
class RedisBrpopsExtAsyncTestCases(unittest.IsolatedAsyncioTestCase):
    r: redis.Redis

    async def asyncSetUp(self) -> None:
        _set_isolated_test_rnd_seed()
        self.r = _get_redis_client()
        self.qname = f"testq::{''.join(random.choices(string.ascii_letters + string.digits, k=10))}"

    async def asyncTearDown(self):
        await self.r.delete(self.qname)
        await self.r.aclose()

    async def test_brpopall_timeout_returns_null(self):
        result = await self.r.execute_command("brpopall", self.qname, 100)
        self.assertIsNone(result)

    async def test_brpopbatch_timeout_returns_null(self):
        result = await self.r.execute_command("brpopbatch", self.qname, 2, 100)
        self.assertIsNone(result)

    async def test_brpopall_return_order(self):
        await self.r.rpush(self.qname, 0, 1, 2)
        result = await self.r.execute_command("brpopall", self.qname, 0)
        self.assertEqual(result, [b"2", b"1", b"0"])
        self.assertEqual(await self.r.llen(self.qname), 0)

    async def test_brpopbatch_return_order(self):
        await self.r.rpush(self.qname, 0, 1, 2)
        result = await self.r.execute_command("brpopbatch", self.qname, 2, 0)
        self.assertEqual(result, [b"2", b"1"])
        result = await self.r.execute_command("brpopbatch", self.qname, 2, 0)
        self.assertEqual(result, [b"0"])


@pytest.mark.redis_ext_test
class RedisBrpopsExtConcurrentTestCases(unittest.TestCase):
    def test_brpopall_concurrent_both_timeout(self):
        _set_isolated_test_rnd_seed()
        key = f"testq::{''.join(random.choices(string.ascii_letters + string.digits, k=10))}"
        host = os.environ.get("REDIS_HOST", "redis")
        port = int(os.environ.get("REDIS_PORT", "6379"))
        ctx = multiprocessing.get_context("spawn")
        result_q = ctx.Queue()

        proc1 = ctx.Process(target=_worker_brpopall, args=(host, port, key, 200, result_q))
        proc2 = ctx.Process(target=_worker_brpopall, args=(host, port, key, 200, result_q))
        proc1.start()
        proc2.start()

        results = [result_q.get(timeout=3), result_q.get(timeout=3)]

        proc1.join(timeout=1)
        proc2.join(timeout=1)

        asyncio.run(_delete_key(key))

        self.assertEqual(results.count(None), 2)

    def test_brpopall_concurrent_one_timeout(self):
        _set_isolated_test_rnd_seed()
        key = f"testq::{''.join(random.choices(string.ascii_letters + string.digits, k=10))}"
        host = os.environ.get("REDIS_HOST", "redis")
        port = int(os.environ.get("REDIS_PORT", "6379"))
        ctx = multiprocessing.get_context("spawn")
        result_q = ctx.Queue()

        proc1 = ctx.Process(target=_worker_brpopall, args=(host, port, key, 500, result_q))
        proc2 = ctx.Process(target=_worker_brpopall, args=(host, port, key, 500, result_q))
        proc1.start()
        proc2.start()

        time.sleep(0.1)
        asyncio.run(_push_values(key, "a", "b"))

        results = [result_q.get(timeout=3), result_q.get(timeout=3)]

        proc1.join(timeout=1)
        proc2.join(timeout=1)

        asyncio.run(_delete_key(key))

        self.assertEqual(results.count(None), 1)
        data_results = [res for res in results if isinstance(res, list)]
        self.assertEqual(data_results, [[b"b", b"a"]])

    def test_brpopbatch_concurrent_one_timeout(self):
        _set_isolated_test_rnd_seed()
        key = f"testq::{''.join(random.choices(string.ascii_letters + string.digits, k=10))}"
        host = os.environ.get("REDIS_HOST", "redis")
        port = int(os.environ.get("REDIS_PORT", "6379"))
        ctx = multiprocessing.get_context("spawn")
        result_q = ctx.Queue()

        proc1 = ctx.Process(target=_worker_brpopbatch, args=(host, port, key, 2, 500, result_q))
        proc2 = ctx.Process(target=_worker_brpopbatch, args=(host, port, key, 2, 500, result_q))
        proc1.start()
        proc2.start()

        time.sleep(0.1)
        asyncio.run(_push_values(key, "1", "2"))

        results = [result_q.get(timeout=3), result_q.get(timeout=3)]

        proc1.join(timeout=1)
        proc2.join(timeout=1)

        asyncio.run(_delete_key(key))

        self.assertEqual(results.count(None), 1)
        data_results = [res for res in results if isinstance(res, list)]
        self.assertEqual(data_results, [[b"2", b"1"]])
