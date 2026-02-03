import asyncio
import multiprocessing
import os
import random
import string
import time
import unittest

import pytest
import redis.asyncio as redis
from redis.exceptions import ResponseError

REBLOCK_TEST_ITERATIONS = 4
SMALL_TIMEOUT_MS = 100

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


async def _execute_brpopbatch(key: str, count: int, timeout_ms: int):
    r = _get_redis_client()
    try:
        return await r.execute_command("brpopbatch", key, count, timeout_ms)
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
        """brpopall returns NULL (None) on timeout when no list elements are available."""
        result = await self.r.execute_command("brpopall", self.qname, SMALL_TIMEOUT_MS)
        self.assertIsNone(result)

    async def test_brpopbatch_timeout_returns_null(self):
        """brpopbatch returns NULL (None) on timeout when no list elements are available."""
        result = await self.r.execute_command("brpopbatch", self.qname, 2, SMALL_TIMEOUT_MS)
        self.assertIsNone(result)

    async def test_brpopall_return_order(self):
        """brpopall returns elements in RPOP-like order and clears the list."""
        await self.r.rpush(self.qname, 0, 1, 2)
        result = await self.r.execute_command("brpopall", self.qname, SMALL_TIMEOUT_MS)
        self.assertEqual(result, [b"2", b"1", b"0"])
        self.assertEqual(await self.r.llen(self.qname), 0)

    async def test_brpopbatch_return_order(self):
        """brpopbatch returns elements in RPOP-like order for count pops."""
        await self.r.rpush(self.qname, 0, 1, 2)
        result = await self.r.execute_command("brpopbatch", self.qname, 2, SMALL_TIMEOUT_MS)
        self.assertEqual(result, [b"2", b"1"])
        result = await self.r.execute_command("brpopbatch", self.qname, 2, SMALL_TIMEOUT_MS)
        self.assertEqual(result, [b"0"])

    async def test_wrong_type_error(self):
        """brpop commands error when the key holds a non-list value."""
        await self.r.set(self.qname, "not-a-list")
        with self.assertRaises(ResponseError):
            await self.r.execute_command("brpopall", self.qname, SMALL_TIMEOUT_MS)
        with self.assertRaises(ResponseError):
            await self.r.execute_command("brpopbatch", self.qname, 2, SMALL_TIMEOUT_MS)

    async def test_brpopbatch_invalid_count(self):
        """brpopbatch rejects count values less than 1."""
        with self.assertRaises(ResponseError):
            await self.r.execute_command("brpopbatch", self.qname, 0, SMALL_TIMEOUT_MS)
        with self.assertRaises(ResponseError):
            await self.r.execute_command("brpopbatch", self.qname, -1, SMALL_TIMEOUT_MS)

    async def test_brpopbatch_large_count_returns_all(self):
        """brpopbatch returns all elements if count exceeds list length."""
        await self.r.rpush(self.qname, 0, 1, 2)
        result = await self.r.execute_command("brpopbatch", self.qname, 100, SMALL_TIMEOUT_MS)
        self.assertEqual(result, [b"2", b"1", b"0"])
        self.assertEqual(await self.r.llen(self.qname), 0)

    async def test_brpops_order_matches_rpop(self):
        """brpopall/brpopbatch ordering matches RPOP on identical data."""
        await self.r.rpush(self.qname, 0, 1, 2, 3, 4)
        rpop_result = await self.r.rpop(self.qname, 5)

        await self.r.delete(self.qname)
        await self.r.rpush(self.qname, 0, 1, 2, 3, 4)
        brpopbatch_result = await self.r.execute_command("brpopbatch", self.qname, 5, SMALL_TIMEOUT_MS)

        await self.r.delete(self.qname)
        await self.r.rpush(self.qname, 0, 1, 2, 3, 4)
        brpopall_result = await self.r.execute_command("brpopall", self.qname, SMALL_TIMEOUT_MS)

        self.assertEqual(brpopbatch_result, rpop_result)
        self.assertEqual(brpopall_result, rpop_result)

    async def test_timeout_duration_bounds(self):
        """Timeouts should not return immediately and should roughly respect the duration."""
        start = time.time()
        result = await self.r.execute_command("brpopall", self.qname, 200)
        elapsed = time.time() - start
        self.assertIsNone(result)
        self.assertGreaterEqual(elapsed, 0.15)


@pytest.mark.redis_ext_test
class RedisBrpopsExtConcurrentTestCases(unittest.TestCase):
    def _assert_no_worker_errors(self, results):
        errors = [res for res in results if isinstance(res, dict) and "error" in res]
        self.assertEqual(errors, [])

    def test_brpopall_concurrent_both_timeout(self):
        """Two concurrent brpopall consumers should both timeout on an empty list."""
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

        self._assert_no_worker_errors(results)
        self.assertEqual(results.count(None), 2)

    def test_brpopall_concurrent_one_timeout(self):
        """Two brpopall consumers, one gets data, the other times out (re-block path)."""
        host = os.environ.get("REDIS_HOST", "redis")
        port = int(os.environ.get("REDIS_PORT", "6379"))
        ctx = multiprocessing.get_context("spawn")

        for _ in range(REBLOCK_TEST_ITERATIONS):
            _set_isolated_test_rnd_seed()
            key = f"testq::{''.join(random.choices(string.ascii_letters + string.digits, k=10))}"
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

            self._assert_no_worker_errors(results)
            self.assertEqual(results.count(None), 1)
            data_results = [res for res in results if isinstance(res, list)]
            self.assertEqual(data_results, [[b"b", b"a"]])

    def test_brpopbatch_concurrent_one_timeout(self):
        """Two brpopbatch consumers, one gets data, the other times out."""
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

        self._assert_no_worker_errors(results)
        self.assertEqual(results.count(None), 1)
        data_results = [res for res in results if isinstance(res, list)]
        self.assertEqual(data_results, [[b"2", b"1"]])

    def test_multiple_consumers_multiple_batches(self):
        """Three brpopbatch consumers should each receive a batch from a larger push of 8 values."""
        _set_isolated_test_rnd_seed()
        key = f"testq::{''.join(random.choices(string.ascii_letters + string.digits, k=10))}"
        host = os.environ.get("REDIS_HOST", "redis")
        port = int(os.environ.get("REDIS_PORT", "6379"))
        ctx = multiprocessing.get_context("spawn")
        result_q = ctx.Queue()

        procs = [
            ctx.Process(target=_worker_brpopbatch, args=(host, port, key, 2, 1000, result_q))
            for _ in range(3)
        ]
        for proc in procs:
            proc.start()

        time.sleep(0.1)
        asyncio.run(_push_values(key, "1", "2", "3", "4", "5", "6", "7", "8"))

        results = [result_q.get(timeout=3) for _ in range(3)]

        for proc in procs:
            proc.join(timeout=1)

        asyncio.run(_delete_key(key))

        self._assert_no_worker_errors(results)
        self.assertEqual(results.count(None), 0)
        flattened = [item for res in results for item in res]
        self.assertEqual(len(flattened), 6)
        remaining = asyncio.run(_execute_brpopbatch(key, 10, SMALL_TIMEOUT_MS))
        self.assertEqual(len(remaining), 2)
        combined = sorted(flattened + remaining)
        self.assertEqual(combined, sorted([b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8"]))

    def test_mixed_commands_same_key(self):
        """brpopall and brpopbatch on the same key should both return valid list data."""
        _set_isolated_test_rnd_seed()
        key = f"testq::{''.join(random.choices(string.ascii_letters + string.digits, k=10))}"
        host = os.environ.get("REDIS_HOST", "redis")
        port = int(os.environ.get("REDIS_PORT", "6379"))
        ctx = multiprocessing.get_context("spawn")
        result_q = ctx.Queue()

        proc_all = ctx.Process(target=_worker_brpopall, args=(host, port, key, 500, result_q))
        proc_batch = ctx.Process(target=_worker_brpopbatch, args=(host, port, key, 2, 500, result_q))
        proc_all.start()
        proc_batch.start()

        time.sleep(0.1)
        asyncio.run(_push_values(key, "a", "b", "c"))

        results = [result_q.get(timeout=3), result_q.get(timeout=3)]

        proc_all.join(timeout=1)
        proc_batch.join(timeout=1)

        asyncio.run(_delete_key(key))

        self._assert_no_worker_errors(results)
        data_results = [res for res in results if isinstance(res, list)]
        none_results = results.count(None)
        self.assertIn(none_results, [0, 1])
        flattened = [item for res in data_results for item in res]
        self.assertTrue(all(val in [b"a", b"b", b"c"] for val in flattened))

    def test_brpopall_block_forever_until_push(self):
        """timeout=0 should block until an element is pushed, then return it."""
        _set_isolated_test_rnd_seed()
        key = f"testq::{''.join(random.choices(string.ascii_letters + string.digits, k=10))}"
        host = os.environ.get("REDIS_HOST", "redis")
        port = int(os.environ.get("REDIS_PORT", "6379"))
        ctx = multiprocessing.get_context("spawn")
        result_q = ctx.Queue()

        proc = ctx.Process(target=_worker_brpopall, args=(host, port, key, 0, result_q))
        proc.start()

        time.sleep(1.1)
        self.assertTrue(proc.is_alive())

        asyncio.run(_push_values(key, "x"))
        result = result_q.get(timeout=3)

        proc.join(timeout=1)
        asyncio.run(_delete_key(key))

        self._assert_no_worker_errors([result])
        self.assertEqual(result, [b"x"])
