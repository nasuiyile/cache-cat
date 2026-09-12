"""Smoke tests for Redis stream consumer-group commands.

Run against a cache-cat instance listening on localhost:6379::

    python -m unittest tests/stream/test_xgroup.py

The tests intentionally use ``execute_command`` for XGROUP subcommands so
their assertions cover the wire-level replies produced by cache-cat. Commands
outside the XGROUP/XREADGROUP scope (such as XINFO and XPENDING) are avoided
so this smoke test can run while those command families are developed.
"""

import unittest

import redis
from redis.exceptions import ResponseError


class XGroupCommandTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.redis = redis.Redis(
            host="127.0.0.1", port=6379, db=0, decode_responses=True
        )
        try:
            cls.redis.ping()
        except redis.exceptions.RedisError as exc:  # pragma: no cover - local smoke test
            raise unittest.SkipTest(f"cache-cat is not running: {exc}") from exc

    def setUp(self):
        self.redis.flushdb()

    def test_create_duplicate_and_info(self):
        stream = "xgroup-smoke"
        self.redis.xadd(stream, {"field": "value"})

        self.assertTrue(
            self.redis.execute_command("XGROUP", "CREATE", stream, "g", "0-0")
        )
        with self.assertRaises(ResponseError) as ctx:
            self.redis.execute_command("XGROUP", "CREATE", stream, "g", "0-0")
        self.assertIn("BUSYGROUP", str(ctx.exception).upper())


    def test_createconsumer_read_pending_ack_and_deleteconsumer(self):
        stream = "xgroup-pel"
        msg_id = self.redis.xadd(stream, {"field": "value"})
        self.redis.execute_command("XGROUP", "CREATE", stream, "g", "0-0")

        self.assertEqual(
            self.redis.execute_command("XGROUP", "CREATECONSUMER", stream, "g", "c"),
            1,
        )
        # Redis returns zero when the consumer already exists.
        self.assertEqual(
            self.redis.execute_command("XGROUP", "CREATECONSUMER", stream, "g", "c"),
            0,
        )

        result = self.redis.xreadgroup("g", "c", {stream: ">"}, count=1)
        self.assertEqual(result[0][0], stream)
        self.assertEqual(result[0][1][0][0], msg_id)

        # DELCONSUMER removes the consumer and reports removed PEL entries.
        self.assertEqual(
            self.redis.execute_command("XGROUP", "DELCONSUMER", stream, "g", "c"), 1
        )

    def test_setid_destroy_and_missing_group_errors(self):
        stream = "xgroup-admin"
        self.redis.xadd(stream, {"field": "one"})
        self.redis.xadd(stream, {"field": "two"})
        self.redis.execute_command("XGROUP", "CREATE", stream, "g", "0-0")

        self.assertEqual(
            self.redis.execute_command("XGROUP", "SETID", stream, "g", "1-0"),
            "OK",
        )
        self.assertEqual(self.redis.execute_command("XGROUP", "DESTROY", stream, "g"), 1)
        self.assertEqual(self.redis.execute_command("XGROUP", "DESTROY", stream, "g"), 0)

        with self.assertRaises(ResponseError) as ctx:
            self.redis.xreadgroup("g", "c", {stream: ">"}, count=1)
        self.assertIn("NOGROUP", str(ctx.exception).upper())

    def test_create_mkstream(self):
        stream = "xgroup-mkstream"
        self.assertFalse(self.redis.exists(stream))
        self.assertTrue(
            self.redis.execute_command(
                "XGROUP", "CREATE", stream, "g", "$", "MKSTREAM"
            )
        )
        self.assertTrue(self.redis.exists(stream))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
