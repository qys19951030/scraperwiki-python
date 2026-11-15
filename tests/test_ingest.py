"""
Tests for the long-running-task batch ingestion channel
(``scraperwiki.ingest.BatchWriter``).
"""

import os
import sqlite3
import sys
from subprocess import PIPE, Popen
from textwrap import dedent
from unittest import TestCase

import scraperwiki
from scraperwiki.ingest import BatchWriter

DB_NAME = "scraperwiki.sqlite"


class IngestTestCase(TestCase):
    """Ensure a clean database and a reset shared connection state per test."""

    def setUp(self):
        state = scraperwiki.sql._State
        if state._connection:
            state._connection.close()
        state._connection = None
        if state.engine:
            state.engine.dispose()
        state.engine = None
        state._transaction = None
        state.metadata = None
        state.table = None
        state.table_pending = None
        if os.path.exists(DB_NAME):
            try:
                os.remove(DB_NAME)
            except OSError:
                pass
        super().setUp()

    def raw_connect(self):
        return sqlite3.connect(DB_NAME)

    def raw_rows(self, table, order_by="rowid"):
        quoted = '"' + table.replace('"', '""') + '"'
        with self.raw_connect() as conn:
            return conn.execute(
                f"SELECT * FROM {quoted} ORDER BY {order_by}"
            ).fetchall()

    def run_in_subprocess(self, script):
        with open(os.devnull) as null:
            process = Popen(
                [sys.executable, "-c", dedent(script)],
                stdout=PIPE,
                stderr=PIPE,
                stdin=null,
            )
        stdout, stderr = process.communicate()
        assert process.returncode == 0, stderr.decode("utf-8")
        return stdout, stderr


class TestBatchWriterShell(IngestTestCase):
    def test_constructor_validates_arguments(self):
        with self.assertRaises(ValueError):
            BatchWriter("", "t", ["id"], 10)
        with self.assertRaises(ValueError):
            BatchWriter("task", "", ["id"], 10)
        with self.assertRaises(TypeError):
            BatchWriter("task", "t", "id", 10)
        with self.assertRaises(ValueError):
            BatchWriter("task", "t", ["id"], 0)
        with self.assertRaises(ValueError):
            BatchWriter("task", "t", ["id"], -3)

    def test_pending_counts_buffered_rows(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=10)
        self.assertEqual(writer.pending(), 0)
        writer.add(0, [{"id": 1}, {"id": 2}])
        writer.add(1, [{"id": 3}])
        self.assertEqual(writer.pending(), 3)

    def test_flush_drains_buffer(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=10)
        writer.add(0, [{"id": 1}, {"id": 2}])
        writer.flush()
        self.assertEqual(writer.pending(), 0)

    def test_auto_flush_at_batch_size(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=3)
        writer.add(0, [{"id": 1}, {"id": 2}])
        self.assertEqual(writer.pending(), 2)
        writer.add(1, [{"id": 3}])
        # Reaching batch_size triggers an automatic flush.
        self.assertEqual(writer.pending(), 0)

    def test_oversized_batch_flushes(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=2)
        writer.add(0, [{"id": 1}, {"id": 2}, {"id": 3}])
        self.assertEqual(writer.pending(), 0)

    def test_context_manager_closes_writer(self):
        with BatchWriter("task", "things", ["id"], batch_size=10) as writer:
            writer.add(0, [{"id": 1}])
        self.assertTrue(writer._closed)
        with self.assertRaises(RuntimeError):
            writer.add(1, [{"id": 2}])
        with self.assertRaises(RuntimeError):
            writer.flush()

    def test_close_is_idempotent(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=10)
        writer.close()
        writer.close()

    def test_stats_start_at_zero(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=10)
        self.assertEqual(writer.stats(), {"success": 0, "failure": 0, "skipped": 0})

    def test_stats_returns_a_copy(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=10)
        stats = writer.stats()
        stats["success"] = 999
        self.assertEqual(writer.stats()["success"], 0)
