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


class TestBatchPersistence(IngestTestCase):
    def test_manual_flush_writes_rows(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=100)
        writer.add(0, [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])
        # Still buffered: a separate connection must not see anything yet.
        with self.raw_connect() as conn:
            self.assertEqual(
                conn.execute(
                    'SELECT name FROM sqlite_master WHERE type="table" '
                    'AND name="things"'
                ).fetchall(),
                [],
            )
        writer.flush()
        self.assertEqual(
            self.raw_rows("things"),
            [(1, "a"), (2, "b")],
        )
        self.assertEqual(writer.stats()["success"], 2)
        writer.close()

    def test_auto_flush_at_batch_size_persists(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=3)
        writer.add(0, [{"id": 1}, {"id": 2}])
        writer.add(1, [{"id": 3}])
        self.assertEqual(writer.pending(), 0)
        self.assertEqual(self.raw_rows("things"), [(1,), (2,), (3,)])
        writer.close()

    def test_context_manager_flushes_on_exit(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1}, {"id": 2}])
        self.assertEqual(self.raw_rows("things"), [(1,), (2,)])

    def test_new_columns_are_added(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=100)
        writer.add(0, [{"id": 1, "a": "x"}])
        writer.flush()
        writer.add(1, [{"id": 2, "a": "y", "b": 42}])
        writer.flush()
        with self.raw_connect() as conn:
            self.assertEqual(
                conn.execute("SELECT id, a, b FROM things ORDER BY id").fetchall(),
                [(1, "x", None), (2, "y", 42)],
            )
        writer.close()

    def test_unique_index_is_created(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "v": "a"}])
        with self.raw_connect() as conn:
            indices = conn.execute("PRAGMA index_list(things)").fetchall()
        self.assertTrue(
            any(row[1] == "things_id_unique" and row[2] == 1 for row in indices),
            indices,
        )

    def test_same_primary_key_replaces_row(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "v": "first"}])
            writer.flush()
            writer.add(1, [{"id": 1, "v": "second"}])
        self.assertEqual(self.raw_rows("things"), [(1, "second")])

    def test_multiple_batches_one_transaction(self):
        with BatchWriter("task", "things", ["id"], batch_size=2) as writer:
            writer.add(0, [{"id": 1}, {"id": 2}])
            self.assertEqual(self.raw_rows("things"), [(1,), (2,)])
            writer.add(1, [{"id": 3}])
            writer.flush()
        self.assertEqual(self.raw_rows("things"), [(1,), (2,), (3,)])

    def test_non_mapping_row_raises(self):
        # Quarantine handling arrives in a later commit; for now a bad row
        # aborts the flush and leaves nothing from that flush behind.
        writer = BatchWriter("task", "things", ["id"], batch_size=100)
        writer.add(0, [{"id": 1}, ["not", "a", "dict"], {"id": 2}])
        with self.assertRaises(TypeError):
            writer.flush()
        with self.raw_connect() as conn:
            self.assertEqual(
                conn.execute(
                    'SELECT name FROM sqlite_master WHERE type="table" '
                    'AND name="things"'
                ).fetchall(),
                [],
            )
        writer.close()

    def test_separate_sqlite_connection_sees_committed_rows(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "v": "x"}])
            writer.flush()
        with self.raw_connect() as conn:
            self.assertEqual(
                conn.execute("SELECT id, v FROM things").fetchall(), [(1, "x")]
            )
