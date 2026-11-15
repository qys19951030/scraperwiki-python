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

    def test_separate_sqlite_connection_sees_committed_rows(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "v": "x"}])
            writer.flush()
        with self.raw_connect() as conn:
            self.assertEqual(
                conn.execute("SELECT id, v FROM things").fetchall(), [(1, "x")]
            )


class TestCheckpoints(IngestTestCase):
    def test_checkpoint_starts_at_minus_one(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=100)
        self.assertEqual(writer.checkpoint, -1)
        writer.close()

    def test_checkpoint_advances_after_flush(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=100)
        writer.add(0, [{"id": 1}])
        writer.add(2, [{"id": 2}])
        writer.flush()
        self.assertEqual(writer.checkpoint, 2)
        writer.close()

    def test_checkpoint_readable_via_get_var(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1}])
        self.assertEqual(scraperwiki.sql.get_var("swingest:checkpoint:task"), 0)

    def test_batches_at_or_below_checkpoint_are_skipped(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=100)
        writer.add(0, [{"id": 1}, {"id": 2}])
        writer.flush()
        writer.add(0, [{"id": 1}, {"id": 2}])  # exact checkpoint
        writer.add(-1, [{"id": 9}])  # below checkpoint
        self.assertEqual(writer.pending(), 0)
        self.assertEqual(
            writer.stats(),
            {"success": 2, "failure": 0, "skipped": 3},
        )
        self.assertEqual(self.raw_rows("things"), [(1,), (2,)])
        writer.close()

    def test_writer_resumes_after_reconstruction(self):
        writer = BatchWriter("task", "things", ["id"], batch_size=100)
        writer.add(0, [{"id": 1}])
        writer.add(1, [{"id": 2}])
        writer.close()

        # Simulate a scraper restart: brand-new writer, same task name.
        restarted = BatchWriter("task", "things", ["id"], batch_size=100)
        self.assertEqual(restarted.checkpoint, 1)
        # Replaying an old batch must not duplicate rows.
        restarted.add(1, [{"id": 2}])
        restarted.add(2, [{"id": 3}])
        restarted.close()

        self.assertEqual(restarted.stats()["skipped"], 1)
        self.assertEqual(restarted.stats()["success"], 1)
        self.assertEqual(self.raw_rows("things"), [(1,), (2,), (3,)])

    def test_resume_works_in_a_new_process(self):
        script = """
            from scraperwiki.ingest import BatchWriter
            writer = BatchWriter("task", "things", ["id"], batch_size=100)
            writer.add(0, [{"id": 1}])
            writer.add(1, [{"id": 2}])
            writer.close()
        """
        self.run_in_subprocess(script)

        script = """
            from scraperwiki.ingest import BatchWriter
            writer = BatchWriter("task", "things", ["id"], batch_size=100)
            assert writer.checkpoint == 1, writer.checkpoint
            writer.add(0, [{"id": 1}])
            writer.add(2, [{"id": 3}])
            writer.close()
            assert writer.stats()["skipped"] == 1
            assert writer.stats()["success"] == 1
        """
        self.run_in_subprocess(script)

        self.assertEqual(self.raw_rows("things"), [(1,), (2,), (3,)])

    def test_checkpoints_are_isolated_per_task(self):
        first = BatchWriter("task-a", "things", ["id"], batch_size=100)
        first.add(0, [{"id": 1}])
        first.close()

        second = BatchWriter("task-b", "things", ["id"], batch_size=100)
        self.assertEqual(second.checkpoint, -1)
        second.add(0, [{"id": 2}])
        second.close()

        recreated = BatchWriter("task-a", "things", ["id"], batch_size=100)
        self.assertEqual(recreated.checkpoint, 0)
        recreated.close()

    def test_checkpoint_and_rows_commit_atomically(self):
        # A flush that fails after rows were written must roll back rows,
        # quarantine records, DDL and the checkpoint together, so a retry
        # after restart does not skip a missing batch.
        writer = BatchWriter("task", "things", ["id"], batch_size=100)
        writer.add(0, [{"id": 1}, ["bad", "row"]])
        original = writer._save_checkpoint

        def failing_checkpoint(connection, batch_no):
            raise RuntimeError("simulated crash after row writes")

        writer._save_checkpoint = failing_checkpoint
        with self.assertRaises(RuntimeError):
            writer.flush()
        writer._save_checkpoint = original
        writer.close()

        restarted = BatchWriter("task", "things", ["id"], batch_size=100)
        self.assertEqual(restarted.checkpoint, -1)
        with self.raw_connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT name FROM sqlite_master "
                    "WHERE type = 'table' AND name IN ('things', 'swvariables')"
                ).fetchall(),
                [],
            )
        restarted.close()


class TestQuarantine(IngestTestCase):
    def quarantine_rows(self):
        with self.raw_connect() as conn:
            return conn.execute(
                "SELECT task, batch_no, row_content, reason "
                "FROM swquarantine ORDER BY id"
            ).fetchall()

    def test_non_mapping_row_is_quarantined(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(3, [{"id": 1}, ["not", "a", "dict"], {"id": 2}])
        self.assertEqual(self.raw_rows("things"), [(1,), (2,)])
        (quarantined,) = self.quarantine_rows()
        task, batch_no, row_content, reason = quarantined
        self.assertEqual(task, "task")
        self.assertEqual(batch_no, 3)
        self.assertEqual(row_content, '["not", "a", "dict"]')
        self.assertIn("TypeError", reason)
        self.assertEqual(
            writer.stats(),
            {"success": 2, "failure": 1, "skipped": 0},
        )

    def test_nested_structure_row_is_quarantined(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "payload": {"nested": "dict"}}])
        self.assertEqual(self.raw_rows("things"), [])
        (quarantined,) = self.quarantine_rows()
        self.assertEqual(quarantined[1], 0)
        self.assertIn('{"nested": "dict"}', quarantined[2])
        self.assertIn("Error", quarantined[3])
        self.assertEqual(writer.stats()["failure"], 1)

    def test_bad_rows_do_not_block_other_batches(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1}, object()])
            writer.add(1, [{"id": 2}, {"id": 3}])
        self.assertEqual(self.raw_rows("things"), [(1,), (2,), (3,)])
        self.assertEqual(len(self.quarantine_rows()), 1)
        self.assertEqual(writer.checkpoint, 1)
        self.assertEqual(
            writer.stats(),
            {"success": 3, "failure": 1, "skipped": 0},
        )

    def test_batch_with_bad_rows_still_checkpoints(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(5, [{"id": 1}, "bad"])
        self.assertEqual(writer.checkpoint, 5)

        restarted = BatchWriter("task", "things", ["id"], batch_size=100)
        restarted.add(5, [{"id": 1}, "bad"])
        restarted.close()
        # Replaying the completed batch skips it entirely: the bad row is
        # not quarantined a second time.
        self.assertEqual(restarted.stats()["skipped"], 2)
        self.assertEqual(len(self.quarantine_rows()), 1)

    def test_quarantine_isolated_per_writer_task(self):
        with BatchWriter("task-a", "things", ["id"], batch_size=100) as writer:
            writer.add(0, ["bad-a"])
        with BatchWriter("task-b", "things", ["id"], batch_size=100) as writer:
            writer.add(0, ["bad-b"])
        rows = self.quarantine_rows()
        self.assertEqual([row[0] for row in rows], ["task-a", "task-b"])


class TestTypeAdaptation(IngestTestCase):
    def test_string_with_leading_zero_round_trips(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "code": 1234}])
            writer.flush()
            writer.add(1, [{"id": 2, "code": "01234"}])
        self.assertEqual(
            self.raw_rows("things", order_by="id"),
            [(1, 1234), (2, "01234")],
        )
        # A fresh sqlite3 connection must see the original storage classes.
        with self.raw_connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT code, typeof(code) FROM things ORDER BY id"
                ).fetchall(),
                [(1234, "integer"), ("01234", "text")],
            )

    def test_type_change_preserves_history_and_index(self):
        # Table born with a typed (INTEGER) column and a unique index, as
        # produced by the legacy sql.save path.
        with self.raw_connect() as conn:
            conn.execute("CREATE TABLE things (id INTEGER, code INTEGER)")
            conn.execute("CREATE UNIQUE INDEX things_id_unique ON things(id)")
            conn.execute("INSERT INTO things VALUES (1, 1234)")
            conn.execute("INSERT INTO things VALUES (2, 5678)")

        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 3, "code": "01234"}])

        # Historical integer rows survive untouched and the new string
        # reads back verbatim.
        self.assertEqual(
            self.raw_rows("things", order_by="id"),
            [(1, 1234), (2, 5678), (3, "01234")],
        )
        with self.raw_connect() as conn:
            affinities = {
                row[1]: row[2]
                for row in conn.execute("PRAGMA table_info(things)").fetchall()
            }
            self.assertEqual(affinities, {"id": "BLOB", "code": "BLOB"})
            indices = conn.execute("PRAGMA index_list(things)").fetchall()
            self.assertTrue(
                any(row[1] == "things_id_unique" and row[2] == 1 for row in indices),
                indices,
            )
            # The unique index still blocks duplicate keys.
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("INSERT INTO things (id, code) VALUES (3, 'x')")

    def test_rebuilt_table_survives_a_further_type_change(self):
        with self.raw_connect() as conn:
            conn.execute("CREATE TABLE things (id INTEGER, v INTEGER)")
            conn.execute("INSERT INTO things VALUES (1, 10)")
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 2, "v": "11"}])
            writer.flush()
            writer.add(1, [{"id": 3, "v": 1.5}])
        with self.raw_connect() as conn:
            self.assertEqual(
                conn.execute("SELECT v, typeof(v) FROM things ORDER BY id").fetchall(),
                [(10, "integer"), ("11", "text"), (1.5, "real")],
            )

    def test_rebuild_with_new_columns_in_same_flush(self):
        with self.raw_connect() as conn:
            conn.execute("CREATE TABLE things (id INTEGER, v INTEGER)")
            conn.execute("INSERT INTO things VALUES (1, 10)")
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 2, "v": "x", "extra": "new"}])
        with self.raw_connect() as conn:
            self.assertEqual(
                conn.execute("SELECT id, v, extra FROM things ORDER BY id").fetchall(),
                [(1, 10, None), (2, "x", "new")],
            )
            self.assertEqual(
                conn.execute("PRAGMA table_info(things)").fetchall()[2][2],
                "BLOB",
            )

    def test_round_trip_survives_process_restart(self):
        script = """
            from scraperwiki.ingest import BatchWriter
            writer = BatchWriter("task", "things", ["id"], batch_size=100)
            writer.add(0, [{"id": 1, "code": 1234}])
            writer.close()
        """
        self.run_in_subprocess(script)
        script = """
            from scraperwiki.ingest import BatchWriter
            writer = BatchWriter("task", "things", ["id"], batch_size=100)
            writer.add(1, [{"id": 2, "code": "01234"}])
            writer.close()
        """
        self.run_in_subprocess(script)
        with self.raw_connect() as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT code, typeof(code) FROM things ORDER BY id"
                ).fetchall(),
                [(1234, "integer"), ("01234", "text")],
            )


class TestUniqueKeyDeclaration(IngestTestCase):
    def test_index_added_when_table_previously_had_no_key(self):
        # Simulate the first-version scraper: a table created without a
        # primary key, holding duplicate keys already.
        with self.raw_connect() as conn:
            conn.execute("CREATE TABLE things (id BLOB, v BLOB)")
            conn.executemany(
                "INSERT INTO things VALUES (?, ?)",
                [(1, "old-a"), (1, "old-b"), (2, "old-c")],
            )

        # The new code declares the key when ingesting the next batch.
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 3, "v": "new"}])

        with self.raw_connect() as conn:
            # Duplicate historical key collapses to its newest row.
            self.assertEqual(
                conn.execute("SELECT id, v FROM things ORDER BY rowid").fetchall(),
                [(1, "old-b"), (2, "old-c"), (3, "new")],
            )
            indices = conn.execute("PRAGMA index_list(things)").fetchall()
            self.assertTrue(
                any(row[1] == "things_id_unique" and row[2] == 1 for row in indices),
                indices,
            )
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute("INSERT INTO things VALUES (3, 'dup')")

    def test_key_declared_in_later_writer_after_keyless_writes(self):
        with BatchWriter("task", "things", [], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "v": "a"}, {"id": 1, "v": "b"}])
        with self.raw_connect() as conn:
            self.assertEqual(conn.execute("PRAGMA index_list(things)").fetchall(), [])

        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(1, [{"id": 2, "v": "c"}])

        with self.raw_connect() as conn:
            indices = conn.execute("PRAGMA index_list(things)").fetchall()
            self.assertTrue(
                any(row[1] == "things_id_unique" and row[2] == 1 for row in indices),
                indices,
            )

    def test_composite_unique_key(self):
        with BatchWriter("task", "things", ["a", "b"], batch_size=100) as writer:
            writer.add(
                0,
                [
                    {"a": 1, "b": 1, "v": "first"},
                    {"a": 1, "b": 2, "v": "other"},
                    {"a": 1, "b": 1, "v": "second"},
                ],
            )
        with self.raw_connect() as conn:
            rows = conn.execute("SELECT a, b, v FROM things ORDER BY b").fetchall()
        self.assertEqual(rows, [(1, 1, "second"), (1, 2, "other")])

    def test_same_key_across_batches_last_write_wins(self):
        with BatchWriter("task", "things", ["id"], batch_size=2) as writer:
            writer.add(0, [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}])
            writer.add(1, [{"id": 1, "v": "c"}])
            writer.add(2, [{"id": 1, "v": "d"}])
        self.assertEqual(
            self.raw_rows("things", order_by="id"),
            [(1, "d"), (2, "b")],
        )

    def test_replaying_a_batch_creates_no_duplicates(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "v": "a"}])
            writer.add(1, [{"id": 2, "v": "b"}])
        # New process re-emits an already-completed batch by number, then a
        # fresh one that happens to reuse a key.
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "v": "stale"}])
            writer.add(2, [{"id": 2, "v": "updated"}])
        self.assertEqual(
            self.raw_rows("things", order_by="id"),
            [(1, "a"), (2, "updated")],
        )


class TestLegacyInterop(IngestTestCase):
    def test_legacy_select_reads_writer_rows_with_storage_classes(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "code": 1234}])
            writer.add(1, [{"id": 2, "code": "01234"}])
        rows = scraperwiki.sql.select("* FROM things ORDER BY id")
        self.assertEqual(
            rows,
            [{"id": 1, "code": 1234}, {"id": 2, "code": "01234"}],
        )

    def test_legacy_save_writes_into_writer_table(self):
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 1, "code": 1234}])
        scraperwiki.sql.save(["id"], {"id": 2, "code": "01234"}, table_name="things")
        self.assertEqual(
            scraperwiki.sql.select("* FROM things ORDER BY id"),
            [{"id": 1, "code": 1234}, {"id": 2, "code": "01234"}],
        )

    def test_writer_reads_table_built_by_legacy_save(self):
        scraperwiki.sql.save(["id"], {"id": 1, "code": 1234}, table_name="things")
        with BatchWriter("task", "things", ["id"], batch_size=100) as writer:
            writer.add(0, [{"id": 2, "code": "01234"}])
        self.assertEqual(
            self.raw_rows("things", order_by="id"),
            [(1, 1234), (2, "01234")],
        )
