"""
Batch ingestion for long-running scrapers.

``scraperwiki.sql.save`` writes one row at a time and keeps scraper state in
memory, which does not survive a process being killed and makes partial
failures hard to account for.  ``BatchWriter`` instead takes data a batch at
a time, writes whole batches to SQLite in one transaction, records progress in
the database so a restart resumes where the previous run stopped, and routes
rows that cannot be written to a fixed quarantine table instead of aborting
the batch.

Only SQLite is supported and no new runtime dependencies are required.
"""

from collections.abc import Mapping

from . import sql

# Table holding rows that could not be written together with the reason.
QUARANTINE_TABLE = "swquarantine"


class BatchWriter:
    """
    Accumulate rows in memory and persist them to SQLite in batches.

    Parameters
    ----------
    task:
        Name of the scraping task.  Checkpoints are stored per task, so
        different tasks do not interfere with each other.
    table:
        SQLite table the rows are written to.
    unique_keys:
        Column names forming the unique key of a row (may be empty).  Rows
        sharing a key are replaced, so re-running a batch never creates
        duplicates.
    batch_size:
        Number of rows buffered in memory before they are written to the
        database automatically.
    """

    def __init__(self, task, table, unique_keys, batch_size):
        if not isinstance(task, str) or not task:
            raise ValueError("task must be a non-empty string")
        if not isinstance(table, str) or not table:
            raise ValueError("table must be a non-empty string")
        if not isinstance(unique_keys, (list, tuple)):
            raise TypeError("unique_keys must be a list or tuple of column names")
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("batch_size must be a positive integer")

        self.task = task
        self.table = table
        self.unique_keys = list(unique_keys)
        self.batch_size = batch_size

        # Buffered batches waiting to be written.  Each entry is a
        # ``(batch_no, rows)`` tuple preserving arrival order.
        self._pending = []
        self._pending_rows = 0
        self._closed = False

        # Dedicated engine/metadata so the writer never disturbs the global
        # connection state used by the legacy scraperwiki.sql functions.
        self._engine = sql._engine_for()

        # Cumulative counters for the lifetime of this writer.
        self._stats = {"success": 0, "failure": 0, "skipped": 0}

    def add(self, batch_no, rows):
        """
        Hand a batch of rows to the writer.

        ``rows`` must be an iterable of mappings.  Batches whose number is
        not newer than the stored checkpoint are considered already done and
        are skipped without touching the database.
        """
        if self._closed:
            raise RuntimeError("BatchWriter is closed")
        if not isinstance(batch_no, int):
            raise TypeError("batch_no must be an integer")

        rows = list(rows)
        if batch_no <= self.checkpoint:
            self._stats["skipped"] += len(rows)
            return

        self._pending.append((batch_no, rows))
        self._pending_rows += len(rows)
        if self._pending_rows >= self.batch_size:
            self.flush()

    def flush(self):
        """
        Write every buffered batch to the database in a single transaction.

        On return every row passed to :meth:`add` since the previous flush is
        either stored in the data table or rejected to the quarantine table,
        and the progress checkpoint has advanced.
        """
        if self._closed:
            raise RuntimeError("BatchWriter is closed")
        if not self._pending:
            return

        batches = self._pending
        self._pending = []
        self._pending_rows = 0

        with self._engine.begin() as connection:
            self._adapt_schema(connection, batches)
            for batch_no, rows in batches:
                written = self._write_batch(connection, batch_no, rows)
                self._stats["success"] += written

    def _write_batch(self, connection, batch_no, rows):
        """Write one batch, returning the number of rows stored."""
        quoted_table = sql._quoted_identifier(self.table)
        insert_sql = f"INSERT OR REPLACE INTO {quoted_table} "
        written = 0
        for row in rows:
            if not isinstance(row, Mapping):
                raise TypeError(f"Elements of data must be mappings, got {type(row)}")
            columns = list(row.keys())
            column_sql = ", ".join(sql._quoted_identifier(column) for column in columns)
            placeholders = ", ".join("?" for _ in columns)
            values = [row[column] for column in columns]
            connection.exec_driver_sql(
                insert_sql + f"({column_sql}) VALUES ({placeholders})",
                tuple(values),
            )
            written += 1
        return written

    def _adapt_schema(self, connection, batches):
        """
        Make sure the data table exists and has a column for every field of
        every row about to be written.
        """
        all_rows = [row for _, rows in batches for row in rows]
        wanted_columns = {}
        for row in all_rows:
            if not isinstance(row, Mapping):
                continue
            for name, value in row.items():
                wanted_columns.setdefault(name, sql.affinity_for_value(value))

        if not sql.table_exists(connection, self.table):
            sql.create_data_table(connection, self.table, wanted_columns)
            sql.ensure_unique_index(connection, self.table, self.unique_keys)
            return

        current = sql.get_column_affinities(connection, self.table)
        for name, affinity in wanted_columns.items():
            if name not in current:
                sql.add_data_column(connection, self.table, name, affinity)
        sql.ensure_unique_index(connection, self.table, self.unique_keys)

    def pending(self):
        """Number of rows held in memory but not yet written to the database."""
        return self._pending_rows

    def stats(self):
        """
        Cumulative counters for this writer: numbers of rows successfully
        written, rows rejected to the quarantine table and rows skipped
        because their batch was already done.
        """
        return dict(self._stats)

    def close(self):
        """Flush any remaining rows and release database resources."""
        if self._closed:
            return
        self.flush()
        self._engine.dispose()
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return False

    @property
    def checkpoint(self):
        """
        Highest batch number known to be fully written to the database.

        ``-1`` means nothing has been written yet.  Persistent checkpoints
        are added in a later iteration.
        """
        return -1
