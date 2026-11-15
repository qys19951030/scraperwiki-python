"""
Batch ingestion for long-running scrapers.

``scraperwiki.sql.save`` writes one row at a time and keeps scraper state in
memory, which does not survive a process being killed and makes partial
failures hard to account for.  ``BatchWriter`` instead takes data a batch at a
time, writes whole batches to SQLite in one transaction, records progress in
the database so a restart resumes where the previous run stopped, and routes
rows that cannot be written to a fixed quarantine table instead of aborting
the batch.

Only SQLite is supported and no new runtime dependencies are required.
"""


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
        Write every buffered batch to the database.

        The concrete persistence logic (batch writes, checkpoints, quarantine
        and schema adaptation) is added in later iterations; the skeleton only
        drains the in-memory buffer.
        """
        if self._closed:
            raise RuntimeError("BatchWriter is closed")
        self._pending.clear()
        self._pending_rows = 0

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
        """Flush any remaining rows and mark the writer as closed."""
        if self._closed:
            return
        self.flush()
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

        ``-1`` means nothing has been written yet.  The skeleton has no
        database behind it, so it always reports ``-1``.
        """
        return -1
