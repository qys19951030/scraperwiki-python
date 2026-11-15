ScraperWiki Python library
==========================

This is a Python library for scraping web pages and saving data.

**Warning: This library is now in maintenance mode.**

**The library has been updated to work with Python 3.14 but there are no guarantees on future maintenance.**

Installing
----------

::

   pip install scraperwiki

Scraping
--------

scraperwiki.scrape(url[, params][,user_agent])
  Returns the downloaded string from the given url.

  ``params`` are sent as a POST if set.

  ``user_agent`` sets the user-agent string if provided.

Saving data
-----------

Helper functions for saving and querying an SQL database. Updates
the schema automatically according to the data you save.

Currently only supports SQLite. It will make a local SQLite database.
It is based on `SQLAlchemy <https://pypi.python.org/pypi/SQLAlchemy>`_.

scraperwiki.sql.save(unique_keys, data[, table_name="swdata"])
  Saves a data record into the datastore into the table given by ``table_name``.

  ``data`` is a dict object with field names as keys; ``unique_keys`` is a subset of data.keys() which determines when a record is overwritten. For large numbers of records `data` can be a list of dicts.

  ``scraperwiki.sql.save`` is entitled to buffer an arbitrary number of
  rows until the next read via the ScraperWiki API, an exception is hit,
  or until process exit. An effort is made to do a timely periodic flush.
  Records can be lost if the process experiences a hard-crash, power outage
  or SIGKILL due to high memory usage during an out-of-memory condition. The
  buffer can be manually flushed with ``scraperwiki.sql.flush()``, which
  commits any buffered rows immediately.

scraperwiki.ingest.BatchWriter(task, table, unique_keys, batch_size)
  Batch ingestion channel for long-running scrapers. Rows are handed over a
  batch at a time with ``add(batch_no, rows)`` and buffered in memory until
  ``batch_size`` rows have accumulated (or ``flush()`` is called), at which
  point whole batches are written in a single transaction. It supports the
  context-manager protocol and exposes ``flush()``, ``pending()``,
  ``stats()`` and ``close()``.

  Progress is checkpointed per ``task`` in the same variable store used by
  ``save_var``/``get_var`` (the highest completed batch number), so a writer
  recreated after a restart resumes from the checkpoint and ``add`` skips
  batches that are already done, counting them in the ``skipped`` stat.
  Different tasks have independent checkpoints.

  A row that cannot be written (for example one containing a nested
  structure, or that is not a mapping) is skipped without affecting the rest
  of its batch and recorded, together with the task, batch number, original
  content and failure reason, in the fixed ``swquarantine`` table. Columns
  are created and widened automatically: values keep their SQLite storage
  class, so a string such as ``"01234"`` is always read back as ``"01234"``,
  and type changes preserve historical rows via a table rebuild. Rows
  sharing a unique key replace each other (last write wins) across batches.

scraperwiki.sql.execute(sql[, vars])
  Executes any arbitrary SQL command. For example CREATE, DELETE, INSERT or DROP.

  ``vars`` is an optional list of parameters, inserted when the SQL command contains ‘?’s. For example::

    scraperwiki.sql.execute("INSERT INTO swdata VALUES (?,?,?)", [a,b,c])

  The ‘?’ convention is like "paramstyle qmark" from `Python's DB API 2.0 <http://www.python.org/dev/peps/pep-0249/>`_ (but note that the API to the datastore is nothing like Python's DB API).  In particular the ‘?’ does not itself need quoting, and can in general only be used where a literal would appear. (Note that you cannot substitute in, for example, table or column names.)

scraperwiki.sql.select(sqlfrag[, vars])
  Executes a select command on the datastore.  For example::

    scraperwiki.sql.select("* FROM swdata LIMIT 10")

  Returns a list of dicts that have been selected.

  ``vars`` is an optional list of parameters, inserted when the select command contains ‘?’s.  This is like the feature in the ``.execute`` command, above.

scraperwiki.sql.commit()
  Functionality now removed. It is retained, but does nothing. (sql.save auto-commits after every action).

scraperwiki.sql.show_tables([dbname])
  Returns an array of tables and their schemas in the current database.

scraperwiki.sql.save_var(key, value)
  Saves an arbitrary single-value into a table called ``swvariables``. Intended to store scraper state so that a scraper can continue after an interruption.

scraperwiki.sql.get_var(key[, default])
  Retrieves a single value that was saved by ``save_var``. Only works for string, float, or int types. For anything else, use the `pickle library <http://docs.python.org/library/pickle.html>`_ to turn it into a string.

Miscellaneous
-------------

scraperwiki.status(type, message=None)
  Functionality now removed since it was only for the now defunct ScraperWiki platform. It is retained but always returns None.

scraperwiki.pdftoxml(pdfdata)
  Convert a byte string containing a PDF file into an XML file containing the coordinates and font of each text string (see `the pdftohtml documentation <http://linux.die.net/man/1/pdftohtml>`_ for details). This requires ``pdftohtml`` which is part of ``poppler-utils``. 

Environment Variables
---------------------

SCRAPERWIKI_DATABASE_NAME
  default: ``scraperwiki.sqlite`` - name of database

SCRAPERWIKI_DATABASE_TIMEOUT
  default: ``300`` - number of seconds database will wait for a lock
