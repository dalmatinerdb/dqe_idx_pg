dqe_idx_pg
==========

Dalmatiner query engine indexer module for Postgres.

Setting up the schema
=====================

Creating new database
---------------------

It is easiest to just create new database with up to date schema.

    $ createdb metric_metadata;
    $ psql metric_metadata < priv/schema.sql

Migrating schema from version <= 0.3.6
--------------------------------------

After version 0.3.6 there was introduced significant schema change that require
manual sql migration.

You need to prepare database to use new column and copy data over. At this time
you should stop any processes writing to database (you still can keep processes
that read data running).

    CREATE EXTENSION hstore;
    ALTER TABLE metrics ADD COLUMN dimensions hstore;
    UPDATE metrics AS m SET dimensions = (
      SELECT hstore(array_agg(d.namespace || ':' || d.name), array_agg(d.value))
        FROM dimensions AS d
        WHERE d.metric_id = m.id
        GROUP BY d.metric_id)
      WHERE dimensions IS NULL;
    ALTER INDEX metrics_idx RENAME TO metrics_collection_metric_bucket_key_idx;
    CREATE INDEX CONCURRENTLY ON metrics USING btree(collection, akeys(dimensions));
    CREATE INDEX CONCURRENTLY ON metrics USING btree(collection, metric, akeys(dimensions));
    CREATE INDEX CONCURRENTLY ON metrics USING GIST (dimensions);

Now you should upgrade all applications to most recent version. Start processes 
writing to database as soon as they are upgraded. You will need also to restart
all remaining processes, so they start using new version of library.

Once you upgrade your code and make sure everything is working, you can clean up
parts of old schema that is no longer used.

    DROP TABLE dimensions;
    ALTER TABLE metrics DROP COLUMN id;
    DROP INDEX metrics_idx_collection;
    DROP INDEX metrics_idx_collection_metric;
    DROP INDEX metrics_idx_id_collection_metric;
    DROP INDEX metrics_idx_metric;

Build
-----

    $ rebar3 compile

Running EQC tests
-----------------

Tests are included with this application that verify the syntactic correctness
of all SQL statements used with the index.  In addition, these SQL statements
are verified against the latest schema included in `priv/schema`.

A working 9.1 or above Postgres installation is required in order to run the
tests. The scripts included in the `priv` directory setup an isolated instance
of Postgres in the `datadir` directory.

    ./priv/setup_test_db.sh # This sets up an installation of Postgres in datadir/

    rebar3 as eqc eqc # Runs the tests

    ./priv/stop_test_db.sh # This stops the instance of Postgres in datadir/

Alternatively, use make to run all:

    make eqc-test # Runs setup, EQC and teardown scripts
