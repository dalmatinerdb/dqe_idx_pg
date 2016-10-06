dqe_idx_pg
==========

Dalmatiner query engine indexer module for Postgres.

Setting up the schema
=====================
    $ createdb dqe_index;
    $ psql dqe_index < db/schema.sql
    $ psql dqe_index < db/seed.sql

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

    make eqc_test # Runs setup, EQC and teardown scripts
