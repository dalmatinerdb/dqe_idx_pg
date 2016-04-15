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
