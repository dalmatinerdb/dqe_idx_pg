#!/bin/sh

if [ -z $(which pg_ctl) ] ; then
    echo "Please install Postgres 9.1 or above and add it to your PATH"
    exit 1
fi

pg_ctl initdb -s -D datadir/
pg_ctl -D datadir/ -o "-F -p 10433" start -w > /dev/null 2>&1
createdb -p 10433 metric_metadata
psql -d metric_metadata -p 10433 -f priv/create_db.sql
