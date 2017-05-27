#!/bin/sh

if [ -z $(which pg_ctl) ] ; then
    echo "Please install Postgres 9.1 or above and add it to your PATH"
    exit 1
fi

pg_ctl stop -D datadir/

if [ -d datadir ]; then
    rm -r datadir
fi
