-module('1495845241_initial').
-export([upgrade/1, downgrade/1]).
-behaviour(sql_migration).

upgrade(Pool) ->
    {ok, _, _} = pgapp:squery(Pool, "CREATE EXTENSION IF NOT EXISTS hstore"),
    {ok, _, _} = pgapp:squery(
                   Pool,
                   "CREATE TABLE metrics ("
                   "  collection  text NOT NULL,"
                   "  metric      text[] NOT NULL,"
                   "  bucket      text NOT NULL,"
                   "  key         text[] NOT NULL,"
                   "  dimensions  hstore"
                   ")"),
    {ok, _, _} =
        pgapp:squery(
          Pool,
          "CREATE UNIQUE INDEX ON metrics (collection, metric, bucket, key)"),
    {ok, _, _} =
        pgapp:squery(
          Pool,
          "CREATE INDEX ON metrics USING btree"
          "(collection, akeys(dimensions))"),
    {ok, _, _} =
        pgapp:squery(
          Pool,
          "CREATE INDEX ON metrics USING btree"
          "(collection, metric, akeys(dimensions))"),
    {ok, _, _} =
        pgapp:squery(
          Pool,
          "CREATE INDEX ON metrics USING GIST (dimensions)"),
    ok.

downgrade(Pool) ->
    %% We do not need to remove indexes seperately
    pgapp:squery(Pool, "DROP TABLE metrics").

