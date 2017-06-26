-module('1498504778_gin_index').

-export([upgrade/1, downgrade/1]).
-behaviour(sql_migration).

upgrade(Pool) ->
    {ok, _, _} = pgapp:squery(Pool, "CREATE EXTENSION IF NOT EXISTS btree_gin"),
    {ok, _, _} =
        pgapp:squery(
          Pool,
          "DROP INDEX metrics_dimensions_idx"),
    {ok, _, _} =
        pgapp:squery(
          Pool,
          "CREATE INDEX ON metrics USING GIN (collection, dimensions)"),
    ok.

downgrade(Pool) ->
    {ok, _, _} =
        pgapp:squery(
          Pool,
          "CREATE INDEX ON metrics USING GIST (dimensions)"),
    ok.
