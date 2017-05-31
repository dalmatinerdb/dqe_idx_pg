-module('1496245693_add_time_range').
-export([upgrade/1, downgrade/1]).
-behaviour(sql_migration).

upgrade(Pool) ->
    {ok, _, _} = pgapp:squery(
                   Pool,
                   "ALTER TABLE metrics"
                   "  ADD time_range tsrange  DEFAULT '(-infinity, infinity)'"),
    {ok, _, _} = pgapp:squery(
                   Pool,
                   "CREATE INDEX metrics_time_range_idx"
                   " ON test USING gist (time_range)"),
    ok.

downgrade(Pool) ->
    {ok, _, _} = pgapp:squery(Pool, "DROP INDEX metrics_time_range_idx"),
    {ok, _, _} = pgapp:squery(Pool, "ALTER TABLE metrics DROP time_range"),
    ok.
