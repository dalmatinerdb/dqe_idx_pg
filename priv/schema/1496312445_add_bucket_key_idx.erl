-module('1496312445_add_bucket_key_idx').
-export([upgrade/1, downgrade/1]).
-behaviour(sql_migration).


upgrade(Pool) ->
    {ok, _, _} = pgapp:squery(
                   Pool,
                   "CREATE INDEX metrics_bucket_key_idx"
                   " ON metrics (bucket, key)"),
    ok.

downgrade(Pool) ->
    {ok, _, _} = pgapp:squery(Pool, "DROP INDEX metrics_bucket_key_idx"),
    ok.
