-module(pg_command_builder).

-export([touch/1,
         add_metric/6,
         update_tags/5,
         delete_metric/4,
         delete_tags/5]).

-include_lib("dqe_idx_pg/include/dqe_idx_pg.hrl").

%%====================================================================
%% API
%%====================================================================

-spec add_metric(dqe_idx:collection(),
                 dqe_idx:metric(),
                 dqe_idx:bucket(),
                 dqe_idx:key(),
                 dqe_idx:timestamp(),
                 dqe_idx:tags()) ->
                        dqe_idx_pg:sql_stmt().
add_metric(Collection, Metric, Bucket, Key, now, Tags) ->
    Now = erlang:system_time(seconds),
    add_metric(Collection, Metric, Bucket, Key, Now, Tags);

add_metric(Collection, Metric, Bucket, Key, undefined, Tags)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Bucket),
       is_list(Key),
       is_list(Tags) ->
    Query = "INSERT INTO " ?MET_TABLE " "
        "(collection, metric, bucket, key, dimensions) VALUES "
        "($1, $2, $3, $4, $5) "
        "ON CONFLICT DO NOTHING RETURNING dimensions",
    HStore = dqe_idx_pg_utils:tags_to_hstore(Tags),
    Values = [Collection, Metric, Bucket, Key, HStore],
    {ok, Query, Values};

add_metric(Collection, Metric, Bucket, Key, FirstSeen, Tags)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Bucket),
       is_list(Key),
       is_integer(FirstSeen),
       is_list(Tags) ->
    FirstSeenD = dqe_idx_pg_utils:s_to_date(FirstSeen),
    Query = "INSERT INTO " ?MET_TABLE " "
        "(collection, metric, bucket, key, time_range, dimensions) VALUES "
        "($1, $2, $3, $4, "
        "tsrange($5::timestamp, $5::timestamp + '1s'::interval), $6) "
        "ON CONFLICT DO NOTHING RETURNING dimensions",
    HStore = dqe_idx_pg_utils:tags_to_hstore(Tags),
    Values = [Collection, Metric, Bucket, Key, FirstSeenD, HStore],
    {ok, Query, Values}.

-spec touch(dqe_idx:touch_points()) ->
                   dqe_idx_pg:sql_stmt().

touch(Points) ->
    {Values, Data} = mk_touch_values(Points),
    Query = "UPDATE metrics AS m SET"
        "  time_range = tsrange(lower(time_range), p.last_seen) "
        "FROM (VALUES " ++ Values ++ ") as p(bucket, key, last_seen) "
        "WHERE m.bucket = p.bucket AND m.key = p.key "
        "AND p.last_seen > upper(time_range)",
    {ok, Query, Data}.

-spec delete_metric(dqe_idx:collection(),
                    dqe_idx:metric(),
                    dqe_idx:bucket(),
                    dqe_idx:key()) ->
                           dqe_idx_pg:sql_stmt().
delete_metric(Collection, Metric, Bucket, Key)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Bucket),
       is_list(Key) ->
    Query = "DELETE FROM " ?MET_TABLE " WHERE "
        "collection = $1 "
        "AND metric = $2 "
        "AND bucket = $3 "
        "AND key = $4",
    Values = [Collection, Metric, Bucket, Key],
    {ok, Query, Values}.

-spec update_tags(dqe_idx:collection(),
                  dqe_idx:metric(),
                  dqe_idx:bucket(),
                  dqe_idx:key(),
                  dqe_idx:tags()) ->
                         dqe_idx_pg:sql_stmt().
update_tags(Collection, Metric, Bucket, Key, Tags) ->
    Query = "UPDATE " ?MET_TABLE
        "  SET dimensions = dimensions || $5"
        "  WHERE collection = $1"
        "    AND metric = $2"
        "    AND bucket = $3"
        "    AND key = $4"
        "  RETURNING dimensions",
    HStore = dqe_idx_pg_utils:tags_to_hstore(Tags),
    Values = [Collection, Metric, Bucket, Key, HStore],
    {ok, Query, Values}.

-spec delete_tags(dqe_idx:collection(),
                  dqe_idx:metric(),
                  dqe_idx:bucket(),
                  dqe_idx:key(),
                  [dqe_idx:tag()]) ->
                         dqe_idx_pg:sql_stmt().
delete_tags(Collection, Metric, Bucket, Key, Tags) ->
    Query = "UPDATE " ?MET_TABLE
        "  SET dimensions = delete(dimensions, $5)"
        "  WHERE collection = $1"
        "    AND metric = $2 "
        "    AND bucket = $3 "
        "    AND key = $4",
    HKeys = [dqe_idx_pg_utils:encode_tag_key(NS, N) || {NS, N, _} <- Tags],
    Values = [Collection, Metric, Bucket, Key, HKeys],
    {ok, Query, Values}.

%%====================================================================
%% Internal functions
%%====================================================================

mk_touch_values(Points) ->
    Now = erlang:system_time(seconds),
    mk_touch_values(Points, Now, 1, "", []).

mk_touch_values([{Bucket, Key} | R], Now, N, Values, Data) ->
    mk_touch_values([{Bucket, Key, Now} | R], Now, N, Values, Data);

mk_touch_values([{Bucket, Key, now} | R], Now, N, Values, Data) ->
    mk_touch_values([{Bucket, Key, Now} | R], Now, N, Values, Data);

mk_touch_values([{Bucket, Key, Time}], _Now, N, Values, Data) ->
    Values1 = [Values, n_to_tpl(N)],
    Data1 = [dqe_idx_pg_utils:s_to_date(Time), Key, Bucket | Data],
    {Values1, lists:reverse(Data1)};

mk_touch_values([{Bucket, Key, Time} | R], Now, N, Values, Data) ->
    Values1 = [Values, n_to_tpl(N), ", "],
    Data1 = [dqe_idx_pg_utils:s_to_date(Time), Key, Bucket | Data],
    mk_touch_values(R, Now, N + 3, Values1, Data1).

n_to_tpl(N) ->
    ["(",
     $$, integer_to_list(N), "::text, ",
     $$, integer_to_list(N + 1), "::text[], ",
     $$, integer_to_list(N + 2), "::timestamp)"].
