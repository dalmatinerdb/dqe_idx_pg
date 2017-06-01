-module(command_builder).

-export([add_metric/6,
         update_tags/5,
         delete_metric/4,
         delete_tags/5]).

-import(dqe_idx_pg_utils, [encode_tag_key/2, tags_to_hstore/1]).

-include_lib("dqe_idx_pg/include/dqe_idx_pg.hrl").
%% 62167219200 == calendar:datetime_to_gregorian_seconds(
%% {{1970, 1, 1}, {0, 0, 0}})
-define(S1970, 62167219200).
%%====================================================================
%% API
%%====================================================================

-spec add_metric(binary(), [binary()], binary(), [binary()],
                 pos_integer() | undefined,
                 [{binary(), binary(), binary()}]) ->
                        {ok, iolist(), [term()]}.
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
    HStore = tags_to_hstore(Tags),
    Values = [Collection, Metric, Bucket, Key, HStore],
    {ok, Query, Values};

add_metric(Collection, Metric, Bucket, Key, FirstSeen, Tags)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Bucket),
       is_list(Key),
       is_integer(FirstSeen),
       is_list(Tags) ->
    FirstSeenD = calendar:gregorian_seconds_to_datetime(FirstSeen + ?S1970),
    Query = "INSERT INTO " ?MET_TABLE " "
        "(collection, metric, bucket, key, time_range, dimensions) VALUES "
        "($1, $2, $3, $4, "
        "tsrange($5, 'infinity'::timestamp), $6) "
        "ON CONFLICT DO NOTHING RETURNING dimensions",
    HStore = tags_to_hstore(Tags),
    Values = [Collection, Metric, Bucket, Key, FirstSeenD, HStore],
    {ok, Query, Values}.

-spec delete_metric(binary(), [binary()], binary(), [binary()]) ->
                           {ok, iolist(), [term()]}.
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

-spec update_tags(binary(), [binary()], binary(), [binary()],
                  [{binary(), binary(), binary()}]) ->
                         {ok, iolist(), [term()]}.
update_tags(Collection, Metric, Bucket, Key, Tags) ->
    Query = "UPDATE " ?MET_TABLE
        "  SET dimensions = dimensions || $5"
        "  WHERE collection = $1"
        "    AND metric = $2"
        "    AND bucket = $3"
        "    AND key = $4"
        "  RETURNING dimensions",
    HStore = tags_to_hstore(Tags),
    Values = [Collection, Metric, Bucket, Key, HStore],
    {ok, Query, Values}.

-spec delete_tags(binary(), [binary()], binary(), [binary()],
                  [{binary(), binary(), binary()}]) ->
                         {ok, iolist(), [term()]}.
delete_tags(Collection, Metric, Bucket, Key, Tags) ->
    Query = "UPDATE " ?MET_TABLE
        "  SET dimensions = delete(dimensions, $5)"
        "  WHERE collection = $1"
        "    AND metric = $2 "
        "    AND bucket = $3 "
        "    AND key = $4",
    HKeys = [encode_tag_key(NS, N) || {NS, N, _} <- Tags],
    Values = [Collection, Metric, Bucket, Key, HKeys],
    {ok, Query, Values}.
