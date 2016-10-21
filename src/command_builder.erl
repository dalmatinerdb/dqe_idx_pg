-module(command_builder).

-export([add_metric/4, add_tags/3,
         update_tags/3,
         delete_metric/4, delete_tag/3]).

-include("dqe_idx_pg.hrl").

%%====================================================================
%% API
%%====================================================================

add_metric(Collection, Metric, Bucket, Key)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Bucket),
       is_list(Key) ->
    Query = "INSERT INTO " ?MET_TABLE " "
            "(collection, metric, bucket, key) VALUES "
            "($1, $2, $3, $4) "
            "ON CONFLICT DO NOTHING RETURNING id",
    Values = [Collection, Metric, Bucket, Key],
    {ok, Query, Values}.

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

add_tags(MID, Collection, Tags) ->
    Query = "INSERT INTO " ?DIM_TABLE " "
            "(metric_id, collection, namespace, name, value) VALUES ",
    OnConflict = "DO NOTHING",
    build_tags(MID, Collection, 1, OnConflict, Tags, Query, []).

update_tags(MID, Collection, Tags) ->
    Query = "INSERT INTO " ?DIM_TABLE " "
            "(metric_id, collection, namespace, name, value) VALUES ",
    OnConflict = "ON CONSTRAINT dimensions_metric_id_namespace_name_key "
        "DO UPDATE SET value = excluded.value",
    build_tags(MID, Collection, 1, OnConflict, Tags, Query, []).

delete_tag(MetricID, Namespace, TagName) ->
    Query = "DELETE FROM " ?DIM_TABLE " WHERE "
            "metric_id = $1 "
            "AND namespace = $2 "
            "AND name = $3",
    Values = [MetricID, Namespace, TagName],
    {ok, Query, Values}.

%%====================================================================
%% Internal functions
%%====================================================================

build_tags(MID, Collection, P, OnConflict, [{NS, N, V}], Q, Vs) ->
    Query = [Q, tag_values(P), " ON CONFLICT ", OnConflict],
    Values= lists:reverse([V, N, NS, Collection, MID | Vs]),
    {ok, Query, Values};

build_tags(MID, Collection, P, OnConflict, [{NS, N, V} | Tags], Q, Vs) ->
    Q1 = [Q, tag_values(P), ","],
    Vs1 = [V, N, NS, Collection, MID | Vs],
    build_tags(MID, Collection, P+5, OnConflict, Tags, Q1, Vs1).

tag_values(P)  ->
    ["($", i2l(P), ", "
     "$", i2l(P + 1), ", "
     "$", i2l(P + 2), ", "
     "$", i2l(P + 3), ", "
     "$", i2l(P + 4), ")"].

i2l(I) ->
    integer_to_list(I).
