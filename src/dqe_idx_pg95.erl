-module(dqe_idx_pg95).
-behaviour(dqe_idx).

%% API exports
-export([
         init/0,
         lookup/1, lookup/2, lookup_tags/1,
         collections/0, metrics/1, namespaces/1, namespaces/2,
         tags/2, tags/3, values/3, values/4, expand/2,
         add/4, add/5, update/5,
         delete/4, delete/5
        ]).

%%====================================================================
%% API functions
%%====================================================================

init() ->
    dqe_idx_pg:init().

lookup(Query) ->
    dqe_idx_pg:lookup(Query, []).

lookup(Query, Groupings) ->
    dqe_idx_pg:lookup(Query, Groupings).

lookup_tags(Query) ->
    dqe_idx_pg:lookup_tags(Query).

collections() ->
    dqe_idx_pg:collections().

metrics(Collection) ->
    dqe_idx_pg:metrics(Collection).

namespaces(Collection) ->
    dqe_idx_pg:namespaces(Collection).

namespaces(Collection, Metric) ->
    dqe_idx_pg:namespaces(Collection, Metric).

tags(Coallection, Namespace) ->
    dqe_idx_pg:tags(Coallection, Namespace).

tags(Collection, Metric, Namespace) ->
    dqe_idx_pg:tags(Collection, Metric, Namespace).

values(Collection, Namespace, Tag) ->
    dqe_idx_pg:values(Collection, Namespace, Tag).

values(Collection, Metric, Namespace, Tag) ->
    dqe_idx_pg:values(Collection, Metric, Namespace, Tag).

expand(Bucket, Globs) ->
    dqe_idx_pg:expand(Bucket, Globs).

-spec add(Collection::binary(),
          Metric::binary(),
          Bucket::binary(),
          Key::binary()) ->
                 {ok, MetricIdx::non_neg_integer()} |
                 ok |
                 {error, Error::term()}.

add(Collection, Metric, Bucket, Key) ->
    Q = "INSERT INTO metrics (collection, metric, bucket, key) VALUES "
        "($1, $2, $3, $4) ON CONFLICT DO NOTHING RETURNING id",
    Vs = [Collection, Metric, Bucket, Key],
    T0 = erlang:system_time(),
    case pgapp:equery(Q, Vs) of
        %% Returned by DO NOTHING
        {ok, 0} ->
            ok;
        {ok, 1, [_], [{ID}]} ->
            lager:debug("[dqe_idx:pg:add/4] Query too ~pms: ~s <- ~p",
                        [dqe_idx_pg:tdelta(T0), Q, Vs]),

            {ok, ID};
        E ->
            lager:info("[dqe_idx:pg:add/4] Query failed after ~pms: ~s <- ~p",
                       [dqe_idx_pg:tdelta(T0), Q, Vs]),
            E
    end.

add(Collection, Metric, Bucket, Key, []) ->
    add(Collection, Metric, Bucket, Key);

add(Collection, Metric, Bucket, Key, NVs) ->
    case add(Collection, Metric, Bucket, Key) of
        ok ->
            ok;
        {ok, MID} ->
            {Q, Vs} = add_tags(MID, NVs),
            T0 = erlang:system_time(),
            case pgapp:equery(Q, Vs) of
                {ok, _, _} ->
                    lager:debug("[dqe_idx:pg:add/5] Query too ~pms: ~s <- ~p",
                                [dqe_idx_pg:tdelta(T0), Q, Vs]),
                    {ok, MID};
                E ->
                    lager:info("[dqe_idx:pg:add/5] Query failed after ~pms: "
                               "~s <- ~p", [dqe_idx_pg:tdelta(T0), Q, Vs]),
                    E
            end;
        EAdd ->
            EAdd
    end.

update(Collection, Metric, Bucket, Key, NVs) ->
    AddRes = case add(Collection, Metric, Bucket, Key) of
                 ok ->
                     dqe_idx_pg:get_id(Collection, Metric, Bucket, Key);
                 RAdd ->
                     RAdd
             end,
    case AddRes of
        {ok, MID} ->
            {Q, Vs} = update_tags(MID, NVs),
            T0 = erlang:system_time(),
            case pgapp:equery(Q, Vs) of
                {ok, _, _} ->
                    lager:debug("[dqe_idx:pg:update/5] Query too ~pms:"
                                " ~s <- ~p",
                                [dqe_idx_pg:tdelta(T0), Q, Vs]),
                    {ok, MID};
                E ->
                    lager:info("[dqe_idx:pg:update/5] Query failed after ~pms:"
                               " ~s <- ~p",
                               [dqe_idx_pg:tdelta(T0), Q, Vs]),
                    E
            end;
        EAdd ->
            EAdd
    end.

delete(Collection, Metric, Bucket, Key) ->
    dqe_idx_pg:delete(Collection, Metric, Bucket, Key).

delete(Collection, Metric, Bucket, Key, Tags) ->
    dqe_idx_pg:delete(Collection, Metric, Bucket, Key, Tags).

%%====================================================================
%% Internal functions
%%====================================================================

add_tags(MID, Tags) ->
    Q = "INSERT INTO tags (metric_id, namespace, name, value) VALUES ",
    OnConflict = "DO NOTHING",
    build_tags(MID, 1, OnConflict, Tags, Q, []).

update_tags(MID, Tags) ->
    Q = "INSERT INTO tags (metric_id, namespace, name, value) VALUES ",
    OnConflict = "ON CONSTRAINT tags_metric_id_namespace_name_key "
        "DO UPDATE SET value = excluded.value",
    build_tags(MID, 1, OnConflict, Tags, Q, []).

build_tags(MID, P, OnConflict, [{NS, N, V}], Q, Vs) ->
    {[Q, tag_values(P), " ON CONFLICT ", OnConflict],
     lists:reverse([V, N, NS, MID | Vs])};

build_tags(MID, P, OnConflict, [{NS, N, V} | Tags], Q, Vs) ->
    Q1 = [Q, tag_values(P), ","],
    Vs1 = [V, N, NS, MID | Vs],
    build_tags(MID, P+4, OnConflict, Tags, Q1, Vs1).

tag_values(P)  ->
    [" ($", query_builder:i2l(P), ", "
     "$", query_builder:i2l(P + 1), ", "
     "$", query_builder:i2l(P + 2), ", "
     "$", query_builder:i2l(P + 3), ")"].
