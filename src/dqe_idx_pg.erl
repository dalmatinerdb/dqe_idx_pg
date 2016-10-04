-module(dqe_idx_pg).
-behaviour(dqe_idx).

-include("dqe_idx_pg.hrl").

%% API exports
-export([
         init/0,
         lookup/1, lookup/2, lookup_tags/1,
         collections/0, metrics/1, namespaces/1, namespaces/2,
         tags/2, tags/3, values/3, values/4, expand/2, metric_variants/2,
         add/4, add/5, update/5,
         delete/4, delete/5,
         get_id/4, tdelta/1
        ]).

%%====================================================================
%% API functions
%%====================================================================

init() ->
    Opts = [size, max_overflow, database, username, password],
    Opts1 = [{O, application:get_env(dqe_idx_pg, O, undefined)}
             || O <- Opts],
    {ok, {Host, Port}} = application:get_env(dqe_idx_pg, server),
    pgapp:connect([{host, Host}, {port, Port} | Opts1]).

lookup(Query) ->
    {ok, Q, Vs} = query_builder:lookup_query(Query, []),
    Rows = execute({select, "lookup/1", Q, Vs}),
    R = [{B, dproto:metric_from_list(M)} || {B, M} <- Rows],
    {ok, R}.

lookup(Query, Groupings) ->
    {ok, Q, Vs} = query_builder:lookup_query(Query, Groupings),
    Rows = execute({select, "lookup/2", Q, Vs}),
    R = [{B, dproto:metric_from_list(M), G} || {B, M, G} <- Rows],
    {ok, R}.

lookup_tags(Query) ->
    {ok, Q, Vs} = query_builder:lookup_tags_query(Query),
    Rows = execute({select, "lookup_tags/1", Q, Vs}),
    {ok, Rows}.

metric_variants(Collection, Prefix) when is_list(Prefix) ->
    {ok, Q, Vs} = query_builder:metric_variants_query(Collection, Prefix),
    Rows = execute({select, "metric_variants/2", Q, Vs}),
    {ok, lists:sort(Rows)}.

collections() ->
    {ok, Q, Vs} = query_builder:collections_query(),
    Rows = execute({select, "collections/0", Q, Vs}),
    {ok, strip_tpl(Rows)}.

metrics(Collection) ->
    {ok, Q, Vs} = query_builder:metrics_query(Collection),
    Rows = execute({select, "metrics/1", Q, Vs}),
    R = [dproto:metric_from_list(M) || {M} <- Rows],
    {ok, R}.

namespaces(Collection) ->
    {ok, Q, Vs} = query_builder:namespaces_query(Collection),
    Rows = execute({select, "namespaces/1", Q, Vs}),
    {ok, strip_tpl(Rows)}.

namespaces(Collection, Metric) ->
    {ok, Q, Vs} = query_builder:namespaces_query(Collection, Metric),
    Rows = execute({select, "namespaces/2", Q, Vs}),
    {ok, strip_tpl(Rows)}.

tags(Collection, Namespace) ->
    {ok, Q, Vs} = query_builder:tags_query(Collection, Namespace),
    Rows = execute({select, "tags/2", Q, Vs}),
    {ok, strip_tpl(Rows)}.

tags(Collection, Metric, Namespace) ->
    {ok, Q, Vs} = query_builder:tags_query(Collection, Metric, Namespace),
    Rows = execute({select, "tags/3", Q, Vs}),
    {ok, strip_tpl(Rows)}.

values(Collection, Namespace, Tag) ->
    {ok, Q, Vs} = query_builder:values_query(Collection, Namespace, Tag),
    Rows = execute({select, "values/3", Q, Vs}),
    {ok, strip_tpl(Rows)}.

values(Collection, Metric, Namespace, Tag) ->
    {ok, Q, Vs} = query_builder:values_query(Collection, Metric,
                                             Namespace, Tag),
    Rows = execute({select, "values/4", Q, Vs}),
    {ok, strip_tpl(Rows)}.

expand(Bucket, []) when is_binary(Bucket) ->
    {ok, {Bucket, []}};

expand(Bucket, Globs) when
      is_binary(Bucket),
      is_list(Globs) ->
    {ok, QueryMap} = query_builder:glob_query(Bucket, Globs),
    RowSets = [begin
                   Rows = execute({select, "expand/2", Q, Vs}),
                   sets:from_list(Rows)
               end || {Q, Vs} <- QueryMap],

    %% Destructuring into [H | T] is safe since the cardinality of `RowSets' is
    %% equal to that of `Globs'
    [H | T] = RowSets,
    UniqueRows = lists:foldl(fun sets:union/2, H, T),
    Metrics = [dproto:metric_from_list(M) || {M} <- sets:to_list(UniqueRows)],
    {ok, {Bucket, Metrics}}.

-spec add(Collection::binary(),
          Metric::binary() | list(),
          Bucket::binary(),
          Key::binary() | list()) ->
                 ok |
                 {ok, MetricIdx::non_neg_integer()} |
                 {error, Error::term()}.

add(Collection, Metric, Bucket, Key) when is_binary(Metric) ->
    add(Collection, dproto:metric_to_list(Metric), Bucket, Key);

add(Collection, Metric, Bucket, Key) when is_binary(Key) ->
    add(Collection, Metric, Bucket, dproto:metric_to_list(Key));

add(Collection, Metric, Bucket, Key)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Bucket),
       is_list(Key) ->
    Q = "INSERT INTO " ?MET_TABLE " (collection, metric, bucket, key) VALUES "
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
            lager:info("[dqe_idx:pg:add/4] Query failed after ~pms: ~s <- ~p:"
                       " ~p", [dqe_idx_pg:tdelta(T0), Q, Vs, E]),
            E
    end.

add(Collection, Metric, Bucket, Key, []) ->
    add(Collection, Metric, Bucket, Key);

add(Collection, Metric, Bucket, Key, NVs) when is_binary(Metric) ->
    add(Collection, dproto:metric_to_list(Metric), Bucket, Key, NVs);

add(Collection, Metric, Bucket, Key, NVs) when is_binary(Key) ->
    add(Collection, Metric, Bucket, dproto:metric_to_list(Key), NVs);

add(Collection, Metric, Bucket, Key, NVs)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Bucket),
       is_list(Key) ->
    case add(Collection, Metric, Bucket, Key) of
        ok ->
            ok;
        {ok, MID} ->
            {Q, Vs} = add_tags(MID, Collection, NVs),
            T0 = erlang:system_time(),
            case pgapp:equery(Q, Vs) of
                {ok, _} ->
                    lager:debug("[dqe_idx:pg:add/5] Query too ~pms: ~s <- ~p",
                                [dqe_idx_pg:tdelta(T0), Q, Vs]),
                    {ok, MID};
                E ->
                    lager:info("[dqe_idx:pg:add/5] Query failed after ~pms: "
                               "~s <- ~p: E",
                               [dqe_idx_pg:tdelta(T0), Q, Vs, E]),
                    E
            end;
        EAdd ->
            EAdd
    end.

update(Collection, Metric, Bucket, Key, NVs) when is_binary(Metric) ->
    update(Collection, dproto:metric_to_list(Metric), Bucket, Key, NVs);

update(Collection, Metric, Bucket, Key, NVs) when is_binary(Key) ->
    update(Collection, Metric, Bucket, dproto:metric_to_list(Key), NVs);

update(Collection, Metric, Bucket, Key, NVs)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Bucket),
       is_list(Key) ->
    AddRes = case add(Collection, Metric, Bucket, Key) of
                 ok ->
                     get_id(Collection, Metric, Bucket, Key);
                 RAdd ->
                     RAdd
             end,
    case AddRes of
        {ok, MID} ->
            {Q, Vs} = update_tags(MID, Collection, NVs),
            T0 = erlang:system_time(),
            case pgapp:equery(Q, Vs) of
                {ok, _, _} ->
                    lager:debug("[dqe_idx:pg:update/5] Query too ~pms:"
                                " ~s <- ~p",
                                [dqe_idx_pg:tdelta(T0), Q, Vs]),
                    {ok, MID};
                E ->
                    lager:info("[dqe_idx:pg:update/5] Query failed after ~pms:"
                               " ~s <- ~p: ~p",
                               [dqe_idx_pg:tdelta(T0), Q, Vs, E]),
                    E
            end;
        EAdd ->
            EAdd
    end.

delete(Collection, Metric, Bucket, Key) when is_binary(Metric) ->
    delete(Collection, dproto:metric_to_list(Metric), Bucket, Key);
delete(Collection, Metric, Bucket, Key) when is_binary(Key) ->
    delete(Collection, Metric, Bucket, dproto:metric_to_list(Key));
delete(Collection, Metric, Bucket, Key)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Bucket),
       is_list(Key) ->
    Q = "DELETE FROM " ?MET_TABLE " WHERE collection = $1 AND " ++
        "metric = $2 AND bucket = $3 AND key = $4",
    Vs = [Collection, Metric, Bucket, Key],
    T0 = erlang:system_time(),
    case pgapp:equery(Q, Vs) of
        {ok, _} ->
            lager:debug("[dqe_idx:pg:delete/4] Query too ~pms: ~s <- ~p",
                        [tdelta(T0), Q, Vs]),
            ok;
        E ->
            lager:info("[dqe_idx:pg:delete/4] Query failed after ~pms: "
                       "~s <- ~p: ~p", [tdelta(T0), Q, Vs, E]),
            E
    end.

delete(Collection, Metric, Bucket, Key, NVs) when is_binary(Metric) ->
    delete(Collection, dproto:metric_to_list(Metric), Bucket, Key, NVs);
delete(Collection, Metric, Bucket, Key, NVs) when is_binary(Key) ->
    delete(Collection, Metric, Bucket, dproto:metric_to_list(Key), NVs);
delete(Collection, Metric, Bucket, Key, Tags) ->
    case get_id(Collection, Metric, Bucket, Key) of
        not_found ->
            ok;
        {ok, ID} ->
            Rs = [delete(ID, Namespace, Name) || {Namespace, Name} <- Tags],
            case [R || R <- Rs, R /= ok] of
                [] ->
                    ok;
                [E | _] ->
                    E
            end;
        E ->
            E
    end.

delete(MetricID, Namespace, TagName) ->
    Q = "DELETE FROM " ?DIM_TABLE " WHERE metric_id = $1 AND "
        "namespace = $2 AND name = $3",
    Vs = [MetricID, Namespace, TagName],
    case pgapp:equery(Q, Vs) of
        {ok, _, _} ->
            ok;
        E ->
            E
    end.

%%====================================================================
%% Internal functions
%%====================================================================

tdelta(T0) ->
    (erlang:system_time() - T0)/1000/1000.

strip_tpl(L) ->
    [E || {E} <- L].

get_id(Collection, Metric, Bucket, Key) ->
    T0 = erlang:system_time(),
    Q = "SELECT id FROM " ?MET_TABLE " WHERE "
        "collection = $1 AND "
        "metric = $2 AND "
        "bucket = $3 AND "
        "key = $4",
    Vs = [Collection, Metric, Bucket, Key],
    case pgapp:equery(Q, Vs) of
        {ok, [_], [{ID}]} ->
            lager:debug("[dqe_idx:pg:get_id/4] Query too ~pms: ~s <- ~p",
                        [tdelta(T0), Q, Vs]),

            {ok, ID};
        {ok, _, _} ->
            not_found;
        E ->
            lager:info("[dqe_idx:pg:get_id/4] Query failed after ~pms:"
                       " ~s <- ~p: ~p", [tdelta(T0), Q, Vs, E]),
            E
    end.

add_tags(MID, Collection, Tags) ->
    Q = "INSERT INTO " ?DIM_TABLE " "
        "(metric_id, collection, namespace, name, value) VALUES ",
    OnConflict = "DO NOTHING",
    build_tags(MID, Collection, 1, OnConflict, Tags, Q, []).

update_tags(MID, Collection, Tags) ->
    Q = "INSERT INTO " ?DIM_TABLE " "
        "(metric_id, collection, namespace, name, value) VALUES ",
    OnConflict = "ON CONSTRAINT dimensions_metric_id_namespace_name_key "
        "DO UPDATE SET value = excluded.value",
    build_tags(MID, Collection, 1, OnConflict, Tags, Q, []).

build_tags(MID, Collection, P, OnConflict, [{NS, N, V}], Q, Vs) ->
    {[Q, tag_values(P), " ON CONFLICT ", OnConflict],
     lists:reverse([V, N, NS, Collection, MID | Vs])};

build_tags(MID, Collection, P, OnConflict, [{NS, N, V} | Tags], Q, Vs) ->
    Q1 = [Q, tag_values(P), ","],
    Vs1 = [V, N, NS, Collection, MID | Vs],
    build_tags(MID, Collection, P+5, OnConflict, Tags, Q1, Vs1).

tag_values(P)  ->
    [" ($", query_builder:i2l(P), ", "
     "$", query_builder:i2l(P + 1), ", "
     "$", query_builder:i2l(P + 2), ", "
     "$", query_builder:i2l(P + 3), ", "
     "$", query_builder:i2l(P + 4), ")"].

execute({select, Name, Q, Vs}) ->
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:~p] Query took ~pms: ~s <- ~p",
                [Name, tdelta(T0), Q, Vs]),
    Rows.
