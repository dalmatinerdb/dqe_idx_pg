-module(dqe_idx_pg).
-behaviour(dqe_idx).

%% API exports
-export([
         init/0,
         lookup/1, lookup/2, lookup_tags/1,
         collections/0, metrics/1, namespaces/1, namespaces/2,
         tags/2, tags/3, values/3, values/4, expand/2,
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
    lookup(Query, []).

lookup(Query, Groupings) ->
    {ok, Q, Vs} = query_builder:lookup_query(Query, Groupings),
    {ok, _Cols, Rows} = run_query(Q, Vs, "lookup/2"),
    {ok, Rows}.

lookup_tags(Query) ->
    {ok, Q, Vs} = query_builder:lookup_tags_query(Query),
    {ok, _Cols, Rows} = run_query(Q, Vs, "lookup_tags"),
    {ok, Rows}.

collections() ->
    Q = "SELECT DISTINCT collection FROM metrics",
    Vs = [],
    {ok, _Cols, Rows} = run_query(Q, Vs, "collections"),
    {ok, strip_tpl(Rows)}.

metrics(Collection) ->
    Q = "SELECT DISTINCT metric FROM metrics WHERE collection = $1",
    Vs = [Collection],
    {ok, _Cols, Rows} = run_query(Q, Vs, "metrics"),
    {ok, strip_tpl(Rows)}.

namespaces(Collection) ->
    Q = "SELECT DISTINCT(namespace) FROM tags "
        "LEFT JOIN metrics ON tags.metric_id = metrics.id "
        "WHERE metrics.collection = $1",
    Vs = [Collection],
    {ok, _Cols, Rows} = run_query(Q, Vs, "namespaces/1"),
    {ok, strip_tpl(Rows)}.

namespaces(Collection, Metric) ->
    Q = "SELECT DISTINCT(namespace) FROM tags "
        "LEFT JOIN metrics ON tags.metric_id = metrics.id "
        "WHERE metrics.collection = $1 AND metrics.metric = $2",
    Vs = [Collection, Metric],
    {ok, _Cols, Rows} = run_query(Q, Vs, "namespaces/2"),
    {ok, strip_tpl(Rows)}.

tags(Collection, Namespace) ->
    Q = "SELECT DISTINCT(name) FROM tags "
        "LEFT JOIN metrics ON tags.metric_id = metrics.id "
        "WHERE metrics.collection = $1 AND tags.namespace = $2",
    Vs = [Collection, Namespace],
    {ok, _Cols, Rows} = run_query(Q, Vs, "tags/2"),
    {ok, strip_tpl(Rows)}.

tags(Collection, Metric, Namespace) ->
    Q = "SELECT DISTINCT(name) FROM tags "
        "LEFT JOIN metrics ON tags.metric_id = metrics.id "
        "WHERE metrics.collection = $1 AND metrics.metric = $2 "
        "AND tags.namespace = $3",
    Vs = [Collection, Metric, Namespace],
    {ok, _Cols, Rows} = run_query(Q, Vs, "tags/3"),
    {ok, strip_tpl(Rows)}.

values(Collection, Namespace, Tag) ->
    Q = "SELECT DISTINCT(value) FROM tags "
        "LEFT JOIN metrics ON tags.metric_id = metrics.id "
        "WHERE metrics.collection = $1 AND tags.namespace = $2 AND name = $3",
    Vs = [Collection, Namespace, Tag],
    {ok, _Cols, Rows} = run_query(Q, Vs, "values/3"),
    {ok, strip_tpl(Rows)}.

values(Collection, Metric, Namespace, Tag) ->
    Q = "SELECT DISTINCT(value) FROM tags "
        "LEFT JOIN metrics ON tags.metric_id = metrics.id "
        "WHERE metrics.collection = $1 AND metrics.metric = $2 "
        "AND tags.namespace = $3 AND name = $4",
    Vs = [Collection, Metric, Namespace, Tag],
    {ok, _Cols, Rows} = run_query(Q, Vs, "values/4"),
    {ok, strip_tpl(Rows)}.

expand(Bucket, Globs) ->
    {ok, Q, Vs} = query_builder:glob_query(Bucket, Globs),
    {ok, _Cols, Rows} = run_query(Q, Vs, "expand/2"),
    Rows1 = [K || {K} <- Rows],
    {ok, {Bucket, Rows1}}.

-spec add(Collection::binary(),
          Metric::binary(),
          Bucket::binary(),
          Key::binary()) ->
                 {ok, MetricIdx::non_neg_integer()} |
                 {error, Error::term()}.

add(Collection, Metric, Bucket, Key) ->
    Q = "SELECT add_metric($1, $2, $3, $4)",
    Vs = [Collection, Metric, Bucket, Key],
    case run_query(Q, Vs, "add/4") of
        {ok, [_], [{ID}]} ->
            {ok, ID};
        E ->
            E
    end.

add(Collection, Metric, Bucket, Key, []) ->
    add(Collection, Metric, Bucket, Key);

add(Collection, Metric, Bucket, Key, NVs) ->
    {ok, MID} = add(Collection, Metric, Bucket, Key),
    {Q, Vs} = query_builder:add_tags(MID, NVs),
    case run_query(Q, Vs, "add/5") of
        {ok, _, _} ->
            {ok, MID};
        E ->
            E
    end.

update(Collection, Metric, Bucket, Key, NVs) ->
    {ok, MID} = add(Collection, Metric, Bucket, Key),
    {Q, Vs} = query_builder:update_tags(MID, NVs),
    case run_query(Q, Vs, "update/5") of
        {ok, _, _} ->
            {ok, MID};
        E ->
            E
    end.

delete(Collection, Metric, Bucket, Key) ->
    Q = "DELETE FROM metrics WHERE collection = $1 AND " ++
        "metric = $2 AND bucket = $3 AND key = $4",
    Vs = [Collection, Metric, Bucket, Key],
    case run_query(Q, Vs, "delete/4") of
        {ok, _} ->
            ok;
        E ->
            E
    end.

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
    Q = "DELETE FROM tags WHERE metric_id = $1 AND "
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

strip_tpl(L) ->
    [E || {E} <- L].

get_id(Collection, Metric, Bucket, Key) ->
    Q = "SELECT id FROM metrics WHERE "
        "collection = $1 AND "
        "metric = $2 AND "
        "bucket = $3 AND "
        "key = $4",
    Vs = [Collection, Metric, Bucket, Key],
    case run_query(Q, Vs, "get_id/4") of
        {ok, [_], [{ID}]} ->
            {ok, ID};
        {ok, _, _} ->
            not_found;
        E ->
            E
    end.

%% Run the given query and log timing information
run_query(Q, Vs, Name) ->
    T0 = erlang:system_time(),
    R = pgapp:equery(Q, Vs),
    Status = element(1, R),
    Delta = [Name, tdelta(T0), Q, Vs],
    case Status of
        ok ->
            lager:debug("[dqe_idx:pg:~p] Query took ~pms: ~s <- ~p", Delta);
        _ ->
            lager:info("[dqe_idx:pg:~p] Query failed after ~pms: ~s <- ~p",
                       Delta)
    end,
    R.

%% Returns the time difference in milliseconds between `T0' and the current
%% system time
tdelta(T0) ->
    (erlang:system_time() - T0) / 1000 / 1000.
