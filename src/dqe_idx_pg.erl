-module(dqe_idx_pg).
-behaviour(dqe_idx).

%% API exports
-export([
         init/0,
         lookup/1, lookup/2, lookup_tags/1,
         collections/0, metrics/1, namespaces/2, tags/3, values/4,
         expand/2,
         add/4, add/5, add/7,
         delete/4, delete/5, delete/7
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
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:lookup] Query took ~pms: ~s <- ~p",
                [tdelta(T0), Q, Vs]),
    {ok, Rows}.

lookup_tags(Query) ->
    {ok, Q, Vs} = query_builder:lookup_tags_query(Query),
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:lookup] Query took ~pms: ~s <- ~p",
                [tdelta(T0), Q, Vs]),
    {ok, Rows}.

collections() ->
    Q = "SELECT DISTINCT collection FROM metrics",
    Vs = [],
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:collections] Query took ~pms: ~s",
                [tdelta(T0), Q]),
    {ok, strip_tpl(Rows)}.

metrics(Collection) ->
    Q = "SELECT DISTINCT metric FROM metrics WHERE collection = $1",
    Vs = [Collection],
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:metrics] Query took ~pms: ~s <- ~p",
                [tdelta(T0), Q, Vs]),
    {ok, strip_tpl(Rows)}.

namespaces(Collection, Metric) ->
    Q = "SELECT DISTINCT(namespace) FROM tags "
        "LEFT JOIN metrics ON tags.metric_id = metrics.id "
        "WHERE metrics.collection = $1 AND metrics.metric = $2",
    Vs = [Collection, Metric],
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:namespaces] Query took ~pms: ~s <- ~p",
                [tdelta(T0), Q, Vs]),
    {ok, strip_tpl(Rows)}.

tags(Collection, Metric, Namespace) ->
    Q = "SELECT DISTINCT(name) FROM tags "
        "LEFT JOIN metrics ON tags.metric_id = metrics.id "
        "WHERE metrics.collection = $1 AND metrics.metric = $2 "
        "AND tags.namespace = $3",
    Vs = [Collection, Metric, Namespace],
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:tags] Query took ~pms: ~s <- ~p",
                [tdelta(T0), Q, Vs]),
    {ok, strip_tpl(Rows)}.

values(Collection, Metric, Namespace, Tag) ->
    Q = "SELECT DISTINCT(value) FROM tags "
        "LEFT JOIN metrics ON tags.metric_id = metrics.id "
        "WHERE metrics.collection = $1 AND metrics.metric = $2 "
        "AND tags.namespace = $3 AND name = $4",
    Vs = [Collection, Metric, Namespace, Tag],
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:tags] Query took ~pms: ~s <- ~p",
                [tdelta(T0), Q, Vs]),
    {ok, strip_tpl(Rows)}.

expand(Bucket, Globs) ->
    {ok, Q, Vs} = query_builder:glob_query(Bucket, Globs),
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:glob] Query took ~pms: ~s <- ~p",
                [tdelta(T0), Q, Vs]),
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
    T0 = erlang:system_time(),
    case pgapp:equery(Q, Vs) of
        {ok,[_],[{ID}]} ->
            lager:debug("[dqe_idx:pg:add/4] Query too ~pms: ~s <- ~p",
                        [tdelta(T0), Q, Vs]),

            {ok, ID};
        E ->
            lager:info("[dqe_idx:pg:add/4] Query failed after ~pms: ~s <- ~p",
                       [tdelta(T0), Q, Vs]),
            E
    end.

add(Collection, Metric, Bucket, Key, []) ->
    add(Collection, Metric, Bucket, Key);

add(Collection, Metric, Bucket, Key, NVs) ->
    {ok, MID} = add(Collection, Metric, Bucket, Key),
    {Q, Vs} = query_builder:add_tags(MID, NVs),
    T0 = erlang:system_time(),
    case pgapp:equery(Q, Vs) of
        {ok,_,_} ->
            lager:debug("[dqe_idx:pg:add/5] Query too ~pms: ~s <- ~p",
                        [tdelta(T0), Q, Vs]),
            {ok, MID};
        E ->
            lager:info("[dqe_idx:pg:add/5] Query failed after ~pms: ~s <- ~p",
                       [tdelta(T0), Q, Vs]),
            E
    end.

-spec add(Collection::binary(),
          Metric::binary(),
          Bucket::binary(),
          Key::binary(),
          Namespace::binary(),
          TagName::binary(),
          TagValue::binary()) ->
                 {ok, MetricIdx::non_neg_integer()}|
                 {error, Error::term()}.

add(Collection, Metric, Bucket, Key, Namespace, Name, Value) ->
    add(Collection, Metric, Bucket, Key, [{Namespace, Name, Value}]).

delete(Collection, Metric, Bucket, Key) ->
    Q = "DELETE FROM metrics WHERE collection = $1 " ++
        "metric = $2 AND bucket = $3 AND key = $4",
    Vs = [Collection, Metric, Bucket, Key],
    T0 = erlang:system_time(),
    case pgapp:equery(Q, Vs) of
        {ok,[_],[{ID}]} ->
            lager:debug("[dqe_idx:pg:delete/4] Query too ~pms: ~s <- ~p",
                        [tdelta(T0), Q, Vs]),
            {ok, ID};
        E ->
            lager:info("[dqe_idx:pg:delete/4] Query failed after ~pms: ~s <- ~p",
                       [tdelta(T0), Q, Vs]),
            E
    end.

delete(_Bucket, _Metric, _LookupBucket, _LookupMetric, _Tags) ->
    {error, not_implemented}.

-spec delete(Collection::binary(),
             Metric::binary(),
             Bucket::binary(),
             Key::binary(),
             Namespace::binary(),
             TagName::binary(),
             TagValue::binary()) ->
                    ok |
                    {error, Error::term()}.

delete(_Bucket, _Metric, _LookupBucket, _LookupMetric, _Namespace, _TagKey,
       _TagValue) ->
    {error, not_implemented}.

%%====================================================================
%% Internal functions
%%====================================================================

tdelta(T0) ->
    (erlang:system_time() - T0)/1000/1000.

strip_tpl(L) ->
    [E || {E} <- L].
