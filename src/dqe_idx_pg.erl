-module(dqe_idx_pg).
-behaviour(dqe_idx).

%% API exports
-export([
         init/0, lookup/1, expand/2,
         add/4, add/5, add/7,
         delete/4, delete/5, delete/7
        ]).

%%====================================================================
%% API functions
%%====================================================================

init() ->
    Opts = [size, database, username, password],
    Opts1 = [{O, application:get_env(dqe_idx_pg, O, undefined)}
             || O <- Opts],
    {ok, {Host, Port}} = application:get_env(dqe_idx_pg, server),
    pgapp:connect([{host, Host}, {port, Port} | Opts1]).


lookup(Query) ->
    {ok, Q, Vs} = query_builder:lookup_query(Query),
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:lookup] Query too ~pms: ~s <- ~p",
                [tdelta(T0), Q, Vs]),
    {ok, Rows}.

expand(_Bucket, _Glob) ->
    {error, not_implemented}.

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
