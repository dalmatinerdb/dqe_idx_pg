-module(dqe_idx_pg).
-behaviour(dqe_idx).

%% API exports
-export([lookup/1,
         add/4,
         add/5,
         add/6,
         delete/6,
         delete/4,
         init/0
        ]).

init() ->
    Opts = [size, database, username, password],
    Opts1 = [{O, application:get_env(dqe_idx_pg, O, undefined)}
             || O <- Opts],
    {ok, {Host, Port}} = application:get_env(dqe_idx_pg, server),
    pgapp:connect([{host, Host}, {port, Port} | Opts1]).

%%====================================================================
%% API functions
%%====================================================================

lookup(Query) ->
    {ok, Q, Vs} = query_builder:lookup_query(Query),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    Rows.

-spec add(Collection::binary(),
          Metric::binary(),
          Bucket::binary(),
          Key::binary()) ->
                 {ok, MetricIdx::non_neg_integer()} |
                 {error, Error::term()}.

add(Collection, Metric, Bucket, Key) ->
    Q = "SELECT add_metric($1, $2, $3, $4)",
    Vs = [Collection, Metric, Bucket, Key],
    case pgapp:equery(Q, Vs) of
        {ok,[_],[{ID}]} ->
            {ok, ID};
        E ->
            E
    end.

-spec add(Collection::binary(),
          Metric::binary(),
          Bucket::binary(),
          Key::binary(),
          TagName::binary(),
          TagValue::binary()) ->
                 {ok, {MetricIdx::non_neg_integer(), TagIdx::non_neg_integer()}}|
                 {error, Error::term()}.



add(Collection, Metric, Bucket, Key, []) ->
    add(Collection, Metric, Bucket, Key);

add(Collection, Metric, Bucket, Key, NVs) ->
    {ok, MID} = add(Collection, Metric, Bucket, Key),
    {Q, Vs} = query_builder:add_tags(MID, NVs),
    case pgapp:equery(Q, Vs) of
        {ok,_,_} ->
            {ok, MID};
        E ->
            E
    end.

add(Collection, Metric, Bucket, Key, Name, Value) ->
    add(Collection, Metric, Bucket, Key, [{Name, Value}]).

delete(Collection, Metric, Bucket, Key) ->
    Q = "DELETE FROM metrics WHERE collection = $1 " ++
        "metric = $2 AND bucket = $3 AND key = $4",
    Vs = [Collection, Metric, Bucket, Key],
    case pgapp:equery(Q, Vs) of
        {ok,[_],[{ID}]} ->
            {ok, ID};
        E ->
            E
    end.

-spec delete(Collection::binary(),
          Metric::binary(),
          Bucket::binary(),
          Key::binary(),
          TagName::binary(),
          TagValue::binary()) ->
    ok |
    {error, Error::term()}.

delete(Bucket, Metric, LookupBucket, LookupMetric, TagKey, TagValue) ->
    io:format("~p~n", [[Bucket, Metric, LookupBucket, LookupMetric, TagKey, TagValue]]).

%%====================================================================
%% Internal functions
%%====================================================================
