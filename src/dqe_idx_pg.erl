-module(dqe_idx_pg).
-behaviour(dqe_idx).

-include("dqe_idx_pg.hrl").

%% API exports
-export([
         init/0,
         lookup/1, lookup/2, lookup_tags/1,
         collections/0, metrics/1, metrics/3, namespaces/1, namespaces/2,
         tags/2, tags/3, values/3, values/4, expand/2,
         add/4, add/5, update/5,
         delete/4, delete/5
        ]).

-import(dqe_idx_pg_utils, [decode_ns/1, hstore_to_tags/1, kvpair_to_tag/1]).

-define(TIMEOUT, 5 * 1000).

%%====================================================================
%% API functions
%%====================================================================

init() ->
    Opts = [size, max_overflow, database, username, password],
    Opts1 = [{O, application:get_env(dqe_idx_pg, O, undefined)}
             || O <- Opts],
    {Host, Port} = case application:get_env(dqe_idx_pg, server) of
        {ok, {H, P}} ->
            {H, P};
        _ ->
            {ok, H} = application:get_env(dqe_idx_pg, host),
            {ok, P} = application:get_env(dqe_idx_pg, port),
            {H, P}
        end,
    pgapp:connect([{host, Host}, {port, Port} | Opts1]).

lookup(Query) ->
    {ok, Q, Vs} = query_builder:lookup_query(Query, []),
    execute({select, "lookup/1", Q, Vs}).

lookup(Query, Groupings) ->
    {ok, Q, Vs} = query_builder:lookup_query(Query, Groupings),
    {ok, Rows} = execute({select, "lookup/2", Q, Vs}),
    R = [{Bucket, Key, get_values(Groupings, Dimensions)} ||
            {Bucket, Key, Dimensions} <- Rows],
    {ok, R}.

lookup_tags(Query) ->
    {ok, Q, Vs} = query_builder:lookup_tags_query(Query),
    {ok, Rows} = execute({select, "lookup_tags/1", Q, Vs}),
    R = [kvpair_to_tag(KV) || KV <- Rows],
    {ok, R}.

collections() ->
    {ok, Q, Vs} = query_builder:collections_query(),
    {ok, Rows} = execute({select, "collections/0", Q, Vs}),
    {ok, strip_tpl(Rows)}.

metrics(Collection) ->
    {ok, Q, Vs} = query_builder:metrics_query(Collection),
    {ok, Rows} = execute({select, "metrics/1", Q, Vs}),
    R = [M || {M} <- Rows],
    {ok, R}.

metrics(Collection, Prefix, Depth) ->
    {ok, Q, Vs} = query_builder:metrics_query(Collection, Prefix, Depth),
    {ok, Rows} = execute({select, "metrics/3", Q, Vs}),
    R = [M || {M} <- Rows],
    {ok, R}.

namespaces(Collection) ->
    {ok, Q, Vs} = query_builder:namespaces_query(Collection),
    {ok, Rows} = execute({select, "namespaces/1", Q, Vs}),
    {ok, decode_ns_rows(Rows)}.

namespaces(Collection, Metric) ->
    {ok, Q, Vs} = query_builder:namespaces_query(Collection, Metric),
    {ok, Rows} = execute({select, "namespaces/2", Q, Vs}),
    {ok, decode_ns_rows(Rows)}.

tags(Collection, Namespace) ->
    {ok, Q, Vs} = query_builder:tags_query(Collection, Namespace),
    {ok, Rows} = execute({select, "tags/2", Q, Vs}),
    {ok, strip_tpl(Rows)}.

tags(Collection, Metric, Namespace) ->
    {ok, Q, Vs} = query_builder:tags_query(Collection, Metric, Namespace),
    {ok, Rows} = execute({select, "tags/3", Q, Vs}),
    {ok, strip_tpl(Rows)}.

values(Collection, Namespace, Tag) ->
    {ok, Q, Vs} = query_builder:values_query(Collection, Namespace, Tag),
    {ok, Rows} = execute({select, "values/3", Q, Vs}),
    {ok, strip_tpl(Rows)}.

values(Collection, Metric, Namespace, Tag) ->
    {ok, Q, Vs} = query_builder:values_query(Collection, Metric,
                                             Namespace, Tag),
    {ok, Rows} = execute({select, "values/4", Q, Vs}),
    {ok, strip_tpl(Rows)}.

expand(Bucket, []) when is_binary(Bucket) ->
    {ok, {Bucket, []}};

expand(Bucket, Globs) when
      is_binary(Bucket),
      is_list(Globs) ->
    {ok, Q, Vs} = query_builder:glob_query(Bucket, Globs),
    {ok, Rows} = execute({select, "expand/2", Q, Vs}),
    Metrics = [M || {M} <- Rows],
    {ok, {Bucket, Metrics}}.

-spec add(collection(),
          metric(),
          bucket(),
          key()) -> ok | {ok, row_id()} | sql_error().
add(Collection, Metric, Bucket, Key) ->
    add(Collection, Metric, Bucket, Key, []).

-spec add(collection(),
          metric(),
          bucket(),
          key(),
          [tag()]) -> ok | {ok, row_id()} | sql_error().
add(Collection, Metric, Bucket, Key, Tags) ->
    {ok, Q, Vs} = command_builder:add_metric(
                    Collection, Metric, Bucket, Key, Tags),
    case execute({command, "add/5", Q, Vs}) of
        {ok, 0, []} ->
            ok;
        {ok, _Count, [{Dims}]} ->
            {ok, hstore_to_tags(Dims)};
        EAdd ->
            EAdd
    end.

-spec update(collection(),
             metric(),
             bucket(),
             key(),
             [tag()]) -> {ok, row_id()} | not_found() | sql_error().
update(Collection, Metric, Bucket, Key, NVs) ->
    {ok, Q, Vs} = command_builder:update_tags(
                    Collection, Metric, Bucket, Key, NVs),
    case execute({command, "update/5", Q, Vs}) of
        {ok, 0, []} ->
            ok;
        {ok, _Count, [{Dims}]} ->
            {ok, hstore_to_tags(Dims)};
        EAdd ->
            EAdd
    end.

-spec delete(collection(),
             metric(),
             bucket(),
             key()) -> ok | sql_error().
delete(Collection, Metric, Bucket, Key) ->
    {ok, Q, Vs} = command_builder:delete_metric(Collection, Metric,
                                                Bucket, Key),
    case execute({command, "delete/4", Q, Vs}) of
        {ok, _Count, _Rows} ->
            ok;
        E ->
            E
    end.

-spec delete(collection(),
             metric(),
             bucket(),
             key(),
             [{tag_ns(), tag_name()}]) -> ok | sql_error().
delete(Collection, Metric, Bucket, Key, Tags) ->
    {ok, Q, Vs} = command_builder:delete_tags(
                    Collection, Metric, Bucket, Key, Tags),
    case execute({command, "delete/5", Q, Vs}) of
        {ok, _Count, _Rows} ->
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

decode_ns_rows(Rows) ->
    [decode_ns(E) || {E} <- Rows].

execute({select, Name, Q, Vs}) ->
    T0 = erlang:system_time(),
    case pgapp:equery(Q, Vs, timeout()) of
        {ok, _Cols, Rows} ->
            lager:debug("[dqe_idx:pg:~p] PG Query took ~pms: ~s <- ~p",
                        [Name, tdelta(T0), Q, Vs]),
            {ok, Rows};
        E ->
            report_error(Name, Q, Vs, T0, E)
    end;
execute({command, Name, Q, Vs}) ->
    T0 = erlang:system_time(),
    case pgapp:equery(Q, Vs, timeout()) of
        {ok, Count} ->
            lager:debug("[dqe_idx:pg:~p] PG Query took ~pms: ~s <- ~p",
                        [Name, tdelta(T0), Q, Vs]),
            {ok, Count, []};
        {ok, Count, _Cols, Rows} ->
            lager:debug("[dqe_idx:pg:~p] PG Query took ~pms: ~s <- ~p",
                        [Name, tdelta(T0), Q, Vs]),
            {ok, Count, Rows};
        E ->
            report_error(Name, Q, Vs, T0, E)
    end.

report_error(Name, Q, Vs, T0, E) ->
    lager:info("[dqe_idx:pg:~p] PG Query failed after ~pms: ~s <- ~p:"
               " ~p", [Name, tdelta(T0), Q, Vs, E]),
    E.

get_values(Grouping, {KVs}) when is_list(KVs) ->
    Tags = [kvpair_to_tag(KV) || KV <- KVs],
    get_values(Grouping, Tags, []).

get_values([], _Tags, Acc) ->
    lists:reverse(Acc);
get_values([TagKey | Rest], Tags, Acc) ->
    Value = get_tag_value(TagKey, Tags),
    get_values(Rest, Tags, [Value | Acc]).

get_tag_value({_, _}, []) ->
    undefined;
get_tag_value({Ns, Name}, [{TNs, TName, TValue} | _])
  when Ns =:= TNs, Name =:= TName ->
    TValue;
get_tag_value(TagKey, [_ | Rest]) ->
    get_tag_value(TagKey, Rest).

timeout() ->
    application:get_env(dqe_idx_pg, timeout, ?TIMEOUT).
