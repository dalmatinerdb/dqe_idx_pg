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
    Rows = execute({select, "lookup/1", Q, Vs}),
    {ok, Rows}.

lookup(Query, Groupings) ->
    {ok, Q, Vs} = query_builder:lookup_query(Query, Groupings),
    Rows = execute({select, "lookup/2", Q, Vs}),
    {ok, Rows}.

lookup_tags(Query) ->
    {ok, Q, Vs} = query_builder:lookup_tags_query(Query),
    Rows = execute({select, "lookup_tags/1", Q, Vs}),
    {ok, Rows}.

collections() ->
    {ok, Q, Vs} = query_builder:collections_query(),
    Rows = execute({select, "collections/0", Q, Vs}),
    {ok, strip_tpl(Rows)}.

metrics(Collection) ->
    {ok, Q, Vs} = query_builder:metrics_query(Collection),
    Rows = execute({select, "metrics/1", Q, Vs}),
    R = [M || {M} <- Rows],
    {ok, R}.

metrics(Collection, Prefix, Depth) ->
    {ok, Q, Vs} = query_builder:metrics_query(Collection, Prefix, Depth),
    Rows = execute({select, "metrics/3", Q, Vs}),
    R = [M || {M} <- Rows],
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
    {ok, Q, Vs} = query_builder:glob_query(Bucket, Globs),
    Rows = execute({select, "expand/2", Q, Vs}),
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
            %% TODO: perhaps decode dimensions to tags
            {ok, Dims};
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
            %% TODO: perhaps decode dimensions to tags
            {ok, Dims};
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
    %% TODO Just ignore errors when it is not existent
    {ok, Q, Vs} = command_builder:delete_tags(
                    Collection, Metric, Bucket, Key, Tags),
    case execute({command, "add_tags/3", Q, Vs}) of
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

execute({select, Name, Q, Vs}) ->
    T0 = erlang:system_time(),
    {ok, _Cols, Rows} = pgapp:equery(Q, Vs),
    lager:debug("[dqe_idx:pg:~p] Query took ~pms: ~s <- ~p",
                [Name, tdelta(T0), Q, Vs]),
    Rows;
execute({command, Name, Q, Vs}) ->
    T0 = erlang:system_time(),
    case pgapp:equery(Q, Vs) of
        {ok, Count} ->
            lager:debug("[dqe_idx:pg:~p] Query took ~pms: ~s <- ~p",
                        [Name, tdelta(T0), Q, Vs]),
            {ok, Count, []};
        {ok, Count, _Cols, Rows} ->
            lager:debug("[dqe_idx:pg:~p] Query took ~pms: ~s <- ~p",
                        [Name, tdelta(T0), Q, Vs]),
            {ok, Count, Rows};
        E ->
            lager:info("[dqe_idx:pg:~p] Query failed after ~pms: ~s <- ~p:"
                       " ~p", [Name, tdelta(T0), Q, Vs, E]),
            E
    end.
