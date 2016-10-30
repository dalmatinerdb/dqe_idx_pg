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
    {Host, Port} = case application:get_env(dqe_idx_pg, server) of
        {ok, {Host, Port}} ->
            {Host, Port};
        _ ->
            {ok, Host} = application:get_env(dqe_idx_pg, host),
            {ok, Port} = application:get_env(dqe_idx_pg, port),
            {Host, Port}
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
    {ok, QueryMap} = query_builder:glob_query(Bucket, Globs),
    RowSets = [begin
                   Rows = execute({select, "expand/2", Q, Vs}),
                   sets:from_list(Rows)
               end || {Q, Vs} <- QueryMap],

    %% Destructuring into [H | T] is safe since the cardinality of `RowSets' is
    %% equal to that of `Globs'
    [H | T] = RowSets,
    UniqueRows = lists:foldl(fun sets:union/2, H, T),
    Metrics = [M || {M} <- sets:to_list(UniqueRows)],
    {ok, {Bucket, Metrics}}.

-spec get_id(collection(),
             metric(),
             bucket(),
             key()) -> {ok, row_id()} | not_found().
get_id(Collection, Metric, Bucket, Key) ->
    {ok, Q, Vs} = query_builder:get_id_query(Collection, Metric, Bucket, Key),
    case execute({select, "get_id/4", Q, Vs}) of
        [{ID}] ->
            {ok, ID};
        _ ->
            {'error', not_found}
    end.

-spec add(collection(),
          metric(),
          bucket(),
          key()) -> ok | {ok, row_id()} | sql_error().
add(Collection, Metric, Bucket, Key) ->
    {ok, Q, Vs} = command_builder:add_metric(Collection, Metric, Bucket, Key),
    case execute({command, "add/4", Q, Vs}) of
        {ok, 0, []} ->
            ok;
        {ok, _Count, [{MID}]} ->
            {ok, MID};
        EAdd ->
            EAdd
    end.

-spec add(collection(),
          metric(),
          bucket(),
          key(),
          [tag()]) -> ok | {ok, row_id()} | sql_error().
add(Collection, Metric, Bucket, Key, []) ->
    add(Collection, Metric, Bucket, Key);
add(Collection, Metric, Bucket, Key, NVs) ->
    case add(Collection, Metric, Bucket, Key) of
        ok ->
            ok;
        {ok, MID} ->
            {ok, Q, Vs} = command_builder:add_tags(MID, Collection, NVs),
            {ok, _Count, _Rows} = execute({command, "add_tags/3", Q, Vs}),
            {ok, MID};
        EAdd ->
            EAdd
    end.

-spec update(collection(),
             metric(),
             bucket(),
             key(),
             [tag()]) -> {ok, row_id()} | not_found() | sql_error().
update(Collection, Metric, Bucket, Key, NVs) ->
    AddRes = case add(Collection, Metric, Bucket, Key) of
                 ok ->
                     get_id(Collection, Metric, Bucket, Key);
                 RAdd ->
                     RAdd
             end,
    case AddRes of
        {ok, MID} ->
            {ok, Q, Vs} = command_builder:update_tags(MID, Collection, NVs),
            {ok, _Count, _Rows} = execute({command, "update_tags/3", Q, Vs}),
            {ok, MID};
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
    case get_id(Collection, Metric, Bucket, Key) of
        {error, not_found} ->
            ok;
        {ok, MetricID} ->
            Rs = [delete_tag(MetricID, NS, Name) || {NS, Name} <- Tags],
            case [R || R <- Rs, R /= ok] of
                [] ->
                    ok;
                [E | _] ->
                    E
            end
    end.

%%====================================================================
%% Internal functions
%%====================================================================

tdelta(T0) ->
    (erlang:system_time() - T0)/1000/1000.

strip_tpl(L) ->
    [E || {E} <- L].

delete_tag(MetricID, Namespace, TagName) ->
    {ok, Q, Vs} = command_builder:delete_tag(MetricID, Namespace, TagName),
    case execute({command, "delete_tag/3", Q, Vs}) of
        {ok, _Count, _Rows} ->
            ok;
        E ->
            E
    end.

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
