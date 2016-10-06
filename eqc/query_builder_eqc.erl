-module(query_builder_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-define(M, query_builder).

%%%-------------------------------------------------------------------
%%% Generators
%%%-------------------------------------------------------------------

str() ->
    list(choose($a, $z)).

non_empty_string() ->
    ?SUCHTHAT(L, str(), length(L) >= 2).

non_empty_binary() ->
    ?LET(L, non_empty_string(), list_to_binary(L)).

non_empty_list(T) ->
    ?SUCHTHAT(L, list(T), L /= []).

pos_int() ->
    ?SUCHTHAT(I, int(), I > 0).

bucket() ->
    non_empty_binary().

namespace() ->
    non_empty_binary().

collection() ->
    non_empty_binary().

metric() ->
    non_empty_list(non_empty_binary()).

lqry_metric() ->
    oneof([metric(), undefined]).

tag_name() ->
    non_empty_binary().

tag() ->
    frequency(
      [{10, {tag, tag_name(), non_empty_binary()}},
       {1,  {tag, <<>>, non_empty_binary()}}]).

lookup() ->
    oneof([{in, collection(), lqry_metric()},
           {in, collection(), lqry_metric(), where()}]).

lookup_tags() ->
    oneof([{in, collection(), metric()},
           {in, collection(), metric(), where()}]).

where() ->
    ?SIZED(S, where_clause(S)).

where_clause(S) when S =< 1 ->
    oneof([{'=', tag(), non_empty_binary()},
           {'!=', tag(), non_empty_binary()}]);
where_clause(S) ->
    ?LAZY(?LET(N, choose(0, S - 1), where_clause_choice(N, S))).

where_clause_choice(N, S) ->
    oneof([{'and', where_clause(N), where_clause(S - N)}]).

prefix() ->
    list(non_empty_binary()).

%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------

prop_lookup() ->
    ?FORALL({LQuery}, {lookup()},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:lookup_query(LQuery, []),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_lookup_tags() ->
    ?FORALL({LTQuery}, {lookup_tags()},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:lookup_tags_query(LTQuery),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_metrics_by_prefix() ->
    ?FORALL({Collection, Prefix, Depth}, {collection(), prefix(), pos_int()},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:metrics_query(Collection, Prefix, Depth),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_collections() ->
    ?FORALL({}, {},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:collections_query(),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_metrics() ->
    ?FORALL({Collection}, {collection()},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:metrics_query(Collection),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_namespaces() ->
    ?FORALL({Collection}, {collection()},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:namespaces_query(Collection),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_metric_namespaces() ->
    ?FORALL({Collection, Metric}, {collection(), metric()},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:namespaces_query(Collection, Metric),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_tags() ->
    ?FORALL({Collection, Namespace}, {collection(), namespace()},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:tags_query(Collection, Namespace),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_metric_tags() ->
    ?FORALL({Collection, Metric, Namespace}, {collection(), metric(),
                                              namespace()},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:tags_query(Collection, Metric, Namespace),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_values() ->
    ?FORALL({Collection, Namespace, Tag}, {collection(), namespace(),
                                           tag_name()},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:values_query(Collection, Namespace, Tag),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_metric_values() ->
    ?FORALL({Collection, Metric, Namespace, Tag}, {collection(), metric(),
                                                   namespace(), tag_name()},
        begin
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:values_query(Collection, Metric,
                                              Namespace, Tag),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

-define(host, "localhost").
-define(port, 5432).

with_connection(F) ->
    with_connection(F, "ddb", []).

with_connection(F, Username, Args) ->
    Args2 = [{port, ?port}, {database, "metric_metadata"} | Args],
    {ok, C} = epgsql:connect(?host, Username, Args2),
    try
        F(C)
    after
        epgsql:close(C)
    end.
