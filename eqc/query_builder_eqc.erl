-module(query_builder_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).

-import(eqc_helper, [lookup/0, lookup_tags/0, collection/0, prefix/0,
                     pos_int/0, metric/0, namespace/0, tag_name/0,
                     with_connection/1]).

-define(M, query_builder).

%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------

prop_lookup() ->
    ?FORALL({LQuery}, {lookup()},
        begin
            {ok, Q, _V} = ?M:lookup_query(LQuery, []),
            Fun = fun(C) ->
                epgsql:parse(C, Q)
            end,
            {Res, Reason} = with_connection(Fun),
            ?WHENFAIL(io:format(user, "Invalid query ~s:~n  ~p~n", [Q, Reason]),
                      ok =:= Res)
        end
    ).

prop_lookup_tags() ->
    ?FORALL({LTQuery}, {lookup_tags()},
        begin
            {ok, Q, _V} = ?M:lookup_tags_query(LTQuery),
            Fun = fun(C) ->
                epgsql:parse(C, Q)
            end,
            {Res, Reason} = with_connection(Fun),
            ?WHENFAIL(io:format(user, "Invalid query ~s:~n  ~p~n", [Q, Reason]),
                      ok =:= Res)
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

