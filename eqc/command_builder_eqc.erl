-module(command_builder_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).

-import(eqc_helper, [bucket/0, collection/0, metric/0, key/0, row_id/0, tag/0,
                     tag_name/0, namespace/0, non_empty_list/1,
                     with_connection/1]).

-define(M, command_builder).

%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------

prop_add_metric() ->
    ?FORALL(GenData, {collection(), metric(), bucket(), key()},
        begin
            {Collection, Metric, Bucket, Key} = GenData,
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:add_metric(Collection, Metric, Bucket, Key),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_delete_metric() ->
    ?FORALL(GenData, {collection(), metric(), bucket(), key()},
        begin
            {Collection, Metric, Bucket, Key} = GenData,
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:delete_metric(Collection, Metric, Bucket, Key),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_add_tags() ->
    ?FORALL(GenData, {row_id(), collection(), non_empty_list(tag())},
        begin
            {MID, Collection, Tags} = GenData,
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:add_tags(MID, Collection, Tags),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_update_tags() ->
    ?FORALL(GenData, {row_id(), collection(), non_empty_list(tag())},
        begin
            {MID, Collection, Tags} = GenData,
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:update_tags(MID, Collection, Tags),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).

prop_delete_tag() ->
    ?FORALL(GenData, {row_id(), namespace(), tag_name()},
        begin
            {MID, Namespace, TagName} = GenData,
            Fun = fun(C) ->
                {ok, Q, _V} = ?M:delete_tag(MID, Namespace, TagName),
                {Res, _} = epgsql:parse(C, Q),
                Res
            end,
            ok =:= with_connection(Fun)
        end
    ).


