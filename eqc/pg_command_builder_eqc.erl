-module(pg_command_builder_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).

-import(eqc_helper, [bucket/0, collection/0, metric/0, key/0, row_id/0, tag/0,
                     tag_name/0, namespace/0, non_empty_list/1,
                     with_connection/1]).

-define(M, pg_command_builder).

timestamp() ->
    oneof([now, undefined, nat()]).
%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------

prop_add_metric() ->
    ?SETUP(
       fun eqc_helper:setup/0,
       ?FORALL(
          {Collection, Metric, Bucket, Key, Time, Ts},
          {collection(), metric(), bucket(), key(), timestamp(), non_empty_list(tag())},
          begin
              Fun = fun(C) ->
                            {ok, Q, _V} = ?M:add_metric(Collection, Metric, Bucket, Key, Time, Ts),
                            {Res, _} = epgsql:parse(C, Q),
                            Res
                    end,
              ok =:= with_connection(Fun)
          end
         )).

prop_delete_metric() ->
    ?SETUP(
       fun eqc_helper:setup/0,
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
              )).

prop_update_tags() ->
    ?SETUP(
       fun eqc_helper:setup/0,
       ?FORALL(GenData, {collection(), metric(), bucket(), key(),
                         non_empty_list(tag())},
               begin
                   {Collection, Metric, Bucket, Key, Tags} = GenData,
                   Fun = fun(C) ->
                                 {ok, Q, _V} = ?M:update_tags(Collection, Metric,
                                                              Bucket, Key, Tags),
                                 {Res, _} = epgsql:parse(C, Q),
                                 Res
                         end,
                   ok =:= with_connection(Fun)
               end
              )).

prop_delete_tags() ->
    ?SETUP(
       fun eqc_helper:setup/0,
       ?FORALL(GenData, {collection(), metric(), bucket(), key(),
                         non_empty_list(tag())},
               begin
                   {Collection, Metric, Bucket, Key, Tags} = GenData,
                   Fun = fun(C) ->
                                 {ok, Q, _V} = ?M:delete_tags(Collection, Metric,
                                                              Bucket, Key, Tags),
                                 {Res, _} = epgsql:parse(C, Q),
                                 Res
                         end,
                   ok =:= with_connection(Fun)
               end
              )).
