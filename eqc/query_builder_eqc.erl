-module(query_builder_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

%%%-------------------------------------------------------------------
%%% Generators
%%%-------------------------------------------------------------------

bucket() ->
    ?LAZY(oneof([<<"bucket1">>,
                <<"bucket2">>,
                <<"bucket3">>])).

metric() ->
    ?LAZY(oneof([[<<"base">>, <<"cpu">>],
                 [<<"bytes">>, <<"sent">>]])).

tag() ->
    ?LAZY(oneof([{<<"direction">>, <<"in">>},
                 {<<"direction">>, <<"out">>},
                 {<<"host">>, <<"web1">>},
                 {<<"host">>, <<"web2">>}])).

where() ->
    ?SIZED(Size, where(Size)).

where(0) -> tag();
where(Size) ->
    ?LAZY(oneof([where(0),
                 {'and', where(Size - 1), where(Size - 1)},
                 {'or', where(Size - 1), where(Size - 1)}
                ])).

query() ->
    oneof([{bucket(), metric()},
           {bucket(), metric(), where()}]).

%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------
prop_tags() ->
    ?FORALL({Query}, {query()},
            begin
                {ok, C} = dqe_idx_pg:connect(),
                {ok, QueryStr, _Values} = query_builder:lookup_query(Query),
                {Res, _} = epgsql:parse(C, QueryStr),
                dqe_idx_pg:close(C),
                Res == ok
            end).
