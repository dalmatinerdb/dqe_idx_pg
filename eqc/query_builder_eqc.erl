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

where(_Gen, 0) -> tag();
where(Gen, Size) ->
    ?LAZY(oneof([where(Gen,0),
                 {'and', where(Gen, Size - 1), where(Gen, Size - 1)},
                 {'or', where(Gen, Size - 1), where(Gen, Size - 1)}
                ])).

query(Gen, Size) ->
    oneof([{bucket(), metric()},
           {bucket(), metric(), where(Gen, Size)}]).

%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------
prop_tags() ->
    ?FORALL({Query}, {query()},
            {ok, C} = dqe_idx_pg:connect(),
            {ok, QueryStr, Values} = query_builder:lookup_query(Query),
            {Res, _} = 

            dqe_idx:close(C))
            FailMsg = io:format("Query: ~p not parseable, for input: ~p 
                                because: ~p~n", [QueryStr, Query, R]),
            ?WHENFAIL(FailMsg, Res == ok)



 
