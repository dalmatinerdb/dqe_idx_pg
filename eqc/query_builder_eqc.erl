-module(query_builder_eqc).

-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

%% In order to run these tests, a valid connection is needed in order to
%% validate queries against the schema.
-define(HOST, "localhost").
-define(PORT, 5432).
-define(USER, "ddb").
-define(PASSWORD, "ddb").
-define(DATABASE, "metric_metadata").

%%%-------------------------------------------------------------------
%%% Generators
%%%-------------------------------------------------------------------

collection() ->
    ?LAZY(oneof([<<"collection1">>,
                 <<"collection2">>,
                 <<"collection3">>])).

metric() ->
    ?LAZY(oneof([[<<"base">>, <<"cpu">>],
                 [<<"bytes">>, <<"sent">>]])).

globs() ->
    ?LAZY(non_empty(list(glob()))).

glob() ->
    ?LAZY(oneof([metric(), ['*']])).

tag() ->
    ?LAZY({tag, binary(), oneof([<<"role">>, <<"host">>])}).

tagValue() ->
    ?LAZY(oneof([<<"web">>,
                 <<"db">>,
                 <<"load-balancer">>,
                 <<"worker">>])).

where() ->
    ?SIZED(Size, where(Size)).

where(0) -> {'=', tag(), tagValue()};
where(Size) ->
    ?LAZY(oneof([where(0),
                 {'and', where(Size - 1), where(Size - 1)},
                 {'or', where(Size - 1), where(Size - 1)}
                ])).

query() ->
    oneof([{'in', collection(), metric()},
           {'in', collection(), metric(), where()}]).

%%%-------------------------------------------------------------------
%%% Properties
%%%-------------------------------------------------------------------
prop_lookup_queries_valid() ->
    ?FORALL({Query}, {query()},
            begin
                {ok, C} = connect(),
                {ok, QueryStr, _Values} = query_builder:lookup_query(Query, []),
                {Res, _} = epgsql:parse(C, QueryStr),
                close(C),
                Res == ok
            end).

prop_glob_queries_valid() ->
    ?FORALL({Bucket, Globs}, {collection(), globs()},
            begin
                {ok, C} = connect(),
                {ok, QueryStr, _Values} = query_builder:glob_query(Bucket,
                                                                   Globs),
                {Res, _} = epgsql:parse(C, QueryStr),
                close(C),
                Res == ok
            end).


connect() ->
    epgsql:connect(?HOST, ?USER, ?PASSWORD, [{database, ?DATABASE},
                                             {timeout, 4000}]).

close(C) ->
    epgsql:close(C).
