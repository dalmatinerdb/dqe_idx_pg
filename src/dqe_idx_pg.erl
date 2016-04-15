-module(dqe_idx_pg).
-behaviour(dqe_idx).

%% API exports
-export([lookup/1, connect/0, close/1]).

%% TODO Configurable values
-define(HOST, "localhost").
-define(PORT, 5432).
-define(USER, "ddb").
-define(PASSWORD, "ddb").
-define(DATABASE, "metric_metadata").

%%====================================================================
%% API functions
%%====================================================================

lookup(Query) ->
    {ok, Query, Values} = query_builder:lookup_query(Query),
    {ok, C} = connect(),
    {ok, _Cols, Rows} = epgsql:equery(C, Query, Values),
    close(C),
    Rows.

connect() ->
    epgsql:connect(?HOST, ?USER, ?PASSWORD, [{database, ?DATABASE},
                                             {timeout, 4000}]).

close(C) ->
    epgsql:close(C).

%%====================================================================
%% Internal functions
%%====================================================================
