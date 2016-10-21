-module(eqc_helper).

-include_lib("eqc/include/eqc.hrl").

-compile([export_all]).

-define(HOST, "localhost").
-define(PORT, 10433).

with_connection(F) ->
    with_connection(F, "ddb", []).

with_connection(F, Username, Args) ->
    Args2 = [{port, ?PORT}, {database, "metric_metadata"} | Args],
    {ok, C} = epgsql:connect(?HOST, Username, Args2),
    try
        F(C)
    after
        epgsql:close(C)
    end.

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

row_id() ->
    pos_int().

metric() ->
    non_empty_list(non_empty_binary()).

key() ->
    [non_empty_binary() | metric()].

lqry_metric() ->
    oneof([metric(), undefined]).

tag_ns() ->
    non_empty_binary().

tag_name() ->
    non_empty_binary().

tag() ->
    {tag_name(), tag_ns(), binary()}.

where_tag() ->
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
    oneof([{'=', where_tag(), non_empty_binary()},
           {'!=', where_tag(), non_empty_binary()}]);
where_clause(S) ->
    ?LAZY(?LET(N, choose(0, S - 1), where_clause_choice(N, S))).

where_clause_choice(N, S) ->
    oneof([{'and', where_clause(N), where_clause(S - N)}]).

prefix() ->
    list(non_empty_binary()).
