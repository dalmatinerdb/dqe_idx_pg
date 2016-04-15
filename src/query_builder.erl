-module(query_builder).
-export([lookup_query/1, add_tags/2]).

-define(TAG_TABLE, "tags").
-define(METRIC_TABLE, "metrics").

%%====================================================================
%% API
%%====================================================================
lookup_query({Collection, Metric}) ->
    Query = ["SELECT DISTINCT bucket, key ",
             "FROM ", ?METRIC_TABLE, " ",
             "WHERE collection = $1 and metric = $2"],
    Values = [Collection, Metric],
    {ok, Query, Values};
lookup_query({Bucket, Metric, Where}) ->
    Query = ["SELECT DISTINCT bucket, key ",
             "FROM ", ?METRIC_TABLE, " ",
             "WHERE collection = $1 AND metric = $2 ",
             "AND id IN "],
    {_N, TagPairs, TagPredicate} = unparse(Where),
    Values = [Bucket, Metric | TagPairs],
    {ok, Query ++ TagPredicate, Values}.

add_tags(MID, Tags) ->
    build_tags(MID, 1, Tags, "SELECT", []).

build_tags(MID, P, [{N, V}], Q, Vs) ->
    {[Q, add_tag(P)], lists:reverse([V, N, MID | Vs])};

build_tags(MID, P, [{N, V} | Tags], Q, Vs) ->
    Q1 = [Q, add_tag(P), ","],
    Vs1 = [V, N, MID | Vs],
    build_tags(MID, P+3, Tags, Q1, Vs1).

add_tag(P)  ->
    [" add_tag($", integer_to_list(P), ", "
     "$", integer_to_list(P + 1), ", "
     "$", integer_to_list(P + 2), ")"].

%%====================================================================
%% Internal functions
%%====================================================================
unparse(Where) ->
    unparse0(Where, 3, []).

unparse0({'and', L, R}, N, TagPairs) ->
    {N1, TagPairs1, Str1} = unparse0(L, N, TagPairs),
    {N2, TagPairs2, Str2} = unparse0(R, N1, TagPairs1),
    {N2, TagPairs2, ["(", Str1, " INTERSECT ", Str2, ")"]};
unparse0({'or', L, R}, N, TagPairs) ->
    {N1, TagPairs1, Str1} = unparse0(L, N, TagPairs),
    {N2, TagPairs2, Str2} = unparse0(R, N1, TagPairs1),
    {N2, TagPairs2, ["(", Str1, " UNION ", Str2, ")"]};
unparse0({K, V}, NIn, Vals) ->
    Str = ["(SELECT DISTINCT metric_id FROM tags WHERE ",
           " name = $", integer_to_list(NIn),
           " AND value = $", integer_to_list(NIn+1), ")"],
    {NIn+2, [K, V | Vals], Str}.


%% unparse0({'and', L, R}, N, TagPairs) ->
%%     {N1, TagPairs1, Str1} = unparse0(L, N, TagPairs),
%%     {N2, TagPairs2, Str2} = unparse0(R, N1, TagPairs1),
%%     {N2, TagPairs2, ["(", Str1, " AND ", Str2, ")"]};
%% unparse0({'or', L, R}, N, TagPairs) ->
%%     {N1, TagPairs1, Str1} = unparse0(L, N, TagPairs),
%%     {N2, TagPairs2, Str2} = unparse0(R, N1, TagPairs1),
%%     {N2, TagPairs2, ["(", Str1, " OR ", Str2, ")"]};
%% unparse0({K, V}, NIn, Vals) ->
%%     Str = ["(name = $", integer_to_list(NIn),
%%            " AND value = $", integer_to_list(NIn+1), ")"],
%%     {NIn+2, [K, V | Vals], Str}.
