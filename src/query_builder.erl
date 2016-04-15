-module(query_builder).
-export([lookup_query/1]).

-define(TABLE, "tags").
-define(SEP, <<".">>).
%%====================================================================
%% API
%%====================================================================
lookup_query({Bucket, Metric}) ->
    Query = ["select distinct bucket, metric ",
                "from ", ?TABLE, " ",
                "where bucket = $1 and metric = $2"],
    Values = [Bucket, decode_metric(Metric)],
    {ok, Query, Values};
lookup_query({Bucket, Metric, Where}) ->
    Query = ["select distinct bucket, metric ",
             "from ", ?TABLE, " ",
             "where bucket = $1 and metric = $2 and "],
    {_N, TagPairs, TagPredicate} = unparse(Where),
    Values = [Bucket, decode_metric(Metric) | (TagPairs)],
    {ok, Query ++ TagPredicate, Values}.

%%====================================================================
%% Internal functions
%%====================================================================
unparse(Where) ->
    unparse0(Where, 3, []).

unparse0({'and', L, R}, N, TagPairs) ->
    {N1, TagPairs1, Str1} = unparse0(L, N, TagPairs),
    {N2, TagPairs2, Str2} = unparse0(R, N1, TagPairs1),
    {N2, TagPairs2, ["(", Str1, " and ", Str2, ")"]};
unparse0({'or', L, R}, N, TagPairs) ->
    {N1, TagPairs1, Str1} = unparse0(L, N, TagPairs),
    {N2, TagPairs2, Str2} = unparse0(R, N1, TagPairs1),
    {N2, TagPairs2, ["(", Str1, " or ", Str2, ")"]};
unparse0({K, V}, NIn, Vals) ->
    Str = ["(tag_name = $", integer_to_list(NIn),
           "AND tag_value = $", integer_to_list(NIn+1), ")"],
    {NIn+2, [K, V | Vals], Str}.

decode_metric(Metric) ->
    dproto:metric_to_string(dproto:metric_from_list(Metric), ?SEP).
