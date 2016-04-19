-module(query_builder).
-export([lookup_query/1, add_tags/2, glob_query/2]).

-define(TAG_TABLE, "tags").
-define(METRIC_TABLE, "metrics").

%%====================================================================
%% API
%%====================================================================

lookup_query({in, Collection, Metric}) when is_list(Metric) ->
    lookup_query({in, Collection, dproto:metric_from_list(Metric)});
lookup_query({in, Collection, Metric, Where}) when is_list(Metric) ->
    lookup_query({in, Collection, dproto:metric_from_list(Metric), Where});
lookup_query({in, Collection, Metric}) ->
    Query = ["SELECT DISTINCT bucket, key ",
             "FROM ", ?METRIC_TABLE, " ",
             "WHERE collection = $1 and metric = $2"],
    Values = [Collection, Metric],
    {ok, Query, Values};
lookup_query({in, Bucket, Metric, Where}) ->
    Query = ["SELECT DISTINCT bucket, key ",
             "FROM ", ?METRIC_TABLE, " ",
             "WHERE collection = $1 AND metric = $2 ",
             "AND id IN "],
    {_N, TagPairs, TagPredicate} = build_tag_lookup(Where),
    Values = [Bucket, Metric | TagPairs],
    {ok, Query ++ TagPredicate, Values}.


glob_query(Bucket, Globs) ->
    Query = ["SELECT DISTINCT key ",
             "FROM ", ?METRIC_TABLE, " ",
             "WHERE bucket = $1 ",
             "AND id IN "],
    GlobWheres = [and_tags(glob_to_tags(Glob))
                  || Glob <- Globs],
    Where = or_tags(GlobWheres),
    {_N, TagPairs, TagPredicate} = build_tag_lookup(Where, 2),
    Values = [Bucket | TagPairs],
    {ok, Query ++ TagPredicate, Values}.

add_tags(MID, Tags) ->
    build_tags(MID, 1, Tags, "SELECT", []).

build_tags(MID, P, [{NS, N, V}], Q, Vs) ->
    {[Q, add_tag(P)], lists:reverse([V, N, NS, MID | Vs])};

build_tags(MID, P, [{NS, N, V} | Tags], Q, Vs) ->
    Q1 = [Q, add_tag(P), ","],
    Vs1 = [V, N, NS, MID | Vs],
    build_tags(MID, P+4, Tags, Q1, Vs1).

add_tag(P)  ->
    [" add_tag($", integer_to_list(P), ", "
     "$", integer_to_list(P + 1), ", "
     "$", integer_to_list(P + 2), ", "
     "$", integer_to_list(P + 3), ")"].

%%====================================================================
%% Internal functions
%%====================================================================

and_tags([E]) ->
    E;
and_tags([E| R]) ->
    {'and', E, and_tags(R)}.

or_tags([E]) ->
    E;
or_tags([E| R]) ->
    {'or', E, or_tags(R)}.

glob_to_tags(Glob) ->
    glob_to_tags(Glob, 1,
                 [{'=', {tag, <<"ddb">>, <<"key_length">>},
                   integer_to_binary(length(Glob))}]).

glob_to_tags([], _,  Tags) ->
    Tags;
glob_to_tags(['*' | R], N, Tags) ->
    glob_to_tags(R, N + 1, Tags);

glob_to_tags([E | R] , N, Tags) ->
    PosBin = integer_to_binary(N),
    T = {'=', {tag, <<"ddb">>, <<"part_", PosBin/binary>>}, E},
    glob_to_tags(R, N + 1, [T | Tags]).

build_tag_lookup(Where) ->
    build_tag_lookup(Where, 3).

build_tag_lookup(Where, N) ->
    build_tag_lookup(Where, N, []).

build_tag_lookup({'and', L, R}, N, TagPairs) ->
    {N1, TagPairs1, Str1} = build_tag_lookup(L, N, TagPairs),
    {N2, TagPairs2, Str2} = build_tag_lookup(R, N1, TagPairs1),
    {N2, TagPairs2, ["(", Str1, " INTERSECT ", Str2, ")"]};
build_tag_lookup({'or', L, R}, N, TagPairs) ->
    {N1, TagPairs1, Str1} = build_tag_lookup(L, N, TagPairs),
    {N2, TagPairs2, Str2} = build_tag_lookup(R, N1, TagPairs1),
    {N2, TagPairs2, ["(", Str1, " UNION ", Str2, ")"]};
build_tag_lookup({'=', {tag, NS, K}, V}, NIn, Vals) ->
    Str = ["(SELECT DISTINCT metric_id FROM tags WHERE ",
           " namespace = $", integer_to_list(NIn),
           " AND name = $", integer_to_list(NIn+1),
           " AND value = $", integer_to_list(NIn+2), ")"],
    {NIn+3, [NS, K, V | Vals], Str}.
