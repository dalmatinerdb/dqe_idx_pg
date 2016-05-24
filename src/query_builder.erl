-module(query_builder).
-export([lookup_query/2, lookup_tags_query/1, add_tags/2, update_tags/2,
         glob_query/2, i2l/1]).

-define(TAG_TABLE, "tags").
-define(METRIC_TABLE, "metrics").

%%====================================================================
%% API
%%====================================================================

lookup_query({in, Collection, Metric}, Groupings) when is_list(Metric) ->
    MetricBin = dproto:metric_from_list(Metric),
    lookup_query({in, Collection, MetricBin}, Groupings);
lookup_query({in, Collection, Metric, Where}, Groupings) when is_list(Metric) ->
    MetricBin = dproto:metric_from_list(Metric),
    lookup_query({in, Collection, MetricBin, Where}, Groupings);
lookup_query({in, Collection, Metric}, Grouping) ->
    GroupingCount = length(Grouping),
    GroupingNames = grouping_names(GroupingCount),
    Query = ["SELECT DISTINCT bucket, key ",
             grouping_select(GroupingNames),
             "FROM ", ?METRIC_TABLE, " ",
             grouping_join(GroupingNames),
             "WHERE collection = $1 AND metric = $2 ",
             grouping_where(GroupingNames, 3)],
    Values = [Collection, Metric | Grouping],
    {ok, Query, Values};
lookup_query({in, Bucket, Metric, Where}, Grouping) ->
    GroupingCount = length(Grouping),
    GroupingNames = grouping_names(GroupingCount),
    Query = ["SELECT DISTINCT bucket, key",
             grouping_select(GroupingNames),
             "FROM ", ?METRIC_TABLE, " ",
             grouping_join(GroupingNames),
             "WHERE collection = $1 AND metric = $2 ",
             grouping_where(GroupingNames, 3),
             "AND "],
    {_N, TagPairs, TagPredicate} = build_tag_lookup(Where, 3 + GroupingCount),
    Values = [Bucket, Metric | Grouping ++ TagPairs],
    {ok, Query ++ TagPredicate, Values}.

lookup_tags_query({in, Collection, Metric}) when is_list(Metric) ->
    lookup_tags_query({in, Collection, dproto:metric_from_list(Metric)});
lookup_tags_query({in, Collection, Metric, Where}) when is_list(Metric) ->
    lookup_tags_query({in, Collection, dproto:metric_from_list(Metric), Where});
lookup_tags_query({in, Collection, Metric}) ->
    Query = "SELECT DISTINCT namespace, name, value "
        "FROM " ?TAG_TABLE " "
        "LEFT JOIN " ?METRIC_TABLE " ON "
        ?TAG_TABLE ".metric_id = " ?METRIC_TABLE ".id "
        "WHERE collection = $1 and metric = $2",
    Values = [Collection, Metric],
    {ok, Query, Values};
lookup_tags_query({in, Bucket, Metric, Where}) ->
    Query = "SELECT DISTINCT namespace, name, value "
        "FROM " ?TAG_TABLE " "
        "LEFT JOIN " ?METRIC_TABLE " ON "
        ?TAG_TABLE ".metric_id = id "
        "WHERE collection = $1 and metric = $2"
        "AND ",
    {_N, TagPairs, TagPredicate} = build_tag_lookup(Where),
    Values = [Bucket, Metric | TagPairs],
    {ok, Query ++ TagPredicate, Values}.

glob_query(Bucket, Globs) ->
    Query = ["SELECT DISTINCT key ",
             "FROM ", ?METRIC_TABLE, " ",
             "WHERE bucket = $1 AND "],
    GlobWheres = [and_tags(glob_to_tags(Glob))
                  || Glob <- Globs],
    Where = or_tags(GlobWheres),
    {_N, TagPairs, TagPredicate} = build_tag_lookup(Where, 2),
    Values = [Bucket | TagPairs],
    {ok, Query ++ TagPredicate, Values}.


add_tags(MID, Tags) ->
    Fn = "add_tag",
    build_add_tags(MID, 1, Fn, Tags, "SELECT", []).

build_add_tags(MID, P, Fn, [{NS, N, V}], Q, Vs) ->
    {[Q, add_tag(Fn, P)], lists:reverse([V, N, NS, MID | Vs])};

build_add_tags(MID, P, Fn, [{NS, N, V} | Tags], Q, Vs) ->
    Q1 = [Q, add_tag(Fn, P), ","],
    Vs1 = [V, N, NS, MID | Vs],
    build_add_tags(MID, P+4, Fn, Tags, Q1, Vs1).

add_tag(Fn, P)  ->
    [" ", Fn, "($", i2l(P), ", "
     "$", i2l(P + 1), ", "
     "$", i2l(P + 2), ", "
     "$", i2l(P + 3), ")"].

update_tags(MID, Tags) ->
    Fn = "update_tag",
    build_add_tags(MID, 1, Fn, Tags, "SELECT", []).

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
    {N2, TagPairs2, ["(", Str1, " AND ", Str2, ")"]};
build_tag_lookup({'or', L, R}, N, TagPairs) ->
    {N1, TagPairs1, Str1} = build_tag_lookup(L, N, TagPairs),
    {N2, TagPairs2, Str2} = build_tag_lookup(R, N1, TagPairs1),
    {N2, TagPairs2, ["(", Str1, " OR ", Str2, ")"]};
build_tag_lookup({'=', {tag, NS, K}, V}, NIn, Vals) ->
    Str = ["id In (SELECT metric_id FROM tags WHERE ",
           " namespace = $", i2l(NIn),
           " AND name = $", i2l(NIn+1),
           " AND value = $", i2l(NIn+2), ")"],
    {NIn+3, [NS, K, V | Vals], Str}.

grouping_names(0) ->
    [];
grouping_names(N) ->
    ["g" ++ i2l(I) || I <- lists:seq(1, N)].

grouping_select([]) ->
    " ";
grouping_select([Name | R]) ->
    [", ARRAY[", Name, ".value" | grouping_select_(R)].

grouping_select_([]) ->
    "] ";
grouping_select_([Name | R]) ->
    [", ", Name, ".value" | grouping_select_(R)].

grouping_where([], _) ->
    "";
grouping_where([Name | R], Pos) ->
    ["AND ", Name, ".name = $",  i2l(Pos), " " | grouping_where(R, Pos + 1)].

grouping_join([]) ->
    " ";
grouping_join([N | R]) ->
    ["INNER JOIN ", ?TAG_TABLE, " AS ", N,
     " ON ", N, ".metric_id = ", ?METRIC_TABLE ".id " | grouping_join(R)].

i2l(I) ->
    integer_to_list(I).
