-module(query_builder).
-export([lookup_query/2, lookup_tags_query/1, add_tags/2, update_tags/2,
         glob_query/2, i2l/1]).

-include("dqe_idx_pg.hrl").

%%====================================================================
%% API
%%====================================================================

lookup_query({in, Collection, Metric}, Groupings)
  when is_binary(Metric) ->
    MetricL = dproto:metric_to_list(Metric),
    build_lookup_query(Collection, MetricL, Groupings);
lookup_query({in, Collection, Metric, Where}, Groupings)
  when is_binary(Metric) ->
    MetricL = dproto:metric_to_list(Metric),
    build_lookup_query(Collection, MetricL, Where, Groupings);
lookup_query({in, Collection, Metric}, Groupings) ->
    build_lookup_query(Collection, Metric, Groupings);
lookup_query({in, Bucket, Metric, Where}, Groupings) ->
    build_lookup_query(Bucket, Metric, Where, Groupings).

build_lookup_query(Collection, Metric, Grouping) ->
    GroupingCount = length(Grouping),
    GroupingNames = grouping_names(GroupingCount),
    {N, {MetricWhere, MetricName}} = metric_where(2, Metric),
    Query = ["SELECT DISTINCT bucket, key ",
             grouping_select(GroupingNames),
             "FROM " ?MET_TABLE " ",
             grouping_join(GroupingNames),
             "WHERE " ?MET_TABLE ".collection = $1 ",
             MetricWhere,
             grouping_where(GroupingNames, N)],
    FlatGrouping = lists:flatten([[Namespace, Name] ||
                                     {Namespace, Name} <- Grouping]),
    Values = [Collection | MetricName ++ FlatGrouping],
    {ok, Query, Values}.

build_lookup_query(Bucket, Metric, Where, Grouping)
  when is_list(Metric) ->
    GroupingCount = length(Grouping),
    GroupingNames = grouping_names(GroupingCount),
    {N, {MetricWhere, MetricName}} = metric_where(2, Metric),
    Query = ["SELECT DISTINCT bucket, key",
             grouping_select(GroupingNames),
             "FROM ", ?MET_TABLE, " ",
             grouping_join(GroupingNames),
             "WHERE " ?MET_TABLE ".collection = $1 ",
             MetricWhere,
             grouping_where(GroupingNames, N),
             "AND "],
    {_N, TagPairs, TagPredicate} =
        %% We need to multipy count by two since we got names
        %% and namespaces
        build_tag_lookup(Where, 3 + GroupingCount * 2),
    FlatGrouping = lists:flatten([[Namespace, Name] ||
                                     {Namespace, Name} <- Grouping]),
    Values = [Bucket | MetricName ++ FlatGrouping ++ TagPairs],
    {ok, Query ++ TagPredicate, Values}.

lookup_tags_query({in, Collection, Metric}) when is_binary(Metric) ->
    lookup_tags_query({in, Collection, dproto:metric_to_list(Metric)});
lookup_tags_query({in, Collection, Metric, Where}) when is_binary(Metric) ->
    lookup_tags_query({in, Collection, dproto:metric_to_list(Metric), Where});
lookup_tags_query({in, Collection, Metric})
  when is_list(Metric) ->
    Query = "SELECT DISTINCT namespace, name, value "
        "FROM " ?DIM_TABLE " "
        "LEFT JOIN " ?MET_TABLE " ON "
        ?DIM_TABLE ".metric_id = " ?MET_TABLE ".id "
        "WHERE " ?MET_TABLE ".collection = $1 and metric = $2",
    Values = [Collection, Metric],
    {ok, Query, Values};
lookup_tags_query({in, Bucket, Metric, Where})
  when is_list(Metric) ->
    Query = "SELECT DISTINCT namespace, name, value "
        "FROM " ?DIM_TABLE " "
        "LEFT JOIN " ?MET_TABLE " ON "
        ?DIM_TABLE ".metric_id = id "
        "WHERE collection = $1 and metric = $2"
        "AND ",
    {_N, TagPairs, TagPredicate} = build_tag_lookup(Where),
    Values = [Bucket, Metric | TagPairs],
    {ok, Query ++ TagPredicate, Values}.

glob_query(Bucket, Globs) ->
    Query = ["SELECT DISTINCT key ",
             "FROM ", ?MET_TABLE, " ",
             "WHERE bucket = $1 AND "],
    GlobWheres = [glob_where(Bucket, Query, Glob) || Glob <- Globs],
    {ok, GlobWheres}.

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

glob_where(Bucket, Query, Glob) ->
    Where = and_tags(glob_to_tags(Glob)),
    {_N, TagPairs, TagPredicate} = build_tag_lookup(Where, 2),
    Values = [Bucket | TagPairs],
    {Query ++ TagPredicate, Values}.

and_tags([E]) ->
    E;
and_tags([E| R]) ->
    {'and', E, and_tags(R)}.

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
    Str = ["id IN (SELECT metric_id FROM " ?DIM_TABLE " WHERE ",
           " namespace = $", i2l(NIn),
           " AND name = $", i2l(NIn+1),
           " AND value = $", i2l(NIn+2), ")"],
    {NIn+3, [NS, K, V | Vals], Str};
build_tag_lookup({'!=', {tag, NS, K}, V}, NIn, Vals) ->
    Str = ["id NOT IN (SELECT metric_id FROM " ?DIM_TABLE " WHERE ",
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

metric_where(N, undefined) ->
    {N, {"", []}};
metric_where(N, Metric) ->
    MetricPredicate = [" AND metric = $", i2l(N), " "],
    {N + 1, {MetricPredicate, [Metric]}}.

grouping_where([], _) ->
    "";
grouping_where([Name | R], Pos) ->
    ["AND ", Name, ".namespace = $",  i2l(Pos), " "
     "AND ", Name, ".name = $",  i2l(Pos + 1), " " |
     grouping_where(R, Pos + 2)].

grouping_join([]) ->
    " ";
grouping_join([N | R]) ->
    ["INNER JOIN ", ?DIM_TABLE, " AS ", N,
     " ON ", N, ".metric_id = ", ?MET_TABLE ".id " | grouping_join(R)].

i2l(I) ->
    integer_to_list(I).
