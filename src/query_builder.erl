-module(query_builder).

-export([collections_query/0, metrics_query/1, metrics_query/3,
         namespaces_query/1, namespaces_query/2,
         lookup_query/2, lookup_tags_query/1,
         tags_query/2, tags_query/3, add_tags/2, update_tags/2,
         values_query/3, values_query/4,
         glob_query/2, get_id_query/4, i2l/1]).

-include("dqe_idx_pg.hrl").

%%====================================================================
%% API
%%====================================================================

collections_query() ->
    Query = "WITH RECURSIVE t AS ("
            "   SELECT MIN(collection) AS collection FROM " ?MET_TABLE
            "   UNION ALL"
            "   SELECT (SELECT MIN(collection) FROM " ?MET_TABLE
            "     WHERE collection > t.collection)"
            "   FROM t WHERE t.collection IS NOT NULL"
            "   )"
            "SELECT collection FROM t WHERE collection IS NOT NULL",
    Values = [],
    {ok, Query, Values}.

metrics_query(Collection)
  when is_binary(Collection) ->
    Query = "WITH RECURSIVE t AS ("
            "   SELECT MIN(metric) AS metric FROM "
            ?MET_TABLE
            "     WHERE collection = $1"
            "   UNION ALL"
            "   SELECT (SELECT MIN(metric) FROM "
            ?MET_TABLE
            "     WHERE metric > t.metric"
            "     AND collection = $1)"
            "   FROM t WHERE t.metric IS NOT NULL"
            "   )"
            "SELECT metric FROM t WHERE metric IS NOT NULL",
    Values = [Collection],
    {ok, Query, Values}.

metrics_query(Collection, Prefix, Depth)
  when is_binary(Collection),
       is_list(Prefix),
       Depth > 0 ->
    {_N, MetricVals, MetricPredicate} = metric_variant_where(Prefix, 6),
    Query = ["SELECT DISTINCT metric[$1:$2]",
             "FROM ", ?MET_TABLE, " ",
             "WHERE metric[$3:$4] IS NOT NULL AND collection = $5 "],
    From = 1 + length(Prefix),
    To = From + Depth - 1,
    Values = [From, To, From, To, Collection | MetricVals],
    {ok, Query ++ MetricPredicate, Values}.

namespaces_query(Collection)
  when is_binary(Collection) ->
    Query = "WITH RECURSIVE t AS ("
            "   SELECT MIN(namespace) AS namespace FROM "
            ?DIM_TABLE
            "     WHERE collection = $1"
            "   UNION ALL"
            "   SELECT (SELECT MIN(namespace) FROM "
            ?DIM_TABLE
            "     WHERE namespace > t.namespace"
            "     AND collection = $1)"
            "   FROM t WHERE t.namespace IS NOT NULL"
            "   )"
            "SELECT namespace FROM t WHERE namespace IS NOT NULL",
    Values = [Collection],
    {ok, Query, Values}.

namespaces_query(Collection, Metric)
  when is_binary(Collection),
       is_list(Metric) ->
    Q = "SELECT DISTINCT(namespace) FROM " ?DIM_TABLE " "
        "LEFT JOIN " ?MET_TABLE " "
        "ON " ?DIM_TABLE ".metric_id = " ?MET_TABLE ".id "
        "WHERE " ?MET_TABLE ".collection = $1 AND " ?MET_TABLE ".metric = $2",
    Vs = [Collection, Metric],
    {ok, Q, Vs}.

lookup_query({in, Collection, Metric}, Groupings) ->
    build_lookup_query(Collection, Metric, Groupings);
lookup_query({in, Bucket, Metric, Where}, Groupings) ->
    build_lookup_query(Bucket, Metric, Where, Groupings).

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
        "WHERE " ?MET_TABLE ".collection = $1 and metric = $2"
        "AND ",
    {_N, TagPairs, TagPredicate} = build_tag_lookup(Where, 3),
    Values = [Bucket, Metric | TagPairs],
    {ok, Query ++ TagPredicate, Values}.

glob_query(Bucket, Globs) ->
    Query = ["SELECT DISTINCT key ",
             "FROM ", ?MET_TABLE, " ",
             "WHERE bucket = $1 AND "],
    GlobWheres = [glob_where(Bucket, Query, Glob) || Glob <- Globs],
    {ok, GlobWheres}.

tags_query(Collection, Namespace)
  when is_binary(Collection),
       is_binary(Namespace) ->
    Q = "WITH RECURSIVE t AS ("
        "   SELECT MIN(name) AS name FROM " ?DIM_TABLE
        "     WHERE collection = $1"
        "     AND namespace = $2"
        "   UNION ALL"
        "   SELECT (SELECT MIN(name) FROM " ?DIM_TABLE
        "     WHERE name > t.name"
        "     AND collection = $1"
        "     AND namespace = $2)"
        "   FROM t WHERE t.name IS NOT NULL"
        "   )"
        "SELECT name FROM t WHERE name IS NOT NULL",
    Vs = [Collection, Namespace],
    {ok, Q, Vs}.

tags_query(Collection, Metric, Namespace)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Namespace) ->
    Q = "SELECT DISTINCT(name) FROM " ?DIM_TABLE " "
        "LEFT JOIN " ?MET_TABLE " "
        "ON " ?DIM_TABLE ".metric_id = " ?MET_TABLE ".id "
        "WHERE " ?MET_TABLE ".collection = $1 AND " ?MET_TABLE ".metric = $2 "
        "AND " ?DIM_TABLE ".namespace = $3",
    Vs = [Collection, Metric, Namespace],
    {ok, Q, Vs}.


add_tags(MID, Tags) ->
    Fn = "add_tag",
    build_add_tags(MID, 1, Fn, Tags, "SELECT", []).

update_tags(MID, Tags) ->
    Fn = "update_tag",
    build_add_tags(MID, 1, Fn, Tags, "SELECT", []).

values_query(Collection, Namespace, Tag)
  when is_binary(Collection),
       is_binary(Namespace),
       is_binary(Tag) ->
    Q = "WITH RECURSIVE t AS ("
        "   SELECT MIN(value) AS value FROM " ?DIM_TABLE
        "     WHERE collection = $1"
        "     AND namespace = $2"
        "     AND name = $3"
        "   UNION ALL"
        "   SELECT (SELECT MIN(value) FROM " ?DIM_TABLE
        "     WHERE value > t.value"
        "     AND collection = $1"
        "     AND namespace = $2"
        "     AND name = $3)"
        "   FROM t WHERE t.value IS NOT NULL"
        "   )"
        "SELECT value FROM t WHERE value IS NOT NULL",
    Vs = [Collection, Namespace, Tag],
    {ok, Q, Vs}.

values_query(Collection, Metric, Namespace, Tag)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Namespace),
       is_binary(Tag) ->
    Q = "SELECT DISTINCT(value) FROM " ?DIM_TABLE " "
        "LEFT JOIN " ?MET_TABLE " "
        "ON " ?DIM_TABLE ".metric_id = " ?MET_TABLE ".id "
        "WHERE " ?MET_TABLE ".collection = $1 AND " ?MET_TABLE ".metric = $2 "
        "AND " ?DIM_TABLE ".namespace = $3 AND name = $4",
    Vs = [Collection, Metric, Namespace, Tag],
    {ok, Q, Vs}.

get_id_query(Collection, Metric, Bucket, Key)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Bucket),
       is_list(Key) ->
    Q = "SELECT id FROM " ?MET_TABLE " WHERE "
        "collection = $1 AND "
        "metric = $2 AND "
        "bucket = $3 AND "
        "key = $4",
    Vs = [Collection, Metric, Bucket, Key],
    {ok, Q, Vs}.

%%====================================================================
%% Internal functions
%%====================================================================

build_lookup_query(Collection, Metric, Grouping)
  when is_list(Metric); Metric =:= undefined ->
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
  when is_list(Metric); Metric =:= undefined ->
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
        build_tag_lookup(Where, N + GroupingCount * 2),
    FlatGrouping = lists:flatten([[Namespace, Name] ||
                                     {Namespace, Name} <- Grouping]),
    Values = [Bucket | MetricName ++ FlatGrouping ++ TagPairs],
    {ok, Query ++ TagPredicate, Values}.

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

%% Build query part responsible for narowing down results to condition defeined
%% by where statement.

build_tag_lookup(Where, N) ->
    Conditions = collect_tag_conditions(Where),
    build_tag_flattened(Conditions, N, [], []).

%% Flatten nested conditions into 2 levels (a list of list).
%%
%% Give that intersection is associative operation:
%% A ∩ (B ∩ C) => (A ∩ B) ∩ C
%% ,we can just flatten all nested set operations and join all of them into 
%% intersection with more argumentas
collect_tag_conditions({'and', L, R}) ->
    LConds = collect_tag_conditions(L),
    RConds = collect_tag_conditions(R),
    LConds ++ RConds;
%% Based on set theory rule, we transform all unions with intersections to
%% intersections of widened sets.
%% A ∪ (B ∩ C) => (A ∪ B) ∩ (A ∪ B)
collect_tag_conditions({'or', L, R}) ->
    LConds = collect_tag_conditions(L),
    RConds = collect_tag_conditions(R),
    [LUnion ++ RUnion || LUnion <- LConds, RUnion <- RConds ];
collect_tag_conditions(Cond) ->
    [[Cond]].

build_tag_flattened([Group], N, Values, Query) ->
    build_tag_group(Group, N, Values, Query);
build_tag_flattened([Group | Rest], N, Values, Query) ->
    {N1, QPart, Values1} = build_tag_group(Group, N, Values, Query),
    Query1 = Query ++ [" AND " | QPart],
    build_tag_flattened(Rest, N1, Values1, Query1).

build_tag_group(Conditions, N, Values, Query) when length(Conditions) > 0 ->
    Query1 = Query ++ ["id IN (SELECT metric_id FROM " ?DIM_TABLE " WHERE "],
    {N1, Values1, Query2} = build_tag_conditions(Conditions, N, Values, Query1),
    Query3 = Query2 ++ [")"],
    {N1, Values1, Query3}.

build_tag_conditions([Condition], N, Vals, Query) ->
    {N1, Vals1, Str} = build_tag_condition(Condition, N, Vals),
    {N1, Vals1, Query ++ Str};
build_tag_conditions([Condition | Rest], N, Vals, Query) ->
    {N1, Vals1, Str} = build_tag_condition(Condition, N, Vals),
    build_tag_conditions(Rest, N1, Vals1, Query ++ [" OR " | Str]).

build_tag_condition({'=', {tag, NS, K}, V}, NIn, Vals) ->
    Str = ["(namespace = $", i2l(NIn),
           " AND name = $", i2l(NIn+1),
           " AND value = $", i2l(NIn+2)],
    {NIn+3, [NS, K, V | Vals], Str};
build_tag_condition({'!=', {tag, NS, K}, V}, NIn, Vals) ->
    Str = ["(namespace = $", i2l(NIn),
           " AND name = $", i2l(NIn+1),
           " AND value = $", i2l(NIn+2)],
    {NIn+3, [NS, K, V | Vals], Str}.

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

%% Example output:
%% AND metric[1:2] = '{base, cpu}'
metric_variant_where([], N) ->
    {N, [], ""};
metric_variant_where(Prefix, N) ->
    L = length(Prefix),
    Pred = ["AND metric[1:$", i2l(N), "] = $", i2l(N + 1), " "],
    Values = [L, Prefix],
    {N + 2, Values, Pred}.

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
