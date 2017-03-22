-module(query_builder).

-export([collections_query/0, metrics_query/1, metrics_query/3,
         namespaces_query/1, namespaces_query/2,
         tags_query/2, tags_query/3,
         values_query/3, values_query/4,
         lookup_query/2, lookup_tags_query/1,
         glob_query/2]).

%-ifdef(TEST).
%% -export([i2l/1]).
%-endif.

-include("dqe_idx_pg.hrl").

-define(NAMESPACE_PATTERN, "'^(([^:]|\\\\|\\:)+):'").
-define(NAME_PATTERN, "'^(?:[^:]|\\\\|\\:)+:(.*)$'").

%% TODO: Make sure namespace is encoded and decoded across the board

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
            "SELECT collection FROM t",
    Values = [],
    {ok, Query, Values}.

%% TODO: check if using just UNION instead of UNION ALL would not perform better
metrics_query(Collection)
  when is_binary(Collection) ->
    Query = "WITH RECURSIVE t AS ("
            "   SELECT MIN(metric) AS metric FROM " ?MET_TABLE
            "     WHERE collection = $1"
            "   UNION ALL"
            "   SELECT (SELECT MIN(metric) FROM " ?MET_TABLE
            "     WHERE metric > t.metric"
            "     AND collection = $1)"
            "   FROM t WHERE t.metric IS NOT NULL"
            "   )"
            "SELECT metric FROM t",
    Values = [Collection],
    {ok, Query, Values}.

%% TODO: for very short prefixes it may be easier to go old way with 
%%   SELECT DISTINCT metric[1] FROM ...
%% ,especially with partial indexes by collection.
%% Benchmark IT!
metrics_query(Collection, Prefix, Depth)
  when is_binary(Collection),
       is_list(Prefix),
       Depth > 0 ->
    Query = "WITH RECURSIVE t AS("
            "   SELECT MIN(metric) AS metric FROM " ?MET_TABLE
            "     WHERE collection = $1"
            "       AND metric > $2"
            "   UNION ALL"
            "   SELECT (SELECT MIN(metric) FROM " ?MET_TABLE
            "     WHERE metric > t.metric"
            "       AND metric[1:$5] <> t.metric[1:$4]"
            "       AND collection = $1)"
            "   FROM t WHERE t.metric[1:$3] = $2"
            "   )"
            "SELECT metric[$4:$5] FROM t",
    PrefLen = length(Prefix),
    From = PrefLen + 1,
    To = From + Depth - 1,
    Values = [Collection, Prefix, PrefLen, From, To],
    {ok, Query, Values}.

namespaces_query(Collection)
  when is_binary(Collection) ->
    namespaces_query(Collection, undefined).

namespaces_query(Collection, Metric)
  when is_binary(Collection),
       is_list(Metric); Metric =:= undefined ->
    %% TODO: I need to decode namespace results somehow
    {SubQ, SubV} = keys_subquery(Collection, Metric),
    Query = "SELECT DISTINCT substring(key from " ?NAMESPACE_PATTERN ")"
            "  FROM " ++ SubQ ++ " AS data(key)",
    {ok, Query, SubV}.

tags_query(Collection, Namespace)
  when is_binary(Collection),
       is_binary(Namespace) ->
    tags_query(Collection, undefined, Namespace).

tags_query(Collection, Metric, Namespace)
  when is_binary(Collection),
       is_list(Metric); Metric =:= undefined,
       is_binary(Namespace)  ->
    {SubQ, SubV} = keys_subquery(Collection, Metric),
    I = length(SubV),
    Query = ["SELECT DISTINCT substring(key from " ?NAME_PATTERN ")"
             "  FROM ", SubQ, " AS data(key)"
             "  WHERE key LIKE $" ++ i2l(I)],
    Ns = encode_tag(Namespace, <<"%">>),
    Values = SubV ++ [Ns],
    {ok, Query, Values}.

values_query(Collection, Namespace, Tag)
  when is_binary(Collection),
       is_binary(Namespace),
       is_binary(Tag) ->
    Query = "SELECT DISTINCT dimensions -> $2 FROM " ?MET_TABLE
            "  WHERE collection = $1"
            "    AND dimensions ? $2",
    Tag1 = encode_tag(Namespace, Tag),
    Values = [Collection, Tag1],
    {ok, Query, Values}.

values_query(Collection, Metric, Namespace, Tag)
  when is_binary(Collection),
       is_list(Metric),
       is_binary(Namespace),
       is_binary(Tag) ->
    Query = "SELECT DISTINCT dimensions -> $3 FROM " ?MET_TABLE
            "  WHERE collection = $1"
            "    AND metric = $2"
            "    AND dimensions ? $3",
    Tag1 = encode_tag(Namespace, Tag),
    Values = [Collection, Metric, Tag1],
    {ok, Query, Values}.

lookup_query(Lookup, ReadKeys) ->
    {Condition, CVals} = lookup_condition(Lookup),
    I = length(CVals),
    Query = ["SELECT buket, key, avals(slice(dimensions, ", i2l(I + 1), "))"
             "  FROM metrics WHERE " | Condition],
    Keys = [encode_tag(Ns, Name) || {Ns, Name} <- ReadKeys],
    Values = CVals ++ Keys,
    {ok, Query, Values}.
                                          
lookup_tags_query(Lookup) ->
    {Condition, CVals} = lookup_condition(Lookup),
    Query = ["SELECT substring((kv).key from " ?NAMESPACE_PATTERN "),"
             "       substring((kv).key from " ?NAME_PATTERN "),"
             "       (kv).value"
             "  FROM ("
             "    SELECT DISTINCT each(dimensions)"
             "      FROM metrics WHERE ", Condition,
             "  ) AS data(kv)"],
    {ok, Query, CVals}.

glob_query(Bucket, Globs) ->
    Query = ["SELECT DISTINCT key ",
             "FROM ", ?MET_TABLE, " ",
             "WHERE bucket = $1 AND "],
    GlobWheres = [glob_where(Bucket, Query, Glob) || Glob <- Globs],
    {ok, GlobWheres}.

%%====================================================================
%% Internal functions
%%====================================================================

keys_subquery(Collection, undefined) ->
    Condition = "collection = $1",
    Values = [Collection],
    keys_subquery_with_condition(Condition, Values);
keys_subquery(Collection, Metric) ->
    Condition = "collection = $1 AND metrics = $2",
    Values = [Collection, Metric],
    keys_subquery_with_condition(Condition, Values).

keys_subquery_with_condition(Condition, Values) ->
    Query = "WITH RECURSIVE t AS ("
            "  SELECT MIN(akeys(dimensions)) AS keys FROM " ?MET_TABLE
            "    WHERE " ++ Condition ++
            "  UNION"
            "  SELECT (SELECT MIN(akeys(dimensions)) AS keys FROM " ?MET_TABLE
            "    WHERE akeys(dimensions) > t.keys"
            "    AND " ++ Condition ++ ")"
            "  FROM t"
            "  )"
            "SELECT DISTINCT unnest(keys) FROM t",
    {Query, Values}.

lookup_condition({in, Collection, Metric}) ->
    lookup_condition({in, Collection, Metric, []});
lookup_condition({in, Collection, Metric, Where}) ->
    %% TODO: Implement me
    {}.

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

encode_tag(Ns, Name)
  when is_binary(Ns),
       is_binary(Name) ->
    encode_ns(Ns, <<$:, Name/binary>>).

encode_ns(<<>>, Acc) ->
    Acc;
encode_ns(<<$:, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<"\:", Acc/binary>>);
encode_ns(<<$\\, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<"\\", Acc/binary>>);
encode_ns(<<Char:1/binary, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<Char/binary, Acc/binary>>).

