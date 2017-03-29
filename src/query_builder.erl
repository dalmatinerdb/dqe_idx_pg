-module(query_builder).

-export([collections_query/0, metrics_query/1, metrics_query/3,
         namespaces_query/1, namespaces_query/2,
         tags_query/2, tags_query/3,
         values_query/3, values_query/4,
         lookup_query/2, lookup_tags_query/1,
         glob_query/2]).

-import(dqe_idx_pg_utils, [encode_tag_key/2]).

-include("dqe_idx_pg.hrl").

-define(NAMESPACE_PATTERN, "'^(([^:]|\\\\\\\\|\\\\:)*):'").
-define(NAME_PATTERN, "'^(?:[^:]|\\\\\\\\|\\\\:)*:(.*)$'").

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
            "   SELECT MIN(metric) AS metric FROM " ?MET_TABLE
            "     WHERE collection = $1"
            "   UNION ALL"
            "   SELECT (SELECT MIN(metric) FROM " ?MET_TABLE
            "     WHERE metric > t.metric"
            "     AND collection = $1)"
            "   FROM t WHERE t.metric IS NOT NULL"
            "   )"
            "SELECT metric FROM t WHERE t IS NOT NULL",
    Values = [Collection],
    {ok, Query, Values}.

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
    {SubQ, SubV} = keys_subquery(Collection, Metric),
    Query = "SELECT DISTINCT substring(key from " ?NAMESPACE_PATTERN ")"
            "  FROM (" ++ SubQ ++ ") AS data(key)",
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
             "  FROM (", SubQ, ") AS data(key)"
             "  WHERE key LIKE $" ++ i2l(I + 1) ++ " ESCAPE '~'"],
    Ns1 = escape_sql_pattern(Namespace, <<>>),
    Ns2 = encode_tag_key(Ns1, <<"%">>),
    Values = SubV ++ [Ns2],
    {ok, Query, Values}.

values_query(Collection, Namespace, Tag)
  when is_binary(Collection),
       is_binary(Namespace),
       is_binary(Tag) ->
    Query = "SELECT DISTINCT dimensions -> $2 FROM " ?MET_TABLE
            "  WHERE collection = $1"
            "    AND dimensions ? $2",
    Tag1 = encode_tag_key(Namespace, Tag),
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
    Tag1 = encode_tag_key(Namespace, Tag),
    Values = [Collection, Metric, Tag1],
    {ok, Query, Values}.

lookup_query(Lookup, []) ->
    {Condition, CVals} = lookup_condition(Lookup),
    Query = ["SELECT bucket, key FROM metrics WHERE " | Condition],
    {ok, Query, CVals};
lookup_query(Lookup, KeysToRead) ->
    {Condition, CVals} = lookup_condition(Lookup),
    I = length(CVals),
    Query = ["SELECT bucket, key, slice(dimensions, $", i2l(I + 1), ")"
             "  FROM metrics WHERE " | Condition],
    Keys = [encode_tag_key(Ns, Name) || {Ns, Name} <- KeysToRead],
    Values = CVals ++ [Keys],
    {ok, Query, Values}.

lookup_tags_query(Lookup) ->
    {Condition, CVals} = lookup_condition(Lookup),
    Query = ["SELECT (kv).key, (kv).value"
             "  FROM ("
             "    SELECT DISTINCT each(dimensions)"
             "      FROM metrics WHERE ", Condition,
             "  ) AS data(kv)"],
    {ok, Query, CVals}.

glob_query(Bucket, Globs) ->
    Criteria = criteria_from_globs(Globs),
    {Condition, CVals} = criteria_condition(Criteria, 0),
    I = length (CVals),
    Query = ["SELECT DISTINCT key "
             "  FROM " ?MET_TABLE " "
             "  WHERE ", Condition,
             "    AND bucket = $", i2l(I + 1)],
    Values = CVals ++ [Bucket],
    {ok, Query, Values}.

%%====================================================================
%% Internal functions
%%====================================================================

keys_subquery(Collection, undefined) ->
    Condition = "collection = $1",
    Values = [Collection],
    keys_subquery_with_condition(Condition, Values);
keys_subquery(Collection, Metric) ->
    Condition = "collection = $1 AND metric = $2",
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
    {"collection = $1 AND metric = $2",
     [Collection, Metric]};
lookup_condition({in, Collection, Metric, Where}) ->
    Criteria = lookup_criteria(Where),
    {Condition, CValues} = criteria_condition(Criteria, 2),
    Query = ["collection = $1 AND metric = $2 AND " | Condition],
    Values = [Collection, Metric | CValues],
    {Query, Values}.

%% Postgres hstore gist index is really quick to resolve intersection (AND)
%% between conditions based on one of operator: '@>' (containing exact values),
%% '?&' (containing all keys) and '?|' (containing any keys).
%%
%% Following function will convert dqe where clause to criteria operating on
%% those operators, doing some optimizations where possible.
%%
%% First lets start from converting simple operators to something we can perform
%% on hstores
lookup_criteria({'=', Tag, Value}) ->
    {'@>', [{Tag, Value}]};
lookup_criteria({'!=', Tag, Value}) ->
    {'not', {'@>', [{Tag, Value}]}};
%% containment checks connected by 'and' can be joined to one
lookup_criteria({'and', L, R}) ->
    case {lookup_criteria(L), lookup_criteria(R)} of
        {{'@>', LKVs}, {'@>', RKVs}} ->
            {'@>', LKVs ++ RKVs};
        {L1, L2} ->
            {'and', L1, L2}
    end;
%% With 'or' operators we can do much, maybe beside trying to optimize children
lookup_criteria({'or', L, R}) ->
    L1 = lookup_criteria(L),
    R1 = lookup_criteria(R),
    {'or', L1, R1}.
%% We can do further improvements using '?&' and '?!' operators once we add
%% support for tag presence only conditions.

%% special, optimized operators
criteria_condition({'@>', Parts}, I)  ->
    Cond = ["dimensions @> $", i2l(I+1)],
    HStore = {[{encode_tag(T), V} || {T, V} <- Parts]},
    {Cond, [HStore]};
criteria_condition({Op, Parts}, I)
  when Op =:= '?&'; Op =:= '?|' ->
    OpStr = case Op of
                '?&' -> "?&";
                '?|' -> "?|"
            end,
    Cond = ["dimensions ", OpStr, " $", i2l(I+1)],
    {Tags, _} = lists:unzip(Parts),
    Keys = lists:map(fun encode_tag/1, Tags),
    {Cond, [Keys]};
%% joining operators
criteria_condition({Op, L, R}, I)
  when Op =:= 'and'; Op =:= 'or' ->
    OpStr = case Op of
                'and' -> "AND";
                'or' -> "OR"
            end,
    {LCond, LVals} = criteria_condition(L, I),
    {RCond, RVals} = criteria_condition(R, I + length(LVals)),
    %% if right part has further nested joining operators, we need brackets
    RCond1 = case R of
                 {Op, _, _} when Op =:= 'and'; Op =:= 'or' ->
                     [$(, RCond, $)];
                 _ ->
                     RCond
             end,
    {[LCond, " ", OpStr, " ", RCond1], LVals ++ RVals};
criteria_condition({'not', Nested}, I) ->
    {Cond, Values} = criteria_condition(Nested, I),
    {["NOT " | Cond], Values}.

criteria_from_globs(Globs) ->
    criteria_from_globs(Globs, []).

criteria_from_globs([], Acc) ->
    Acc;
criteria_from_globs([Pattern | Rest], []) ->
    Crit = criteria_from_glob_pattern(Pattern),
    criteria_from_globs(Rest, Crit);
criteria_from_globs([Pattern | Rest], Acc) ->
    Crit = criteria_from_glob_pattern(Pattern),
    criteria_from_globs(Rest, {'or', Crit, Acc}).

criteria_from_glob_pattern(Pattern) ->
    criteria_from_glob_pattern(Pattern, 0, []).

criteria_from_glob_pattern([], Count, Acc) ->
    LenCrit = {{tag, <<"ddb">>, <<"key_length">>}, integer_to_binary(Count)},
    {'@>', [LenCrit | Acc]};
criteria_from_glob_pattern(['*' | Rest], Count, Acc) ->
    criteria_from_glob_pattern(Rest, Count + 1, Acc);
criteria_from_glob_pattern([Part | Rest], Count, Acc) ->
    CountBin = integer_to_binary(Count + 1),
    TName = <<"part_",  CountBin/binary>>,
    Acc1 = [{{tag, <<"ddb">>, TName}, Part} | Acc],
    criteria_from_glob_pattern(Rest, Count + 1, Acc1).

i2l(I) ->
    integer_to_list(I).

encode_tag({tag, Ns, Name}) ->
    encode_tag_key(Ns, Name).

escape_sql_pattern(<<>>, Acc) ->
    Acc;
escape_sql_pattern(<<C:8/integer, Rest/binary>>, Acc)
  when C =:= $%; C =:= $~; C =:= $_->
    escape_sql_pattern(Rest, <<Acc/binary, $~, C:8/integer>>);
escape_sql_pattern(<<C:1/binary, Rest/binary>>, Acc) ->
    escape_sql_pattern(Rest, <<Acc/binary, C/binary>>).

