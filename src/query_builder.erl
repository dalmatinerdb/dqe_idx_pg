-module(query_builder).

-export([collections_query/0, metrics_query/1, metrics_query/3,
         namespaces_query/1, namespaces_query/2,
         tags_query/2, tags_query/3,
         values_query/3, values_query/4,
         lookup_query/4, lookup_tags_query/1,
         glob_query/2]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([interalize_and/1, simplify/1, optimize_logic/1]).
-endif.

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
        "SELECT metric FROM t WHERE metric IS NOT NULL",
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
        "       AND metric[1:$3] = $2"
        "       AND metric[1:$5] <> t.metric[1:$5]"
        "       AND collection = $1)"
        "   FROM t WHERE t.metric IS NOT NULL"
        "   )"
        "SELECT metric[$4:$5] FROM t WHERE metric[1:$3] = $2",
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
    {SubQ, SubV} = keys_subquery(Collection, Metric, 1),
    Query = ["SELECT DISTINCT substring(key from " ?NAMESPACE_PATTERN ")"
             "  FROM (", SubQ, ") AS data(key)"],
    {ok, Query, SubV}.

tags_query(Collection, Namespace)
  when is_binary(Collection),
       is_binary(Namespace) ->
    tags_query(Collection, undefined, Namespace).

tags_query(Collection, Metric, Namespace)
  when is_binary(Collection),
       is_list(Metric); Metric =:= undefined,
       is_binary(Namespace)  ->
    {SubQ, SubV} = keys_subquery(Collection, Metric, 2),
    Query = ["SELECT DISTINCT substring(key from " ?NAME_PATTERN ")"
             "  FROM (", SubQ, ") AS data(key)"
             "  WHERE key LIKE $1 ESCAPE '~'"],
    Ns1 = escape_sql_pattern(Namespace, <<>>),
    Ns2 = dqe_idx_pg_utils:encode_tag_key(Ns1, <<"%">>),
    Values = [Ns2 | SubV],
    {ok, Query, Values}.

values_query(Collection, Namespace, Tag)
  when is_binary(Collection),
       is_binary(Namespace),
       is_binary(Tag) ->
    Query = "SELECT DISTINCT dimensions -> $2 FROM " ?MET_TABLE
        "  WHERE collection = $1"
        "    AND dimensions ? $2",
    Tag1 = dqe_idx_pg_utils:encode_tag_key(Namespace, Tag),
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
    Tag1 = dqe_idx_pg_utils:encode_tag_key(Namespace, Tag),
    Values = [Collection, Metric, Tag1],
    {ok, Query, Values}.

lookup_query(Lookup, Start, Finish, []) ->
    {Condition, CVals} = lookup_condition(Lookup, 3),
    Query = ["SELECT bucket, key FROM metrics WHERE "
             "time_range && tsrange($1, $2) AND " | Condition],
    Values = [dqe_idx_pg_utils:ms_to_date(Start),
              dqe_idx_pg_utils:ms_to_date(Finish)
              | CVals],
    {ok, Query, Values};
lookup_query(Lookup, Start, Finish, KeysToRead) ->
    {Condition, CVals} = lookup_condition(Lookup, 4),
    Query = ["SELECT bucket, key, slice(dimensions, $1)"
             "  FROM metrics WHERE time_range && tsrange($2, $3) AND "
             | Condition],
    Keys = [dqe_idx_pg_utils:encode_tag_key(Ns, Name)
            || {Ns, Name} <- KeysToRead],
    Values = [Keys,
              dqe_idx_pg_utils:ms_to_date(Start),
              dqe_idx_pg_utils:ms_to_date(Finish)
              | CVals],
    {ok, Query, Values}.

lookup_tags_query(Lookup) ->
    {Condition, CVals} = lookup_condition(Lookup, 1),
    Query = ["SELECT (kv).key, (kv).value"
             "  FROM ("
             "    SELECT DISTINCT each(dimensions)"
             "      FROM metrics WHERE ", Condition,
             "  ) AS data(kv)"],
    {ok, Query, CVals}.

glob_query(Bucket, Globs) ->
    Criteria = criteria_from_globs(Globs),
    {Condition, CVals, _NextI} = criteria_condition(Criteria, 2),
    Query = ["SELECT DISTINCT key "
             "  FROM " ?MET_TABLE " "
             "  WHERE ", Condition,
             "    AND bucket = $1"],
    Values = [Bucket | CVals],
    {ok, Query, Values}.

%%====================================================================
%% Internal functions
%%====================================================================

keys_subquery(Collection, undefined, N) ->
    Condition = ["collection = $", i2l(N)],
    Values = [Collection],
    keys_subquery_with_condition(Condition, Values);
keys_subquery(Collection, Metric, N) ->
    Condition = ["collection = $", i2l(N), " AND metric = $", i2l(N + 1)],
    Values = [Collection, Metric],
    keys_subquery_with_condition(Condition, Values).

keys_subquery_with_condition(Condition, Values) ->
    Query = ["WITH RECURSIVE t AS ("
             "  SELECT MIN(akeys(dimensions)) AS keys FROM " ?MET_TABLE
             "    WHERE ", Condition,
             "  UNION"
             "  SELECT (SELECT MIN(akeys(dimensions)) AS keys FROM " ?MET_TABLE
             "    WHERE akeys(dimensions) > t.keys"
             "    AND ", Condition, ")"
             "  FROM t"
             "  )"
             "SELECT DISTINCT unnest(keys) FROM t"],
    {Query, Values}.

lookup_condition({in, Collection, undefined}, N) ->
    {["collection = $", i2l(N)], [Collection]};
lookup_condition({in, Collection, Metric}, N) ->
    {["collection = $", i2l(N),
      " AND metric = $", i2l(N + 1)],
     [Collection, Metric]};
lookup_condition({in, Collection, undefined, Where}, N) ->
    Criteria = lookup_criteria(Where),
    {Condition, CValues, _NextI} = criteria_condition(Criteria, N + 1),
    Query = ["collection = $", i2l(N), " AND " | Condition],
    Values = [Collection | CValues],
    {Query, Values};
lookup_condition({in, Collection, Metric, Where}, N) ->
    Criteria = lookup_criteria(Where),
    {Condition, CValues, _NextI} = criteria_condition(Criteria, N + 2),
    Query = ["collection = $", i2l(N), " AND metric = $", i2l(N + 1), " AND "
             | Condition],
    Values = [Collection, Metric | CValues],
    {Query, Values}.

lookup_criteria(C) ->
    %% We optimize the logic for hstore lookups before we build the lookup query
    lookup_criteria_(optimize_logic(C)).

%% Postgres hstore gist index is really quick to resolve intersection (AND)
%% between conditions based on one of operator: '@>' (containing exact values),
%% '?&' (containing all keys) and '?|' (containing any keys).
%%
%% Following function will convert dqe where clause to criteria operating on
%% those operators, doing some optimizations where possible.
%%
%% First lets start from converting simple operators to something we can perform
%% on hstores
lookup_criteria_({'=', Tag, Value}) ->
    {'@>', [{Tag, Value}]};
lookup_criteria_({'!=', Tag, Value}) ->
    {'not', {'@>', [{Tag, Value}]}};
%% containment checks connected by 'and' can be joined to one
lookup_criteria_({'and', L, R}) ->
    case {lookup_criteria_(L), lookup_criteria_(R)} of
        {{'@>', LKVs}, {'@>', RKVs}} ->
            {'@>', LKVs ++ RKVs};
       {L1, L2} ->
            {'and', L1, L2}
    end;
%% With 'or' operators we can do much, maybe beside trying to optimize children
lookup_criteria_({'or', L, R}) ->
    L1 = lookup_criteria_(L),
    R1 = lookup_criteria_(R),
    {'or', L1, R1}.
%% We can do further improvements using '?&' and '?!' operators once we add
%% support for tag presence only conditions.

%% special, optimized operators
criteria_condition({'@>', Parts}, I)  ->
    Cond = ["dimensions @> $", i2l(I)],
    HStore = {[{encode_tag(T), V} || {T, V} <- Parts]},
    {Cond, [HStore], I + 1};
criteria_condition({Op, Parts}, I)
  when Op =:= '?&'; Op =:= '?|' ->
    OpStr = case Op of
                '?&' -> "?&";
                '?|' -> "?|"
            end,
    Cond = ["dimensions ", OpStr, " $", i2l(I)],
    {Tags, _} = lists:unzip(Parts),
    Keys = lists:map(fun encode_tag/1, Tags),
    {Cond, [Keys], I + 1};
%% joining operators
criteria_condition({Op, L, R}, I)
  when Op =:= 'and'; Op =:= 'or' ->
    OpStr = case Op of
                'and' -> "AND";
                'or' -> "OR"
            end,
    {LCond, LVals, NextI} = criteria_condition(L, I),
    {RCond, RVals, NextI1} = criteria_condition(R, NextI),
    %% It seems that postgres will always evaluate ANDs before ORs independent
    %% of order, so we put brackets arroudn all logical operators to get right
    %% evaluation order.
    {["(", LCond, " ", OpStr, " ", RCond, ")"], LVals ++ RVals, NextI1};
criteria_condition({'not', Nested}, I) ->
    {Cond, Values, NextI} = criteria_condition(Nested, I),
    {["NOT " | Cond], Values, NextI}.

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
    dqe_idx_pg_utils:encode_tag_key(Ns, Name).

escape_sql_pattern(<<>>, Acc) ->
    Acc;
escape_sql_pattern(<<C:8/integer, Rest/binary>>, Acc)
  when C =:= $%; C =:= $~; C =:= $_->
    escape_sql_pattern(Rest, <<Acc/binary, $~, C:8/integer>>);
escape_sql_pattern(<<C:1/binary, Rest/binary>>, Acc) ->
    escape_sql_pattern(Rest, <<Acc/binary, C/binary>>).



extrac_values({'or', C1, C2}, Acc) ->
    extrac_values(C2, extrac_values(C1, Acc));
extrac_values({'and', C1, C2}, Acc) ->
    [{'and', simplify(C1), simplify(C2)} | Acc];
extrac_values({'not', C1}, Acc) ->
    [{'not', simplify(C1)} | Acc];
extrac_values(C, Acc) ->
    [C | Acc].

simplify(T) ->
    Flat = extrac_values(T, []),
    orify(Flat, []).

orify([C], []) ->
    C;
orify([C1, C2 | R], Acc) ->
    orify(R, [{'or', C1, C2} | Acc]);
orify([], Acc) ->
    orify(Acc, []);
orify([C], Acc) ->
    {'or', C, orify(Acc, [])}.

can_internaize({'not', _}) ->
    false;
can_internaize({'and', C1, C2}) ->
    can_internaize(C1) andalso can_internaize(C2);
can_internaize({'or', _, _}) ->
    false;
can_internaize(_) ->
    true.

interalize_and({'not', C1}) ->
    {'not', interalize_and(C1)};

interalize_and({'or', C1, C2}) ->
    {'or', interalize_and(C1), interalize_and(C2)};

interalize_and({'and', C1, C2}) ->
    I1 = interalize_and(C1),
    I2 = interalize_and(C2),
    case {can_internaize(I1), can_internaize(I2)} of
        {false, false} ->
            {'and', I1, I2};
        {true, true} ->
            {'and', I1, I2};
        {false, true} ->
            internalize_and(I2, I1);
        {true, false} ->
            internalize_and(I1, I2)
    end;
interalize_and(C) ->
    C.

internalize_and(And, {'or', {'or', C11, C12}, {'or', C21, C22}}) ->
    {'or',
     {'or',
      internalize_and(And, C11),
      internalize_and(And, C12)},
      {'or',
       internalize_and(And, C21),
       internalize_and(And, C22)}};
internalize_and(And, {'or', C1, {'or', C21, C22}}) ->
    {'or',
     {'and', And, C1},
     {'or',
      internalize_and(And, C21),
      internalize_and(And, C22)}};
internalize_and(And, {'or', {'or', C11, C12}, C2}) ->
    {'or',
     {'or',
      internalize_and(And, C11),
      internalize_and(And, C12)},
     {'and', And, C2}};

internalize_and(And, {'or', C1, C2}) ->
    {'or',
     {'and', And, C1},
     {'and', And, C2}};

internalize_and(And, C) ->
    {'and', And, C}.


optimize_logic(Logic) ->
    interalize_and(simplify(Logic)).


-ifdef(TEST).

o(A, B) ->
    {'or', A, B}.

orify_test() ->
    In = [c, c, c, c],
    Expected = o(o(c, c), o(c, c)),
    Out =orify(In, []),
    ?assertEqual(Expected, Out).

values4_test() ->
    In = {'or',
          {'or',
           {'or',
            {'or',
             c,
             c},
            c},
           c},
          c},
  Expected = [c, c, c, c, c],
  Out = extrac_values(In, []),
  ?assertEqual(Expected, Out).

balance_or3_test() ->
    In = {'or',
          {'or',
           {'or',
            c,
            c},
           c},
          c},
    Expected = {'or',
                 {'or' , c, c},
                 {'or',  c, c}},
    Out = simplify(In),
    ?assertEqual(Expected, Out).

balance_or4_test() ->
    In = {'or',
          {'or',
           {'or',
            {'or',
             c,
             c},
            c},
           c},
          c},
    Expected = {'or',
                c,
                {'or',
                 {'or', c, c},
                 {'or', c, c}}},
    Out = simplify(In),
    ?assertEqual(Expected, Out).


balance_or5_test() ->
    In = {'or',
          {'or',
           {'or',
            {'or',
             {'or', c, c},
             c},
            c},
           c},
          c},
    Expected = {'or',
                {'or', c, c},
                {'or',
                 {'or' , c, c},
                 {'or', c, c}}},
    Out = simplify(In),
    ?assertEqual(Expected, Out).


and_simplify_test() ->
    In = {'and',
          {'or',
           {'or',
            {'or',
             {'or',
              {'or', c, c},
              c},
             c},
            c},
           c},
          c1},
    Expected1 = {'and',
                {'or',
                 {'or', c, c},
                 {'or',
                  {'or' , c, c},
                  {'or', c, c}}},
                c1},
    Expected2 = {'or',
                 {'or', {'and', c1, c}, {'and', c1, c}},
                 {'or',
                  {'or', {'and', c1, c}, {'and', c1, c}},
                  {'or', {'and', c1, c}, {'and', c1, c}}}},
    Out1 = simplify(In),
    ?assertEqual(Expected1, Out1),
    Out2 = interalize_and(Expected1),
    ?assertEqual(Expected2, Out2) .

and_simplify2_test() ->
    In = {'and',
          {'or',
           {'or',
            {'or',
             {'or',
              {'or', c, c},
              c},
             c},
            c},
           c},
          {'or', c1, c1}},
    Expected = {'and',
                {'or',
                 {'or', c, c},
                 {'or',
                  {'or' , c, c},
                  {'or', c, c}}},
                {'or', c1, c1}},
    Out = simplify(In),
    ?assertEqual(Expected, Out).

and_simplify3_test() ->
    In = {'and',
          {'or',
           {'or',
            {'or',
             {'or',
              {'or', c, c},
              c},
             c},
            c},
           c},
          {'and', c1, c1}},
    Expected1 = {'and',
                 {'or',
                  {'or', c, c},
                  {'or',
                   {'or' , c, c},
                   {'or', c, c}}},
                 {'and', c1, c1}},
    Expected2 = {'or',
                 {'or',
                  {'and', {'and', c1, c1}, c},
                  {'and', {'and', c1, c1}, c}},
                 {'or',
                  {'or',
                   {'and', {'and', c1, c1}, c},
                   {'and', {'and', c1, c1}, c}},
                  {'or',
                   {'and', {'and', c1, c1}, c},
                   {'and', {'and', c1, c1}, c}}}},
    Out1 = simplify(In),
    ?assertEqual(Expected1, Out1),
    Out2 = interalize_and(Out1),
    ?assertEqual(Expected2, Out2).

-endif.
