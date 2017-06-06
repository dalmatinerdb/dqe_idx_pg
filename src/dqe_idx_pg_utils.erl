-module(dqe_idx_pg_utils).

-export([encode_tag_key/2,
         decode_ns/1,
         s_to_date/1,
         ms_to_date/1,
         tags_to_hstore/1,
         hstore_to_tags/1,
         kvpair_to_tag/1]).

-type non_empty_binary() :: <<_:8, _:_*8>>.

%% 62167219200 == calendar:datetime_to_gregorian_seconds(
%% {{1970, 1, 1}, {0, 0, 0}})
-define(S1970, 62167219200).

%%====================================================================
%% API
%%====================================================================

-spec encode_tag_key(binary(), binary()) ->
                            non_empty_binary().
encode_tag_key(Ns, Name)
  when is_binary(Ns),
       is_binary(Name) ->
    EncodedNs = encode_ns(Ns, <<>>),
    <<EncodedNs/binary, $:, Name/binary>>.

-spec decode_ns(binary()) ->
                       binary().
decode_ns(E) ->
    {Ns, <<>>} = decode_key(E, <<>>),
    Ns.

-spec tags_to_hstore([{binary(), binary(), binary()}]) ->
                            {[{binary(), binary()}]}.
tags_to_hstore(Tags) ->
    tags_to_hstore_iter(Tags, []).

-spec hstore_to_tags({[{binary(), binary()}]}) ->
                            [{binary(), binary(), binary()}].
hstore_to_tags({KVs}) when is_list(KVs) ->
    [kvpair_to_tag(KV) || KV <- KVs].

-spec kvpair_to_tag({binary(), binary()}) ->
                           {binary(), binary(), binary()}.
kvpair_to_tag({Key, Value}) ->
    {Ns, Name} = decode_key(Key, <<>>),
    {Ns, Name, Value}.

ms_to_date(MS) ->
    S = erlang:convert_time_unit(MS, milli_seconds, seconds),
    s_to_date(S).

s_to_date(S) ->
    calendar:gregorian_seconds_to_datetime(S + ?S1970).

%%====================================================================
%% Internal functions
%%====================================================================

encode_ns(<<>>, Acc) ->
    Acc;
encode_ns(<<$:, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<Acc/binary, "\\:">>);
encode_ns(<<$\\, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<Acc/binary, "\\\\">>);
encode_ns(<<Char:1/binary, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<Acc/binary, Char/binary>>).

tags_to_hstore_iter([], Acc) ->
    {Acc};
tags_to_hstore_iter([{Ns, Name, Val} | Rest], Acc) ->
    Key = encode_tag_key(Ns, Name),
    tags_to_hstore_iter(Rest, [{Key, Val} | Acc]).

decode_key(<<$:, Name/binary>>, Ns) ->
    {Ns, Name};
decode_key(<<>>, Ns) ->
    {Ns, <<>>};
decode_key(<<$\\, L:1/binary, Rest/binary>>, NsAcc) ->
    decode_key(Rest, <<NsAcc/binary, L/binary>>);
decode_key(<<L:1/binary, Rest/binary>>, NsAcc) ->
    decode_key(Rest, <<NsAcc/binary, L/binary>>).
