-module(dqe_idx_pg_utils).

-export([encode_tag_key/2,
         decode_ns/1,
         tags_to_hstore/1,
         hstore_to_tags/1,
         kvpair_to_tag/1]).

%%====================================================================
%% API
%%====================================================================

-spec encode_tag_key(binary(), binary()) ->
                            binary().
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
                            [{binary() | binary() | binary()}].
hstore_to_tags({KVs}) when is_list(KVs) ->
    [kvpair_to_tag(KV) || KV <- KVs].

-spec kvpair_to_tag({binary(), binary()}) ->
                           {binary(), binary(), binary()}.
kvpair_to_tag({Key, Value}) ->
    {Ns, Name} = decode_key(Key, <<>>),
    {Ns, Name, Value}.

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
