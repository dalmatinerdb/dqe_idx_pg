-module(dqe_idx_pg_utils).

-export([encode_tag_key/2,
         tags_to_hstore/1]).

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

-spec tags_to_hstore([{binary(), binary(), binary()}]) ->
                            {[{binary(), binary()}]}.
tags_to_hstore(Tags) ->
    tags_to_hstore_iter(Tags, []).

%%====================================================================
%% Internal functions
%%====================================================================

encode_ns(<<>>, Acc) ->
    Acc;
encode_ns(<<$:, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<Acc/binary, "\:">>);
encode_ns(<<$\\, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<Acc/binary, "\\">>);
encode_ns(<<Char:1/binary, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<Acc/binary, Char/binary>>).

tags_to_hstore_iter([], Acc) ->
    {Acc};
tags_to_hstore_iter([{Ns, Name, Val} | Rest], Acc) ->
    Key = encode_tag_key(Ns, Name),
    tags_to_hstore_iter(Rest, [{Key, Val} | Acc]).
