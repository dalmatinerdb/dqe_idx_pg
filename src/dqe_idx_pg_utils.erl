-module(dqe_idx_pg_utils).

-export([encode_tag_key/2,
         tags_to_keys_and_values/1]).

%%====================================================================
%% API
%%====================================================================

-spec encode_tag_key(binary(), binary()) ->
                            binary().
encode_tag_key(Ns, Name)
  when is_binary(Ns),
       is_binary(Name) ->
    encode_ns(Ns, <<$:, Name/binary>>).

-spec tags_to_keys_and_values([{binary(), binary(), binary()}]) ->
                                     {[binary()], [binary()]}.
tags_to_keys_and_values(Tags) ->
    tags_to_keys_and_values(Tags, {[], []}).
    

%%====================================================================
%% Internal functions
%%====================================================================

encode_ns(<<>>, Acc) ->
    Acc;
encode_ns(<<$:, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<"\:", Acc/binary>>);
encode_ns(<<$\\, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<"\\", Acc/binary>>);
encode_ns(<<Char:1/binary, Rest/binary>>, Acc) ->
    encode_ns(Rest, <<Char/binary, Acc/binary>>).

tags_to_keys_and_values([], Acc) ->
    Acc;
tags_to_keys_and_values([{Ns, Name, Val} | Rest], {KeysAcc, ValsAcc}) ->
    Key = encode_tag_key(Ns, Name),
    tags_to_keys_and_values(Rest, {[Key | KeysAcc], [Val | ValsAcc]}).
