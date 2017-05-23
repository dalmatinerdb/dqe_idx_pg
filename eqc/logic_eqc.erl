-module(logic_eqc).

-include_lib("eqc/include/eqc.hrl").

-compile(export_all).

op() ->
    oneof(['and', 'or']).

logic() ->
    ?SIZED(Size, logic(Size)).


eval_logic(true) ->
    true;
eval_logic(false) ->
    false;
eval_logic({'not', C}) ->
    not eval_logic(C);
eval_logic({'and', C1, C2}) ->
    eval_logic(C1) andalso eval_logic(C2);
eval_logic({'or', C1, C2}) ->
    eval_logic(C1) orelse eval_logic(C2).

logic(0) ->
    bool();
logic(Size) ->
    ?LAZY(
       oneof([
              ?LETSHRINK(
                 [L1], [logic(Size - 1)],
                 {'not', L1}),
              ?LETSHRINK(
                 [L1, L2], [logic(Size div 2), logic(Size div 2)],
                 {op(), L1, L2})
             ])).

prop_optimize() ->
    ?FORALL(
       L, logic(),
       begin
           S = query_builder:simplify(L),
           R = query_builder:interalize_and(S),
           eval_logic(L) =:= eval_logic(R)
       end).
