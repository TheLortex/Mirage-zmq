val int_to_network_order : int -> int -> bytes

val cstruct_to_bigstringaf : Cstruct.t -> Bigstringaf.t

val consume_parser :
  ?res:'a list ->
  'a Angstrom.Buffered.state ->
  'a Angstrom.t -> 'a list * 'a Angstrom.Buffered.state
