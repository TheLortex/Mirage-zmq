val int_to_network_order : int -> int -> bytes

val cstruct_to_bigstringaf : Cstruct.t -> Bigstringaf.t

val consume_parser :
  ?res:'a list ->
  'a Angstrom.Buffered.state ->
  'a Angstrom.t -> 'a list * 'a Angstrom.Buffered.state

module Trie :
  sig
    type t
    val create : unit -> t
    val insert : t -> string -> unit
    val delete : t -> string -> unit
    val find : t -> string -> bool
    val is_empty : t -> bool
    val to_list : t -> string list
  end
