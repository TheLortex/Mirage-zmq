type t

val create : unit -> t
val insert : t -> string -> unit
val delete : t -> string -> unit
val find : t -> string -> bool
val is_empty : t -> bool
val to_list : t -> string list
