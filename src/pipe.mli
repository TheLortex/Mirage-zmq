type 'a t
type 'a or_closed = ('a, [ `Closed of string ]) result

val v : ?max_size:int -> unit -> 'a t

val push_or_drop : 'a t -> 'a -> unit
(** may lose data if queue is full *)

val push : 'a t -> 'a -> unit Lwt.t
(** block if full *)

val pop : 'a t -> 'a or_closed Lwt.t
(** block if nothing is available. raise if closed *)

val close : 'a t -> string -> unit
val is_full : 'a t -> bool