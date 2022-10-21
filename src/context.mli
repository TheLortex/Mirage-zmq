type t

val create_context : unit -> t
(** Create a new context *)

val set_default_queue_size : t -> int -> unit
(** Set the default queue size for this context *)

val get_default_queue_size : t -> int
(** Get the default queue size for this context *)