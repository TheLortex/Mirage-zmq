type t

(*
    val of_string : ?if_long:bool -> ?if_more:bool -> string -> t
    (** Make a message from the string *)
*)
val list_of_string : string -> t list
(** Make a list of messages from the string to send *)

val to_frame : t -> Frame.t
(** Convert a message to a frame *)