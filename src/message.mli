type t = { content : string; more : bool }

val make : string -> more:bool -> t

val to_frame : t -> Frame.t
(** Convert a message to a frame *)

val of_frame : Frame.t -> t
(** raises if it's a command *)

val is_delimiter : t -> bool
val delimiter : t
val merge : t list -> string