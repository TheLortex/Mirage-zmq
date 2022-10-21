type t

val to_frame : t -> Frame.t
(** Convert a command to a frame *)

val get_name : t -> string
(** Get name of the command (without length byte) *)

val get_data : t -> bytes
(** Get data of the command *)

val of_frame : Frame.t -> t
(** Construct a command from the enclosing frame *)

val make_command : string -> bytes -> t
(** Construct a command from given name and data *)
