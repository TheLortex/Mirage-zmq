
type t

val make_frame : Bytes.t -> if_more:bool -> if_command:bool -> t
(** make_frame body ifMore ifCommand *)

val to_bytes : t -> Bytes.t
(** Convert a frame to raw bytes *)

val parser : t Angstrom.t

val get_if_more : t -> bool
(** Get if_more flag from a frame *)

val get_if_long : t -> bool
(** Get if_long flag from a frame *)

val get_if_command : t -> bool
(** Get if_command flag from a frame *)

val get_body : t -> Bytes.t
(** Get body from a frame *)

val is_delimiter_frame : t -> bool
(** A helper function checking whether a frame is empty (delimiter) *)

val delimiter_frame : t
(** Make a delimiter frame*)

val splice_message_frames : t list -> string
(** A helper function that takes a list of message frames and returns the reconstructed message *)
