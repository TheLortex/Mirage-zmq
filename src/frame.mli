type t

val make : string -> more:bool -> command:bool -> t
(** make body ifMore ifCommand *)

val serialize : Faraday.t -> t -> unit
(** serialize *)

val parser : t Angstrom.t
(** parse *)

val is_more : t -> bool
(** Get if_more flag from a frame *)

val is_long : t -> bool
(** Get if_long flag from a frame *)

val is_command : t -> bool
(** Get if_command flag from a frame *)

val get_body : t -> string
(** Get body from a frame *)

val is_delimiter_frame : t -> bool
(** A helper function checking whether a frame is empty (delimiter) *)

val delimiter_frame : t
(** Make a delimiter frame*)
