type t

type action =
  | Write of bytes
  | Continue
  | Close
  | Received_property of string * string
  | Ok

type security_data =
  | Null
  | Plain_client of string * string
  | Plain_server of (string, string) Hashtbl.t

type mechanism_type = NULL | PLAIN

type socket_metadata = (string * string) list

val get_name_string : t -> string
(** Get the string description of the mechanism *)

val get_as_server : t -> bool
(** Whether the socket is a PLAIN server (always false if mechanism is NULL) *)

val get_as_client : t -> bool
(** Whether the socket is a PLAIN client (always false if mechanism is NULL) *)

val if_send_command_after_greeting : t -> bool

val init : security_data -> socket_metadata -> t
(** Initialise a t from security mechanism data and socket metadata *)
(*
    val client_first_message : t -> bytes
    (** If the socket is a PLAIN mechanism client, it needs to send the HELLO command first *)
*)

val first_command : t -> bytes

val fsm : t -> Command.t -> t * action list
(** FSM for handling the handshake *)
