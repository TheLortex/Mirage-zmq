type identity_and_data = { identity : string; data : Message.t list }
type ('s, 'a) t

val create_socket :
  Context.t ->
  ?mechanism:Security_mechanism.mechanism_type ->
  ('a, 'b) Socket_type.t ->
  ('a, 'b) t
(** Create a socket from the given context, mechanism and type *)

val subscribe : (_, [> `Sub ]) t -> string -> unit
val unsubscribe : (_, [> `Sub ]) t -> string -> unit
val recv : (_, [> `Recv ]) t -> string Lwt.t
val recv_multipart : (_, [> `Recv ]) t -> Message.t list Lwt.t
val recv_from : (_, [> `Recv_from ]) t -> identity_and_data Lwt.t
val send : (_, [> `Send ]) t -> string -> unit Lwt.t
val send_multipart : (_, [> `Send ]) t -> Message.t list -> unit Lwt.t
val send_to : (_, [> `Send_to ]) t -> identity_and_data -> unit Lwt.t
val send_blocking : (_, [> `Send ]) t -> string -> unit Lwt.t

(* Connections bound to the socket. Returns when the connection is closed. *)
val add_connection : ('a, _) t -> 'a Raw_connection.t -> unit Lwt.t
val typ' : ('a, 'b) t -> ('a, 'b) Socket_type.t
val security_info : _ t -> Security_mechanism.security_data
val metadata : _ t -> Security_mechanism.socket_metadata

val set_plain_credentials : _ t -> string -> string -> unit
(** Set username and password for PLAIN client *)

val set_plain_user_list : _ t -> (string * string) list -> unit
(** Set password list for PLAIN server *)

val set_identity : _ t -> string -> unit
(** Set identity string of a socket if applicable *)

val set_incoming_queue_size : _ t -> int -> unit
(** Set the maximum capacity of the incoming queue *)

val set_outgoing_queue_size : _ t -> int -> unit
(** Set the maximum capacity of the outgoing queue *)