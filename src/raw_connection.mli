type 'a t

val wait_until_ready : _ t -> bool Lwt.t
val is_ready : _ t -> bool

(* write frames to the network *)
val write : _ t -> Message.t list -> unit Lwt.t

(* read frames from the network*)
val read : _ t -> Message.t Pipe.or_closed Lwt.t

(* notify Eof from network *)
val close_input : _ t -> unit

(* notify Eof to network *)
val close_output : _ t -> unit

val init :
  ('a, _) Socket_type.t ->
  ?incoming_queue_size:int ->
  ?outgoing_queue_size:int ->
  Security_mechanism.t ->
  string ->
  'a t

val tag : _ t -> string
val incoming_identity : _ t -> string
val incoming_socket_type : _ t -> Socket_type.any

type action = Data of Cstruct.t | Close of string

val input : _ t -> action -> Cstruct.t list Pipe.or_closed
val output : _ t -> Cstruct.t list Pipe.or_closed Lwt.t
val is_send_queue_full : _ t -> bool
val is_closed : _ t -> bool
