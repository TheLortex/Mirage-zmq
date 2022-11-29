module type S = sig
  type 'a t

  val wait_until_ready : _ t -> bool Lwt.t
  val is_ready : _ t -> bool
  val write : _ t -> Frame.t list -> unit Lwt.t
  val read : _ t -> Frame.t Lwt.t
  val close : _ t -> unit Lwt.t
end

include S

val init :
  ('a, _) Socket_type.t ->
  ?incoming_queue_size:int ->
  ?outgoing_queue_size:int ->
  Security_mechanism.t ->
  string ->
  'a t

val tag : _ t -> string

type action = Data of Cstruct.t | Close of string

val input : _ t -> action -> action list
val output : _ t -> action list Lwt.t
val is_send_queue_full : _ t -> bool