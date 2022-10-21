type t

type event =
  | Recv_sig of bytes
  | Recv_Vmajor of bytes
  | Recv_Vminor of bytes
  | Recv_Mechanism of bytes
  | Recv_as_server of bytes
  | Recv_filler
  | Init of string

type action =
  | Check_mechanism of string
  | Set_server of bool
  | Continue
  | Ok
  | Error of string

val init : Security_mechanism.t -> t
(** Initialise a t from a security mechanism to be used *)

val fsm_single : t -> event -> t * action
(** FSM call for handling a single event *)

val fsm : t -> event list -> t * action list
(** FSM call for handling a list of events. *)

val new_greeting : Security_mechanism.t -> Bytes.t
