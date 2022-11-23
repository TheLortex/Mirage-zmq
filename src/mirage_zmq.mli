(*
 * Copyright 2018-2019 Huiyao Zheng <huiyaozheng@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)
exception No_Available_Peers

exception Incorrect_use_of_API of string
(** Raised when the function calls are invalid as defined by the RFCs, e.g. calling recv before send on a REQ socket. *)

exception Connection_closed
(** Raised when the connection that is the target of send/source of recv unexpectedly closes. Catch this exception to re-try the current operation on another connection if available. *)

type identity_and_data = { identity : string; data : string }

(** NULL and PLAIN security mechanisms are implemented in Mirage-zmq. *)
type mechanism_type = NULL | PLAIN

module Socket_type : sig
  type req
  type rep
  type dealer
  type router
  type pub
  type sub
  type xpub
  type xsub
  type push
  type pull
  type pair

  type ('s, 'p) t =
    | Rep : (rep, [ `Send | `Recv ]) t
    | Req : (req, [ `Send | `Recv ]) t
    | Dealer : (dealer, [ `Send | `Recv ]) t
    | Router : (router, [ `Send_to | `Recv_from ]) t
    | Pub : (pub, [ `Send ]) t
    | Sub : (sub, [ `Recv | `Sub ]) t
    | Xpub : (xpub, [ `Send | `Recv ]) t
    | Xsub : (xsub, [ `Send | `Recv | `Sub ]) t
    | Push : (push, [ `Send ]) t
    | Pull : (pull, [ `Recv ]) t
    | Pair : (pair, [ `Send | `Recv ]) t
end

(** A context contains a set of default options (queue size). New sockets created in a context inherits the default options. *)
module Context : sig
  type t

  val create_context : unit -> t
  (** Create a new context with default queue sizes. *)

  val set_default_queue_size : t -> int -> unit
  (** Set the default queue size for this context. The queue size is measured in the number of messages in the queue. *)

  val get_default_queue_size : t -> int
  (** Get the default queue size for this context. *)
end

(** Due to the characteristics of a unikernel, we need the network stack module to create TCP sockets *)
module Socket_tcp (S : Tcpip.Stack.V4V6) : sig
  type 'a t

  val create_socket :
    Context.t -> ?mechanism:mechanism_type -> (_, 'a) Socket_type.t -> 'a t
  (** Create a socket in the given context, mechanism and type *)

  val set_plain_credentials : _ t -> string -> string -> unit
  (** Set user name and password for a socket of PLAIN mechanism. Call this function before connect or bind. *)

  val set_plain_user_list : _ t -> (string * string) list -> unit
  (** Set the admissible password list for a socket of PLAIN mechanism. Call this function before connect or bind. *)

  val set_identity : _ t -> string -> unit
  (** Set the IDENTITY property of a socket. Call this function before connect or bind. *)

  val set_incoming_queue_size : _ t -> int -> unit
  (** Set the maximum capacity (size) of the incoming queue in terms of the number of messages. *)

  val set_outgoing_queue_size : _ t -> int -> unit
  (** Set the maximum capacity (size) of the outgoing queue in terms of the number of messages. *)

  val subscribe : [> `Sub ] t -> string -> unit
  (** Add a subscription topic to SUB/XSUB socket. *)

  val unsubscribe : [> `Sub ] t -> string -> unit
  (** Remove a subscription topic from SUB/XSUB socket *)

  val recv : [> `Recv ] t -> string Lwt.t
  (** Receive a message from the socket, according to the semantics of the socket type. The returned promise is not resolved until a message is available. *)

  val recv_from : [> `Recv_from ] t -> identity_and_data Lwt.t

  val send : [> `Send ] t -> string -> unit Lwt.t
  (** Send a message to the connected peer(s), according to the semantics of the socket type. The returned promise is not resolved until the message enters the outgoing queue(s). *)

  val send_to : [> `Send_to ] t -> identity_and_data -> unit Lwt.t

  val send_blocking : [> `Send ] t -> string -> unit Lwt.t
  (** Send a message to the connected peer(s). The returned promise is not resolved until the message has been sent by the TCP connection. *)

  val bind : _ t -> int -> S.t -> unit
  (** Bind the socket to a local TCP port, so the socket will accept incoming connections. *)

  type flow

  val connect : _ t -> string -> int -> S.t -> flow Lwt.t
  (** Connect the socket to a remote IP address and port. *)

  val disconnect : flow -> unit Lwt.t
end
