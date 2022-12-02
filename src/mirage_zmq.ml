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
open Lwt.Syntax

exception No_Available_Peers
exception Incorrect_use_of_API of string
exception Connection_closed

include Socket

(* CURVE not implemented *)
(* type connection_stage = GREETING | HANDSHAKE | TRAFFIC | CLOSED
   type connection_fsm_data = Input_data of Cstruct.t | End_of_connection
*)
module Context = Context
module Message = Message
include Security_mechanism

let src = Logs.Src.create "zmq" ~doc:"ZeroMQ"

module Log = (val Logs.src_log src : Logs.LOG)

module Socket_type = struct
  include Socket_type
  include Socket_type.T
end

(*
module rec Socket : sig
  type ('s, 'capabilities) t

  val get_socket_type : ('s, 'a) t -> ('s, 'a) Socket_type.t
  (** Get the type of the socket *)

  val get_metadata : _ t -> Security_mechanism.socket_metadata
  (** Get the metadata of the socket for handshake *)
  (*
     val get_mechanism : t -> mechanism_type
     (** Get the security mechanism of the socket *)
  *)

  val get_security_data : _ t -> Security_mechanism.security_data
  (** Get the security credentials of the socket *)

  val get_incoming_queue_size : _ t -> int option
  (** Get the maximum capacity of the incoming queue *)

  val get_outgoing_queue_size : _ t -> int option
  (** Get the maximum capacity of the outgoing queue *)

  val get_pair_connected : (pair, _) t -> bool
  (** Whether a PAIR is already connected to another PAIR *)

  val create_socket :
    Context.t ->
    ?mechanism:Security_mechanism.mechanism_type ->
    ('a, 'b) Socket_type.t ->
    ('a, 'b) t
  (** Create a socket from the given context, mechanism and type *)

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

  val set_pair_connected : (pair, _) t -> bool -> unit
  (** Set PAIR's connection status *)

  val subscribe : (_, [> `Sub ]) t -> string -> unit
  val unsubscribe : (_, [> `Sub ]) t -> string -> unit

  val recv : (_, [> `Recv ]) t -> string Lwt.t
  (** Receive a msg from the underlying connections, according to the semantics of the socket type *)

  val recv_from : (_, [> `Recv_from ]) t -> identity_and_data Lwt.t

  val send : (_, [> `Send ]) t -> string -> unit Lwt.t
  (** Send a msg to the underlying connections, according to the semantics of the socket type *)

  val send_to : (_, [> `Send_to ]) t -> identity_and_data -> unit Lwt.t

  (* val send_to : (_, [> `Send_to ]) t -> identity_and_data -> unit Lwt.t *)
  val send_blocking : (_, [> `Send ]) t -> string -> unit Lwt.t
  val add_connection : _ t -> Connection.t -> unit

  val initial_traffic_messages : _ t -> Frame.t list
  (** Get the messages to send at the beginning of a connection, e.g. subscriptions *)
end = struct
  type rep_state = {
    if_received : bool;
    last_received_connection_tag : string;
    address_envelope : Frame.t list;
  }

  type req_state = { if_sent : bool; last_sent_connection_tag : string }
  type dealer_state = { request_order_queue : string Queue.t }
  type sub_state = { subscriptions : Trie.t }
  type pair_state = { connected : bool }

  type ('s, 'p) socket_state =
    | SRep : rep_state -> (rep, [< `Send | `Recv ]) socket_state
    | SReq : req_state -> (req, [< `Send | `Recv ]) socket_state
    | SDealer : dealer_state -> (dealer, [< `Send | `Recv ]) socket_state
    | SRouter : (router, [< `Send_to | `Recv_from ]) socket_state
    | SPub : (pub, [< `Send ]) socket_state
    | SSub : sub_state -> (sub, [< `Recv | `Sub ]) socket_state
    | SXpub : (xpub, [< `Send | `Recv ]) socket_state
    | SXsub : sub_state -> (xsub, [< `Send | `Recv | `Sub ]) socket_state
    | SPush : (push, [< `Send ]) socket_state
    | SPull : (pull, [< `Recv ]) socket_state
    | SPair : pair_state -> (pair, [< `Send | `Recv ]) socket_state

  type (!'s, 'a) t = {
    mutable metadata : Security_mechanism.socket_metadata;
    security_mechanism : Security_mechanism.mechanism_type;
    mutable security_info : Security_mechanism.security_data;
    connections : Connection.t Queue.t;
    connections_condition : unit Lwt_condition.t;
    socket_type : ('s, 'a) Socket_type.t;
    mutable socket_states : ('s, 'a) socket_state;
    mutable incoming_queue_size : int option;
    mutable outgoing_queue_size : int option;
  }

  (* Start of helper functions *)

  (* If success, the connection at the front of the queue is available and returns it.
     Otherwise, return None *)
  let find_available_connection connections =
    if Queue.is_empty connections then None
    else
      let buffer_queue = Queue.create () in
      let rec rotate () =
        if Queue.is_empty connections then (
          Queue.transfer buffer_queue connections;
          None)
        else
          let head = Queue.peek connections in
          if
            Connection.get_stage head <> TRAFFIC
            || Connection.if_send_queue_full head
          then (
            Queue.push (Queue.pop connections) buffer_queue;
            rotate ())
          else (
            Queue.transfer buffer_queue connections;
            Some head)
      in
      rotate ()

  (** Put connection at the end of the list or remove the connection from the list *)
  let rotate connections if_drop_head =
    if if_drop_head then ignore (Queue.pop connections)
    else Queue.push (Queue.pop connections) connections

  (* connections is a queue of Connection.t.
     This functions returns the Connection.t with the compare function if found *)
  let find_connection connections comp =
    if not (Queue.is_empty connections) then
      let head = Queue.peek connections in
      if comp head then Some head
      else
        Queue.fold
          (fun accum connection ->
            match accum with
            | Some _ -> accum
            | None -> if comp connection then Some connection else accum)
          None connections
    else None

  (* Rotate a queue until the front connection has non-empty incoming buffer.
     Will rotate indefinitely until return, unless queue is empty at start*)
  let rec find_connection_with_incoming_buffer connections =
    if Queue.is_empty connections then Lwt.return None
    else
      let head = Queue.peek connections in
      if Connection.get_stage head = TRAFFIC then
        let buffer = Connection.get_read_buffer head in
        let* empty = Lwt_stream.is_empty buffer in
        if empty then (
          rotate connections false;
          let* () = Lwt.pause () in
          find_connection_with_incoming_buffer connections)
        else Lwt.return (Some head)
      else if Connection.get_stage head = CLOSED then (
        rotate connections true;
        find_connection_with_incoming_buffer connections)
      else (
        rotate connections false;
        let* () = Lwt.pause () in
        find_connection_with_incoming_buffer connections)

  (* Get the next list of frames containing a complete message/command from the read buffer.
     Return None if the stream is closed.
     Block if message not complete
  *)
  let get_frame_list connection =
    let i = ref 0 in
    let rec get_reverse_frame_list_accumu list =
      let* v = Lwt_stream.get (Connection.get_read_buffer connection) in
      incr i;
      match v with
      | None -> (* Stream closed *) Lwt.return_none
      | Some next_frame ->
          if Frame.is_more next_frame then
            get_reverse_frame_list_accumu (next_frame :: list)
          else Lwt.return (Some (next_frame :: list))
    in
    get_reverse_frame_list_accumu [] |> Lwt.map (Option.map List.rev)

  let wait_for_message (connections, connections_condition) =
    Lwt.choose
      (Lwt_condition.wait connections_condition
      :: (Queue.to_seq connections
         |> Seq.filter_map (fun connection ->
                if Connection.get_stage connection = TRAFFIC then
                  Some
                    (Lwt_stream.last_new (Connection.get_read_buffer connection)
                    |> Lwt.map ignore)
                else None)
         |> List.of_seq))

  (* receive from the first available connection in the queue and rotate the queue once after receving *)
  let receive_and_rotate (connections, connections_condition) =
    let rec loop () =
      Log.debug (fun f -> f "receive_and_rotate");
      if Queue.is_empty connections then
        let* () = wait_for_message (connections, connections_condition) in
        loop ()
      else
        let* conn = find_connection_with_incoming_buffer connections in
        match conn with
        | None ->
            let* () = wait_for_message (connections, connections_condition) in
            loop ()
        | Some connection -> (
            (* Reconstruct message from the connection *)
            let* frame_list = get_frame_list connection in
            match frame_list with
            | None ->
                Connection.close connection;
                rotate connections true;
                loop ()
            | Some frames ->
                rotate connections false;
                Lwt.return (Frame.splice_message_frames frames))
    in
    loop ()

  (* Get the address envelope from the read buffer, stop when the empty delimiter is encountered and discard the delimiter.connections
     Return None if stream closed
     Block if envelope not complete yet *)
  let get_address_envelope connection =
    let rec get_reverse_frame_list_accumu list =
      let* v = Lwt_stream.get (Connection.get_read_buffer connection) in
      match v with
      | None -> (* Stream closed *) Lwt.return_none
      | Some next_frame ->
          if Frame.is_delimiter_frame next_frame then Lwt.return (Some list)
          else if Frame.is_more next_frame then
            get_reverse_frame_list_accumu (next_frame :: list)
          else Lwt.return (Some (next_frame :: list))
    in
    get_reverse_frame_list_accumu [] |> Lwt.map (Option.map List.rev)

  (* Broadcast a message to all connections that satisfy predicate if_send_to in the queue *)
  let broadcast connections msg if_send_to =
    let frames_to_send =
      List.map (fun x -> Message.to_frame x) (Message.list_of_string msg)
    in
    let publish connection =
      if if_send_to connection then
        (* TODO: drop on overflow *)
        Connection.send connection frames_to_send
      else Lwt.return_unit
    in
    Queue.iter
      (fun x ->
        if Connection.get_stage x = TRAFFIC then Lwt.async (fun () -> publish x)
        else ())
      connections

  let subscription_frame content =
    Frame.make_frame
      (Bytes.cat (Bytes.make 1 (Char.chr 1)) (Bytes.of_string content))
      ~if_more:false ~if_command:false

  let unsubscription_frame content =
    Frame.make_frame
      (Bytes.cat (Bytes.make 1 (Char.chr 0)) (Bytes.of_string content))
      ~if_more:false ~if_command:false

  let send_message_to_all_active_connections connections frame =
    Queue.iter
      (fun x ->
        if Connection.get_stage x = TRAFFIC then
          Lwt.async (fun () -> Connection.send x [ frame ]))
      connections

  (* End of helper functions *)

  let get_socket_type t = t.socket_type
  let get_metadata t = t.metadata

  (*
     let get_mechanism t = t.security_mechanism
  *)
  let get_security_data t = t.security_info
  let get_incoming_queue_size t = t.incoming_queue_size
  let get_outgoing_queue_size t = t.outgoing_queue_size

  let get_pair_connected : type a. (pair, a) t -> bool =
   fun t ->
    match (t.socket_states : _ socket_state) with
    | SPair { connected } -> connected

  let create_socket context ?(mechanism = Security_mechanism.NULL) (type s r)
      (socket_type : (s, r) Socket_type.t) =
    match socket_type with
    | Rep ->
        ({
           socket_type;
           metadata = [ ("Socket-Type", "REP") ];
           security_mechanism = mechanism;
           security_info = Null;
           connections = Queue.create ();
           connections_condition = Lwt_condition.create ();
           socket_states =
             SRep
               {
                 if_received = false;
                 last_received_connection_tag = "";
                 address_envelope = [];
               };
           incoming_queue_size = None;
           outgoing_queue_size = None;
         }
          : (s, r) t)
    | Req ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "REQ") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states =
            SReq { if_sent = false; last_sent_connection_tag = "" };
          incoming_queue_size = None;
          outgoing_queue_size = None;
        }
    | Dealer ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "DEALER"); ("Identity", "") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = SDealer { request_order_queue = Queue.create () };
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | Router ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "ROUTER") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = SRouter;
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | Pub ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "PUB") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = SPub;
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | Xpub ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "XPUB") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = SXpub;
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | Sub ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "SUB") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = SSub { subscriptions = Trie.create () };
          incoming_queue_size =
            Some (Context.get_default_queue_size context)
            (* Need an outgoing queue to send subscriptions *);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | Xsub ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "XSUB") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = SXsub { subscriptions = Trie.create () };
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | Push ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "PUSH") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = SPush;
          incoming_queue_size = None;
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | Pull ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "PULL") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = SPull;
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = None;
        }
    | Pair ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "PAIR") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = SPair { connected = false };
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }

  let set_plain_credentials t name password =
    if t.security_mechanism = PLAIN then
      t.security_info <- Plain_client (name, password)
    else raise Not_Able_To_Set_Credentials

  let set_plain_user_list t list =
    if t.security_mechanism = PLAIN then (
      let hashtable = Hashtbl.create (List.length list) in
      List.iter
        (fun (username, password) -> Hashtbl.add hashtable username password)
        list;
      t.security_info <- Plain_server hashtable)
    else raise Not_Able_To_Set_Credentials

  let set_identity t identity =
    let set (name, value) =
      if name = "Identity" then (name, identity) else (name, value)
    in
    if
      List.fold_left
        (fun b (name, _) -> b || name = "Identity")
        false t.metadata
    then t.metadata <- List.map set t.metadata
    else t.metadata <- t.metadata @ [ ("Identity", identity) ]

  let set_incoming_queue_size t size = t.incoming_queue_size <- Some size
  let set_outgoing_queue_size t size = t.outgoing_queue_size <- Some size

  let set_pair_connected (type a) (t : (pair, a) t) status =
    match (t.socket_states : (pair, a) socket_state) with
    | SPair _ -> t.socket_states <- SPair { connected = status }

  type ('a, 'b) can_subscribe =
    | Yes_subscribe :
        ('a, [ `Sub ]) socket_state
        -> ('a, [> `Sub ]) can_subscribe
    | No_subscribe
        : ('a, [< `Send | `Recv | `Send_to | `Recv_from ]) can_subscribe

  let can_subscribe : type a b. (a, b) t -> (a, b) can_subscribe =
   fun t ->
    match t.socket_type with
    | Sub ->
        let (SSub s) = t.socket_states in
        Yes_subscribe (SSub s)
    | Xsub ->
        let (SXsub s) = t.socket_states in
        Yes_subscribe (SXsub s)
    | Req -> No_subscribe
    | Rep -> No_subscribe
    | Push -> No_subscribe
    | Pull -> No_subscribe
    | Dealer -> No_subscribe
    | Router -> No_subscribe
    | Pub -> No_subscribe
    | Xpub -> No_subscribe
    | Pair -> No_subscribe

  let subscribe (type a) (t : (a, [> `Sub ]) t) subscription =
    let (Yes_subscribe socket_state) = can_subscribe t in
    match socket_state with
    | SSub { subscriptions } ->
        Trie.insert subscriptions subscription;
        send_message_to_all_active_connections t.connections
          (subscription_frame subscription)
    | SXsub { subscriptions } ->
        Trie.insert subscriptions subscription;
        send_message_to_all_active_connections t.connections
          (subscription_frame subscription)

  let unsubscribe (type a) (t : (a, [> `Sub ]) t) subscription =
    let (Yes_subscribe socket_state) = can_subscribe t in
    match socket_state with
    | SSub { subscriptions } ->
        Trie.delete subscriptions subscription;
        send_message_to_all_active_connections t.connections
          (unsubscription_frame subscription)
    | SXsub { subscriptions } ->
        Trie.delete subscriptions subscription;
        send_message_to_all_active_connections t.connections
          (unsubscription_frame subscription)

  type ('a, 'b) can_recv =
    | Yes_recv :
        ('a, [ `Recv ]) socket_state * (('a, [< `Recv ]) socket_state -> unit)
        -> ('a, [> `Recv ]) can_recv
    | No_recv : ('a, [< `Send | `Sub | `Recv_from | `Send_to ]) can_recv

  let can_recv : type a b. (a, b) t -> (a, b) can_recv =
   fun t ->
    let magic x = t.socket_states <- Obj.magic x in
    match t.socket_type with
    | Sub ->
        let (SSub s) = t.socket_states in
        Yes_recv (SSub s, magic)
    | Xsub ->
        let (SXsub s) = t.socket_states in
        Yes_recv (SXsub s, magic)
    | Req ->
        let (SReq s) = t.socket_states in
        Yes_recv (SReq s, magic)
    | Rep ->
        let (SRep s) = t.socket_states in
        Yes_recv (SRep s, magic)
    | Pull -> Yes_recv (SPull, magic)
    | Dealer ->
        let (SDealer s) = t.socket_states in
        Yes_recv (SDealer s, magic)
    | Xpub -> Yes_recv (SXpub, magic)
    | Pair ->
        let (SPair s) = t.socket_states in
        Yes_recv (SPair s, magic)
    | Router -> No_recv
    | Push -> No_recv
    | Pub -> No_recv

  (*
     type queue_fold_type = UNINITIALISED | Result of Connection.t
  *)
  let rec recv : type a. (a, [> `Recv ]) t -> string Lwt.t =
   fun t ->
    let (Yes_recv (socket_state, socket_state_update)) = can_recv t in
    match socket_state with
    | SRep _ -> (
        if Queue.is_empty t.connections then
          let* () = Lwt.pause () in
          recv t
        else
          (* Go through the queue of connections and check buffer *)
          let* conn = find_connection_with_incoming_buffer t.connections in
          match conn with
          | None ->
              let* () = Lwt.pause () in
              recv t
          | Some connection -> (
              (* Reconstruct message from the connection *)
              let* addr_envelope = get_address_envelope connection in
              match addr_envelope with
              | None ->
                  Connection.close connection;
                  rotate t.connections true;
                  let* () = Lwt.pause () in
                  recv t
              | Some address_envelope -> (
                  let* frame_list = get_frame_list connection in
                  match frame_list with
                  | None ->
                      Connection.close connection;
                      rotate t.connections true;
                      let* () = Lwt.pause () in
                      recv t
                  | Some frames ->
                      socket_state_update
                        (SRep
                           {
                             if_received = true;
                             last_received_connection_tag =
                               Connection.get_tag connection;
                             address_envelope;
                           });
                      Lwt.return (Frame.splice_message_frames frames))))
    | SReq { if_sent; last_sent_connection_tag = tag } -> (
        if not if_sent then
          raise (Incorrect_use_of_API "Need to send a request before receiving")
        else
          let find_and_send connections =
            let head = Queue.peek connections in
            if tag = Connection.get_tag head then
              if Connection.get_stage head = TRAFFIC then (
                let* frame_list = get_frame_list head in
                match frame_list with
                | None -> Lwt.return None
                | Some frames ->
                    socket_state_update
                      (SReq { if_sent = false; last_sent_connection_tag = "" });
                    Lwt.return (Some (Frame.splice_message_frames frames)))
              else (
                rotate t.connections true;
                socket_state_update
                  (SReq { if_sent = false; last_sent_connection_tag = "" });
                raise Connection_closed)
            else
              raise (Internal_Error "Receive target no longer at head of queue")
          in
          let* result = find_and_send t.connections in
          match result with
          | Some result ->
              rotate t.connections false;
              Lwt.return result
          | None ->
              rotate t.connections true;
              socket_state_update
                (SReq { if_sent = false; last_sent_connection_tag = "" });
              raise Connection_closed)
    | SDealer { request_order_queue } -> (
        if Queue.is_empty request_order_queue then
          raise (Incorrect_use_of_API "You need to send requests first!")
        else
          let tag = Queue.peek request_order_queue in
          find_connection t.connections (fun connection ->
              Connection.get_tag connection = tag)
          |> function
          | None ->
              let* () = Lwt.pause () in
              recv t
          | Some connection -> (
              (* Reconstruct message from the connection *)
              let* frame_list = get_frame_list connection in
              match frame_list with
              | None ->
                  Connection.close connection;
                  ignore (Queue.pop request_order_queue);
                  raise Connection_closed
              | Some frames ->
                  (* Put the received connection at the end of the queue *)
                  ignore (Queue.pop request_order_queue);
                  Lwt.return (Frame.splice_message_frames frames)))
    | SSub _ -> receive_and_rotate (t.connections, t.connections_condition)
    | SXpub -> receive_and_rotate (t.connections, t.connections_condition)
    | SXsub _ -> receive_and_rotate (t.connections, t.connections_condition)
    | SPull -> receive_and_rotate (t.connections, t.connections_condition)
    | SPair { connected } ->
        if Queue.is_empty t.connections then raise No_Available_Peers
        else
          let connection = Queue.peek t.connections in
          if connected && Connection.get_stage connection = TRAFFIC then
            let* frames = get_frame_list connection in
            match frames with
            | None ->
                Connection.close connection;
                socket_state_update (SPair { connected = false });
                rotate t.connections true;
                raise Connection_closed
            | Some frames -> Lwt.return (Frame.splice_message_frames frames)
          else raise No_Available_Peers

  type ('a, 'b) can_recv_from =
    | Yes_recv_from :
        ('a, [ `Recv_from ]) socket_state
        -> ('a, [> `Recv_from ]) can_recv_from
    | No_recv_from : ('a, [< `Send | `Sub | `Recv | `Send_to ]) can_recv_from

  let can_recv_from : type a b. (a, b) t -> (a, b) can_recv_from =
   fun t ->
    match t.socket_type with
    | Router -> Yes_recv_from SRouter
    | Push -> No_recv_from
    | Pub -> No_recv_from
    | Pair -> No_recv_from
    | Xpub -> No_recv_from
    | Sub -> No_recv_from
    | Xsub -> No_recv_from
    | Req -> No_recv_from
    | Rep -> No_recv_from
    | Pull -> No_recv_from
    | Dealer -> No_recv_from

  let rec recv_from : type a. (a, [> `Recv_from ]) t -> identity_and_data Lwt.t
      =
   fun t ->
    let (Yes_recv_from SRouter) = can_recv_from t in
    if Queue.is_empty t.connections then
      let* () = Lwt.pause () in
      recv_from t
    else
      let* connection = find_connection_with_incoming_buffer t.connections in
      match connection with
      | None ->
          let* () = Lwt.pause () in
          recv_from t
      | Some connection -> (
          (* Reconstruct message from the connection *)
          let* frames = get_frame_list connection in
          match frames with
          | None ->
              Connection.close connection;
              rotate t.connections true;
              let* () = Lwt.pause () in
              recv_from t
          | Some frames ->
              (* Put the received connection at the end of the queue *)
              rotate t.connections false;
              Lwt.return
                {
                  identity = Connection.get_identity connection;
                  data = Frame.splice_message_frames frames;
                })

  type ('a, 'b) can_send =
    | Yes_send :
        (('a, [ `Send ]) socket_state * (('a, [< `Recv ]) socket_state -> unit))
        -> ('a, [> `Send ]) can_send
    | No_send : ('a, [< `Recv | `Sub | `Recv_from | `Send_to ]) can_send

  let can_send : type a b. (a, b) t -> (a, b) can_send =
   fun t ->
    let magic x = t.socket_states <- Obj.magic x in
    match t.socket_type with
    | Xsub ->
        let (SXsub s) = t.socket_states in
        Yes_send (SXsub s, magic)
    | Req ->
        let (SReq s) = t.socket_states in
        Yes_send (SReq s, magic)
    | Rep ->
        let (SRep s) = t.socket_states in
        Yes_send (SRep s, magic)
    | Dealer ->
        let (SDealer s) = t.socket_states in
        Yes_send (SDealer s, magic)
    | Xpub -> Yes_send (SXpub, magic)
    | Pair ->
        let (SPair s) = t.socket_states in
        Yes_send (SPair s, magic)
    | Push -> Yes_send (SPush, magic)
    | Pub -> Yes_send (SPub, magic)
    | Sub -> No_send
    | Pull -> No_send
    | Router -> No_send

  let send : type a. (a, [> `Send ]) t -> string -> unit Lwt.t =
   fun t msg ->
    let frames =
      List.map (fun x -> Message.to_frame x) (Message.list_of_string msg)
    in
    let (Yes_send (socket_state, socket_state_update)) = can_send t in
    let try_send () =
      match socket_state with
      | SRep
          { if_received; last_received_connection_tag = tag; address_envelope }
        ->
          if not if_received then
            raise
              (Incorrect_use_of_API
                 "Need to receive a request before sending a message")
          else
            let find_and_send connections =
              let head = Queue.peek connections in
              if tag = Connection.get_tag head then
                if Connection.get_stage head = TRAFFIC then (
                  let* () =
                    Connection.send head
                      (address_envelope @ (Frame.delimiter_frame :: frames))
                  in
                  socket_state_update
                    (SRep
                       {
                         if_received = false;
                         last_received_connection_tag = "";
                         address_envelope = [];
                       });
                  rotate t.connections false;
                  Lwt.return_unit)
                else Lwt.return_unit
              else
                raise (Internal_Error "Send target no longer at head of queue")
            in
            find_and_send t.connections
      | SReq { if_sent; last_sent_connection_tag = _ } -> (
          if if_sent then
            raise
              (Incorrect_use_of_API
                 "Need to receive a reply before sending another message")
          else
            find_available_connection t.connections |> function
            | None -> raise No_Available_Peers
            | Some connection ->
                (* TODO check re-send is working *)
                Lwt_stream.get_available (Connection.get_read_buffer connection)
                |> ignore;
                let* () =
                  Connection.send connection (Frame.delimiter_frame :: frames)
                in
                socket_state_update
                  (SReq
                     {
                       if_sent = true;
                       last_sent_connection_tag = Connection.get_tag connection;
                     });
                Lwt.return_unit)
      | SDealer { request_order_queue } -> (
          if
            (* TODO investigate DEALER dropping messages *)
            Queue.is_empty t.connections
          then raise No_Available_Peers
          else
            find_available_connection t.connections |> function
            | None -> raise No_Available_Peers
            | Some connection ->
                let* () =
                  Connection.send connection ~wait_until_sent:true
                    (Frame.delimiter_frame :: frames)
                in
                Queue.push (Connection.get_tag connection) request_order_queue;
                Log.debug (fun f -> f "Message sent");
                rotate t.connections false;
                Lwt.return_unit)
      (* | SRouter -> (
          match msg with
          | Identity_and_data (id, _msg) -> (
              find_connection t.connections (fun connection ->
                  Connection.get_identity connection = id)
              |> function
              | None -> Lwt.return_unit
              | Some connection ->
                  let frame_list =
                    if Connection.get_incoming_socket_type connection == U Req
                    then Frame.delimiter_frame :: frames
                    else frames
                  in
                  Connection.send connection frame_list
              (* TODO: Drop message when queue full *))
          | _ ->
              raise
                (Incorrect_use_of_API
                   "Sending a message via ROUTER needs a specified receiver \
                    identity!")) *)
      | SPub ->
          broadcast t.connections msg (fun connection ->
              Trie.find (Connection.get_subscriptions connection) msg);
          Lwt.return_unit
      | SXpub ->
          broadcast t.connections msg (fun connection ->
              Trie.find (Connection.get_subscriptions connection) msg);
          Lwt.return_unit
      | SXsub _ ->
          broadcast t.connections msg (fun _ -> true);
          Lwt.return_unit
      | SPush -> (
          if Queue.is_empty t.connections then raise No_Available_Peers
          else
            find_available_connection t.connections |> function
            | None -> raise No_Available_Peers
            | Some connection ->
                let+ () = Connection.send connection frames in
                rotate t.connections false)
      | SPair { connected } ->
          if Queue.is_empty t.connections then raise No_Available_Peers
          else
            let connection = Queue.peek t.connections in
            if
              connected
              && Connection.get_stage connection = TRAFFIC
              && not (Connection.if_send_queue_full connection)
            then Connection.send connection frames
            else raise No_Available_Peers
    in
    try_send ()

  type ('a, 'b) can_send_to =
    | Yes_send_to :
        ('a, [ `Send_to ]) socket_state
        -> ('a, [> `Send_to ]) can_send_to
    | No_send_to : ('a, [< `Send | `Sub | `Recv | `Recv_from ]) can_send_to

  let can_send_to : type a b. (a, b) t -> (a, b) can_send_to =
   fun t ->
    match t.socket_type with
    | Router -> Yes_send_to SRouter
    | Push -> No_send_to
    | Pub -> No_send_to
    | Pair -> No_send_to
    | Xpub -> No_send_to
    | Sub -> No_send_to
    | Xsub -> No_send_to
    | Req -> No_send_to
    | Rep -> No_send_to
    | Pull -> No_send_to
    | Dealer -> No_send_to

  let send_to : type a. (a, [> `Send_to ]) t -> identity_and_data -> unit Lwt.t
      =
   fun t { identity; data } ->
    let frames =
      List.map (fun x -> Message.to_frame x) (Message.list_of_string data)
    in
    let try_send () =
      let (Yes_send_to SRouter) = can_send_to t in
      find_connection t.connections (fun connection ->
          Connection.get_identity connection = identity)
      |> function
      | None -> Lwt.return_unit
      | Some connection ->
          let frame_list =
            if Connection.get_incoming_socket_type connection == U Req then
              Frame.delimiter_frame :: frames
            else frames
          in
          Connection.send connection frame_list
      (* TODO: Drop message when queue full *)
    in
    try_send ()

  let rec send_blocking t msg =
    try send t msg
    with No_Available_Peers ->
      Log.debug (fun f -> f "send_blocking: no available peers");
      let* () = Lwt.pause () in
      send_blocking t msg

  let add_connection t connection =
    Queue.push connection t.connections;
    Lwt_condition.broadcast t.connections_condition ()

  let initial_traffic_messages (type a b) (t : (a, b) t) =
    match t.socket_states with
    | SSub { subscriptions } ->
        if not (Trie.is_empty subscriptions) then
          List.map (fun x -> subscription_frame x) (Trie.to_list subscriptions)
        else []
    | SXsub { subscriptions } ->
        if not (Trie.is_empty subscriptions) then
          List.map (fun x -> subscription_frame x) (Trie.to_list subscriptions)
        else []
    | _ -> []
end
 *)
(*
and Connection : sig
  type t
  type action = Write of bytes | Close of string

  val init : _ Socket.t -> Security_mechanism.t -> string -> t
  (** Create a new connection for socket with specified security mechanism *)

  val get_tag : t -> string
  (** Get the unique tag used to identify the connection *)

  val get_read_buffer : t -> Frame.t Lwt_stream.t
  (** Get the read buffer of the connection *)

  val write_read_buffer : t -> Frame.t option -> unit Lwt.t
  (** Write to the read buffer of the connection *)

  val get_write_buffer : t -> Bytes.t Lwt_stream.t
  (** Get the output buffer of the connection *)

  val get_stage : t -> connection_stage
  (** Get the stage of the connection. It is considered usable if in TRAFFIC *)

  type any_socket = U : _ Socket.t -> any_socket

  val get_socket : t -> any_socket
  val get_identity : t -> string
  val get_subscriptions : t -> Trie.t
  val get_incoming_socket_type : t -> Socket_type.any
  val greeting_message : t -> Bytes.t

  val fsm : t -> connection_fsm_data -> action list Lwt.t
  (** FSM for handing raw bytes transmission *)

  val send : t -> ?wait_until_sent:bool -> Frame.t list -> unit Lwt.t
  (** Send the list of frames to underlying connection *)

  val close : t -> unit
  (** Force close connection *)

  val if_send_queue_full : t -> bool
  (** Returns whether the send queue is full (always false if unbounded size *)
end = struct
  type action = Write of bytes | Close of string
  type subscription_message = Subscribe | Unsubscribe | Ignore

  type 'a buffer_stream = {
    stream : 'a Lwt_stream.t;
    push : 'a -> unit Lwt.t;
    close : unit -> unit;
    bounded_push : 'a Lwt_stream.bounded_push option;
  }

  let bounded_buffer_stream bound =
    let stream, bounded_push = Lwt_stream.create_bounded bound in
    {
      stream;
      push = (fun v -> bounded_push#push v);
      close = (fun () -> bounded_push#close);
      bounded_push = Some bounded_push;
    }

  let unbounded_buffer_stream () =
    let stream, push = Lwt_stream.create () in
    {
      stream;
      push =
        (fun v ->
          push (Some v);
          Lwt.return_unit);
      close = (fun () -> push None);
      bounded_push = None;
    }

  type any_socket = U : _ Socket.t -> any_socket

  type t = {
    tag : string;
    socket : any_socket;
    mutable greeting_state : Greeting.t;
    mutable handshake_state : Security_mechanism.t;
    mutable stage : connection_stage;
    mutable expected_bytes_length : int;
    mutable incoming_as_server : bool;
    mutable incoming_socket_type : Socket_type.any;
    mutable incoming_identity : string;
    read_buffer : Frame.t Pipe.t option;
    send_buffer : Bytes.t Pipe.t option;
    mutable subscriptions : Trie.t;
    mutable fragments_parser : Frame.t Angstrom.Buffered.state;
  }

  let init socket security_mechanism tag =
    let read_buffer =
      match Socket.get_incoming_queue_size socket with
      | None -> unbounded_buffer_stream ()
      | Some v -> bounded_buffer_stream v
    in
    let send_buffer =
      match Socket.get_outgoing_queue_size socket with
      | None -> unbounded_buffer_stream ()
      | Some v -> bounded_buffer_stream v
    in

    {
      tag;
      socket = U socket;
      greeting_state = Greeting.init security_mechanism;
      handshake_state = security_mechanism;
      stage = GREETING;
      expected_bytes_length = 64;
      (* A value of 0 means expecting a frame of any length; starting with expectint the whole greeting *)
      incoming_socket_type = U Socket_type.Rep;
      incoming_as_server = false;
      incoming_identity = tag;
      read_buffer;
      send_buffer;
      subscriptions = Trie.create ();
      fragments_parser = Angstrom.Buffered.parse Frame.parser;
    }

  let get_tag t = t.tag
  let get_stage t = t.stage
  let get_read_buffer t = t.read_buffer.stream

  let write_read_buffer t = function
    | Some v -> t.read_buffer.push v
    | None ->
        t.read_buffer.close ();
        Lwt.return_unit

  let get_write_buffer t = t.send_buffer.stream

  let write_write_buffer t = function
    | Some v -> t.send_buffer.push v
    | None ->
        t.send_buffer.close ();
        Lwt.return_unit

  let get_socket t = t.socket
  let get_identity t = t.incoming_identity
  let get_subscriptions t = t.subscriptions
  let get_incoming_socket_type t = t.incoming_socket_type
  let greeting_message t = Greeting.new_greeting t.handshake_state

  let rec fsm t data =
    match data with
    | End_of_connection ->
        let+ () = write_read_buffer t None in
        []
    | Input_data bytes -> (
        match t.stage with
        | GREETING ->
            let if_pair_already_connected =
              let (U s) = t.socket in
              match Socket.get_socket_type s with
              | Pair -> Socket.get_pair_connected s
              | _ -> false
            in
            Log.debug (fun f -> f "Module Connection: Greeting -> FSM");
            let () =
              let (U s) = t.socket in
              match Socket.get_socket_type s with
              | Pair -> Socket.set_pair_connected s true
              | _ -> ()
            in
            let convert = function
              | Greeting.Set_server b ->
                  t.incoming_as_server <- b;
                  if
                    t.incoming_as_server
                    && Security_mechanism.get_as_server t.handshake_state
                  then Some (Close "Both ends cannot be servers")
                  else if
                    Security_mechanism.get_as_client t.handshake_state
                    && not t.incoming_as_server
                    (* TODO check validity of the logic *)
                  then None (*[Close "Other end is not a server"]*)
                  else None
              (* Assume security mechanism is pre-set*)
              | Check_mechanism s ->
                  if s <> Security_mechanism.get_name_string t.handshake_state
                  then Some (Close "Security Policy mismatch")
                  else None
              | Continue -> None
              | Ok ->
                  Log.debug (fun f -> f "Module Connection: Greeting OK");
                  t.stage <- HANDSHAKE;
                  if
                    Security_mechanism.send_command_after_greeting
                      t.handshake_state
                  then
                    Some
                      (Write
                         (Security_mechanism.first_command t.handshake_state))
                  else None
              | Error s -> Some (Close ("Greeting FSM error: " ^ s))
            in
            let state, actions, rest = Greeting.input t.greeting_state bytes in
            if if_pair_already_connected then
              Lwt.return [ Close "This PAIR is already connected" ]
            else (
              t.greeting_state <- state;
              (* Handle greeting part *)
              let action_list_1 = List.filter_map convert actions in
              (* Handle handshake part *)
              let* action_list_2 =
                if Cstruct.length rest > 0 then fsm t (Input_data rest)
                else Lwt.return_nil
              in
              Lwt.return (action_list_1 @ action_list_2))
        | HANDSHAKE -> (
            Log.debug (fun f -> f "Module Connection: Handshake -> FSM");
            t.fragments_parser <-
              Angstrom.Buffered.feed t.fragments_parser
                (`Bigstring (Utils.cstruct_to_bigstringaf bytes));
            match Utils.consume_parser t.fragments_parser Frame.parser with
            | [], state ->
                t.fragments_parser <- state;
                Lwt.return []
            | frame :: _, state ->
                t.fragments_parser <- state;
                (* assume only one command is received at a time *)
                let command = Command.of_frame frame in
                let new_state, actions =
                  Security_mechanism.fsm t.handshake_state command
                in
                let convert = function
                  | Security_mechanism.Write b -> [ Write b ]
                  | Security_mechanism.Continue -> []
                  | Security_mechanism.Ok ->
                      Log.debug (fun f -> f "Module Connection: Handshake OK");
                      t.stage <- TRAFFIC;
                      let (U s) = t.socket in
                      let frames = Socket.initial_traffic_messages s in
                      List.map (fun x -> Write (Frame.to_bytes x)) frames
                  | Security_mechanism.Close -> [ Close "Handshake FSM error" ]
                  | Security_mechanism.Received_property (name, value) -> (
                      match name with
                      | "Socket-Type" ->
                          let (U s) = t.socket in
                          let (U ty) = Socket_type.of_string value in
                          if
                            Socket_type.valid_socket_pair
                              (Socket.get_socket_type s) ty
                          then (
                            t.incoming_socket_type <- U ty;
                            [])
                          else [ Close "Socket type mismatch" ]
                      | "Identity" ->
                          t.incoming_identity <- value;
                          []
                      | _ ->
                          Log.debug (fun f ->
                              f "Module Connection: Ignore unknown property %s"
                                name);
                          [])
                in
                let actions = List.map convert actions |> List.flatten in
                t.handshake_state <- new_state;
                Lwt.return actions)
        | TRAFFIC -> (
            Log.debug (fun f -> f "Module Connection: TRAFFIC -> FSM");
            t.fragments_parser <-
              Angstrom.Buffered.feed t.fragments_parser
                (`Bigstring (Utils.cstruct_to_bigstringaf bytes));
            let frames, next_state =
              Utils.consume_parser t.fragments_parser Frame.parser
            in
            t.fragments_parser <- next_state;
            let manage_subscription () =
              let match_subscription_signature frame =
                if
                  (not (Frame.is_more frame))
                  && (not (Frame.is_command frame))
                  && not (Frame.is_long frame)
                then
                  let first_char =
                    (Bytes.to_string (Frame.get_body frame)).[0]
                  in
                  if first_char = Char.chr 1 then Subscribe
                  else if first_char = Char.chr 0 then Unsubscribe
                  else Ignore
                else Ignore
              in
              List.iter
                (fun x ->
                  match match_subscription_signature x with
                  | Unsubscribe ->
                      let body = Bytes.to_string (Frame.get_body x) in
                      let sub = String.sub body 1 (String.length body - 1) in
                      Trie.delete t.subscriptions sub
                  | Subscribe ->
                      let body = Bytes.to_string (Frame.get_body x) in
                      let sub = String.sub body 1 (String.length body - 1) in
                      Trie.insert t.subscriptions sub
                  | Ignore -> ())
                frames
            in
            let enqueue () =
              (* Put the received frames into the buffer *)
              Lwt_list.iter_s (fun x -> write_read_buffer t (Some x)) frames
            in
            let (U s) = t.socket in
            match Socket.get_socket_type s with
            | Pub ->
                manage_subscription ();
                Lwt.return []
            | Xpub ->
                let+ _ = enqueue () in
                manage_subscription ();
                []
            | _ ->
                let+ _ = enqueue () in
                [])
        | CLOSED -> Lwt.return [ Close "Connection FSM error" ])

  let close t =
    let (U s) = t.socket in
    (match Socket.get_socket_type s with
    | Pair -> Socket.set_pair_connected s false
    | _ -> ());
    t.stage <- CLOSED;
    t.send_buffer.close ()

  let send t ?(wait_until_sent = false) msg_list =
    let* () =
      Lwt_list.iter_s
        (fun x -> write_write_buffer t (Some (Frame.to_bytes x)))
        msg_list
    in
    let rec wait () =
      let* empty = Lwt_stream.is_empty t.send_buffer.stream in
      if empty then Lwt.return_unit
      else
        let* () = Lwt.pause () in
        wait ()
    in
    if wait_until_sent then wait () else Lwt.return_unit

  let if_send_queue_full t =
    match t.send_buffer.bounded_push with
    | None -> false
    | Some v -> v#count >= v#size
end
 *)
module Connection_tcp (S : Tcpip.Stack.V4V6) = struct
  (* Start of helper functions *)

  (** Creates a tag for a TCP connection *)
  let tag_of_tcp_connection ipaddr port = Fmt.str "TCP.%s.%d" ipaddr port

  (** Read input from flow, send the input to FSM and execute FSM actions *)
  let rec process_input ?(tag = "") flow connection =
    let* res = S.TCP.read flow in
    let* continue =
      match res with
      | Ok `Eof ->
          Log.info (fun f ->
              f "Module Connection_tcp(%s): Closing connection EOF" tag);
          Lwt.return_false
      | Error e ->
          Log.warn (fun f ->
              f
                "Module Raw_connection_tcp(%s): Error reading data from \
                 established connection: %a"
                tag S.TCP.pp_error e);
          Lwt.return_false
      | Ok (`Data b) -> (
          Log.info (fun f ->
              f "Module Connection_tcp(%s): Read: %d bytes" tag
                (Cstruct.length b));
          match Raw_connection.input connection (Data b) with
          | Result.Ok [] -> Lwt.return_true
          | Ok b ->
              Log.debug (fun f ->
                  f "Module Connection_tcp(%s): Connection FSM Write %d bytes"
                    tag (Cstruct.lenv b));
              let+ write_res = S.TCP.writev flow b in
              (match write_res with
              | Error _ ->
                  Log.warn (fun f ->
                      f
                        "Module Connection_tcp(%s): Error writing data to \
                         established connection."
                        tag)
              | Ok () -> ());
              true
          | Error (`Closed s) ->
              Log.warn (fun f ->
                  f "Module Connection_tcp(%s): Connection FSM Close due to: %s"
                    tag s);
              Lwt.return_false)
    in
    if continue then process_input ~tag flow connection else Lwt.return_unit

  (** Check the 'mailbox' and send outgoing data / close connection *)
  let rec process_output ?(tag = "") flow connection =
    let* actions = Raw_connection.output connection in
    match actions with
    | Error (`Closed msg) ->
        Log.info (fun f ->
            f
              "Module Connection_tcp(%s): Connection was instructed to close: \
               %s"
              tag msg);
        Lwt.return_unit
    | Ok [] -> process_output ~tag flow connection
    | Ok data -> (
        Log.debug (fun f ->
            f "Module Connection_tcp(%s): Connection mailbox Write %d bytes" tag
              (Cstruct.lenv data));
        let* write_res = S.TCP.writev flow data in
        match write_res with
        | Error _ ->
            Log.warn (fun f ->
                f
                  "Module Connection_tcp(%s): Error writing data to \
                   established connection."
                  tag);
            Lwt.return_unit
        | Ok () -> process_output ~tag flow connection)

  (* End of helper functions *)

  let listen s port socket =
    S.TCP.listen (S.tcp s) ~port (fun flow ->
        let dst, dst_port = S.TCP.dst flow in
        Log.info (fun f ->
            f "Module Connection_tcp: New tcp connection from IP %s on port %d"
              (Ipaddr.to_string dst) dst_port);
        let connection =
          Raw_connection.init (Socket.typ' socket)
            (Security_mechanism.init
               (Socket.security_info socket)
               (Socket.metadata socket))
            (tag_of_tcp_connection (Ipaddr.to_string dst) dst_port)
        in
        let* () =
          Lwt.pick
            [
              Socket.add_connection socket connection;
              process_input ~tag:"server" flow connection;
              process_output ~tag:"server" flow connection;
            ]
        in
        Log.info (fun f ->
            f "Module Connection_tcp(server): client %s:%d disconnected "
              (Ipaddr.to_string dst) dst_port);
        S.TCP.close flow);
    S.listen s

  let rec connect s addr port socket =
    let connection =
      Raw_connection.init (Socket.typ' socket)
        (Security_mechanism.init
           (Socket.security_info socket)
           (Socket.metadata socket))
        (tag_of_tcp_connection addr port)
    in
    let ipaddr = Ipaddr.of_string_exn addr in
    (* TODO what if connection lost after successful one: should transparently work *)
    let* flow = S.TCP.create_connection (S.tcp s) (ipaddr, port) in
    let disconnect = Lwt_switch.create () in
    match flow with
    | Ok flow ->
        Lwt_switch.add_hook (Some disconnect) (fun () ->
            Log.info (fun f ->
                f
                  "Module Connection_tcp(client): %a:%d asking for \
                   disconnection"
                  Ipaddr.pp ipaddr port);
            Raw_connection.close_output connection;
            S.TCP.close flow);
        Lwt.dont_wait
          (fun () ->
            let+ () =
              Lwt.join
                [
                  Socket.add_connection socket connection;
                  (let+ () = process_input ~tag:"client" flow connection in
                   Log.info (fun f ->
                       f "Module Connection_tcp(client): process_input finished");
                   Raw_connection.close_input connection);
                  (let+ () = process_output ~tag:"client" flow connection in
                   Log.info (fun f ->
                       f
                         "Module Connection_tcp(client): process_output \
                          finished"));
                ]
            in
            Log.info (fun f ->
                f "Module Connection_tcp(client): server %s:%d disconnected "
                  (Ipaddr.to_string ipaddr) port))
          raise;
        Log.info (fun f -> f "Spawned threads");
        let+ _ = Raw_connection.wait_until_ready connection in
        Log.info (fun f -> f "Traffic");
        disconnect
    | Error e ->
        Log.warn (fun f ->
            f
              "Module Connection_tcp: Error establishing connection: %a, \
               retrying"
              S.TCP.pp_error e);
        connect s addr port socket

  let disconnect s = Lwt_switch.turn_off s
end

module Socket_tcp (S : Tcpip.Stack.V4V6) = struct
  (* type transport_info = Tcp of string * int
 *)
  type 'b t = U : ('a, 'b) Socket.t -> 'b t

  let create_socket context ?(mechanism = Security_mechanism.NULL) socket_type =
    U (Socket.create_socket context ~mechanism socket_type)

  let set_plain_credentials (U socket) username password =
    Socket.set_plain_credentials socket username password

  let set_plain_user_list (U socket) list =
    Socket.set_plain_user_list socket list

  let set_identity (U socket) identity = Socket.set_identity socket identity

  let set_incoming_queue_size (U socket) size =
    Socket.set_incoming_queue_size socket size

  let set_outgoing_queue_size (U socket) size =
    Socket.set_outgoing_queue_size socket size

  let subscribe (U socket) subscription = Socket.subscribe socket subscription

  let unsubscribe (U socket) subscription =
    Socket.unsubscribe socket subscription

  let recv (U socket) = Socket.recv socket
  let recv_multipart (U socket) = Socket.recv_multipart socket
  let send (U socket) msg = Socket.send socket msg
  let send_multipart (U socket) msg = Socket.send_multipart socket msg
  let recv_from (U socket) = Socket.recv_from socket
  let send_to (U socket) msg = Socket.send_to socket msg
  let send_blocking (U socket) msg = Socket.send_blocking socket msg

  module C_tcp = Connection_tcp (S)

  let bind (U socket) port s = Lwt.async (fun () -> C_tcp.listen s port socket)

  type flow = Lwt_switch.t

  let connect (U socket) ipaddr port s = C_tcp.connect s ipaddr port socket
  let disconnect f = C_tcp.disconnect f
end
