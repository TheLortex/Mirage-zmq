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

let print_debug_logs = true

exception No_Available_Peers
exception Should_Not_Reach
exception Socket_Name_Not_Recognised
exception Not_Able_To_Set_Credentials
exception Internal_Error of string
exception Incorrect_use_of_API of string
exception Connection_closed

type socket_type =
  | REQ
  | REP
  | DEALER
  | ROUTER
  | PUB
  | XPUB
  | SUB
  | XSUB
  | PUSH
  | PULL
  | PAIR

(* CURVE not implemented *)
type message_type = Data of string | Identity_and_data of string * string

type connection_stage = GREETING | HANDSHAKE | TRAFFIC | CLOSED
type connection_fsm_data = Input_data of Cstruct.t | End_of_connection

module rec Socket : sig
  type t

  val socket_type_from_string : string -> socket_type
  val if_valid_socket_pair : socket_type -> socket_type -> bool

  (*
     val if_has_incoming_queue : socket_type -> bool *)

  val if_has_outgoing_queue : socket_type -> bool

  val get_socket_type : t -> socket_type
  (** Get the type of the socket *)

  val get_metadata : t -> Security_mechanism.socket_metadata
  (** Get the metadata of the socket for handshake *)
  (*
     val get_mechanism : t -> mechanism_type
     (** Get the security mechanism of the socket *)
  *)

  val get_security_data : t -> Security_mechanism.security_data
  (** Get the security credentials of the socket *)

  val get_incoming_queue_size : t -> int option
  (** Get the maximum capacity of the incoming queue *)

  val get_outgoing_queue_size : t -> int option
  (** Get the maximum capacity of the outgoing queue *)

  val get_pair_connected : t -> bool
  (** Whether a PAIR is already connected to another PAIR *)

  val create_socket : Context.t -> ?mechanism:Security_mechanism.mechanism_type -> socket_type -> t
  (** Create a socket from the given context, mechanism and type *)

  val set_plain_credentials : t -> string -> string -> unit
  (** Set username and password for PLAIN client *)

  val set_plain_user_list : t -> (string * string) list -> unit
  (** Set password list for PLAIN server *)

  val set_identity : t -> string -> unit
  (** Set identity string of a socket if applicable *)

  val set_incoming_queue_size : t -> int -> unit
  (** Set the maximum capacity of the incoming queue *)

  val set_outgoing_queue_size : t -> int -> unit
  (** Set the maximum capacity of the outgoing queue *)

  val set_pair_connected : t -> bool -> unit
  (** Set PAIR's connection status *)

  val subscribe : t -> string -> unit
  val unsubscribe : t -> string -> unit

  val recv : t -> message_type Lwt.t
  (** Receive a msg from the underlying connections, according to the semantics of the socket type *)

  val send : t -> message_type -> unit Lwt.t
  (** Send a msg to the underlying connections, according to the semantics of the socket type *)

  val send_blocking : t -> message_type -> unit Lwt.t
  val add_connection : t -> Connection.t -> unit

  val initial_traffic_messages : t -> Frame.t list
  (** Get the messages to send at the beginning of a connection, e.g. subscriptions *)
end = struct
  type socket_states =
    (* | NONE *)
    | Rep of {
        if_received : bool;
        last_received_connection_tag : string;
        address_envelope : Frame.t list;
      }
    | Req of { if_sent : bool; last_sent_connection_tag : string }
    | Dealer of { request_order_queue : string Queue.t }
    | Router
    | Pub
    | Sub of { subscriptions : Utils.Trie.t }
    | Xpub
    | Xsub of { subscriptions : Utils.Trie.t }
    | Push
    | Pull
    | Pair of { connected : bool }

  type t = {
    socket_type : socket_type;
    mutable metadata : Security_mechanism.socket_metadata;
    security_mechanism : Security_mechanism.mechanism_type;
    mutable security_info : Security_mechanism.security_data;
    connections : Connection.t Queue.t;
    connections_condition : unit Lwt_condition.t;
    mutable socket_states : socket_states;
    mutable incoming_queue_size : int option;
    mutable outgoing_queue_size : int option;
  }

  (* Start of helper functions *)

  (** Returns the socket type from string *)
  let socket_type_from_string = function
    | "REQ" -> REQ
    | "REP" -> REP
    | "DEALER" -> DEALER
    | "ROUTER" -> ROUTER
    | "PUB" -> PUB
    | "XPUB" -> XPUB
    | "SUB" -> SUB
    | "XSUB" -> XSUB
    | "PUSH" -> PUSH
    | "PULL" -> PULL
    | "PAIR" -> PAIR
    | _ -> raise Socket_Name_Not_Recognised

  (** Checks if the pair is valid as specified by 23/ZMTP *)
  let if_valid_socket_pair a b =
    match (a, b) with
    | REQ, REP
    | REQ, ROUTER
    | REP, REQ
    | REP, DEALER
    | DEALER, REP
    | DEALER, DEALER
    | DEALER, ROUTER
    | ROUTER, REQ
    | ROUTER, DEALER
    | ROUTER, ROUTER
    | PUB, SUB
    | PUB, XSUB
    | XPUB, SUB
    | XPUB, XSUB
    | SUB, PUB
    | SUB, XPUB
    | XSUB, PUB
    | XSUB, XPUB
    | PUSH, PULL
    | PULL, PUSH
    | PAIR, PAIR ->
        true
    | _ -> false
  (*
     (** Whether the socket type has connections with limited-size queues *)
     let if_queue_size_limited socket =
       match socket with
       | REP | REQ ->
           false
       | DEALER | ROUTER | PUB | SUB | XPUB | XSUB | PUSH | PULL | PAIR ->
           true
  *)

  (** Whether the socket type has an outgoing queue *)
  let if_has_outgoing_queue socket =
    match socket with
    | REP | REQ | DEALER | ROUTER | PUB | SUB | XPUB | XSUB | PUSH | PAIR ->
        true
    | PULL -> false
  (*
     (** Whether the socket type has an incoming queue *)
     let if_has_incoming_queue socket =
       match socket with
       | REP | REQ | DEALER | ROUTER | PUB | SUB | XPUB | XSUB | PULL | PAIR ->
           true
       | PUSH ->
           false *)

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

  (* let rotate list connection if_remove_head =
     let rec rotate_accumu list accumu =
       match list with
       | [] ->
           if not if_remove_head then connection :: accumu else accumu
       | hd :: tl ->
           if Connection.get_tag !hd = Connection.get_tag !connection then
             rotate_accumu tl accumu
           else rotate_accumu tl (hd :: accumu)
     in
     List.rev (rotate_accumu list [])
  *)

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
          if Frame.get_if_more next_frame then
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
      Logs.debug (fun f -> f "receive_and_rotate");
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
                Lwt.return (Data (Frame.splice_message_frames frames)))
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
          else if Frame.get_if_more next_frame then
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

  let get_pair_connected t =
    match t.socket_states with
    | Pair { connected } -> connected
    | _ ->
        raise
          (Incorrect_use_of_API
             "Cannot call this function on a socket other than PAIR!")

  let create_socket context ?(mechanism = Security_mechanism.NULL) socket_type =
    match socket_type with
    | REP ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "REP") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states =
            Rep
              {
                if_received = false;
                last_received_connection_tag = "";
                address_envelope = [];
              };
          incoming_queue_size = None;
          outgoing_queue_size = None;
        }
    | REQ ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "REQ") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = Req { if_sent = false; last_sent_connection_tag = "" };
          incoming_queue_size = None;
          outgoing_queue_size = None;
        }
    | DEALER ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "DEALER"); ("Identity", "") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = Dealer { request_order_queue = Queue.create () };
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | ROUTER ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "ROUTER") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = Router;
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | PUB ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "PUB") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = Pub;
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | XPUB ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "XPUB") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = Xpub;
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | SUB ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "SUB") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = Sub { subscriptions = Utils.Trie.create () };
          incoming_queue_size =
            Some (Context.get_default_queue_size context)
            (* Need an outgoing queue to send subscriptions *);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | XSUB ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "XSUB") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = Xsub { subscriptions = Utils.Trie.create () };
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | PUSH ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "PUSH") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = Push;
          incoming_queue_size = None;
          outgoing_queue_size = Some (Context.get_default_queue_size context);
        }
    | PULL ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "PULL") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = Pull;
          incoming_queue_size = Some (Context.get_default_queue_size context);
          outgoing_queue_size = None;
        }
    | PAIR ->
        {
          socket_type;
          metadata = [ ("Socket-Type", "PAIR") ];
          security_mechanism = mechanism;
          security_info = Null;
          connections = Queue.create ();
          connections_condition = Lwt_condition.create ();
          socket_states = Pair { connected = false };
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

  let set_pair_connected t status =
    match t.socket_type with
    | PAIR -> t.socket_states <- Pair { connected = status }
    | _ ->
        raise
          (Incorrect_use_of_API "This state can only be set for PAIR socket!")

  let subscribe t (subscription : string) =
    match t.socket_type with
    | SUB | XSUB -> (
        match t.socket_states with
        | Sub { subscriptions } ->
            Utils.Trie.insert subscriptions subscription;
            send_message_to_all_active_connections t.connections
              (subscription_frame subscription)
        | Xsub { subscriptions } ->
            Utils.Trie.insert subscriptions subscription;
            send_message_to_all_active_connections t.connections
              (subscription_frame subscription)
        | _ -> raise Should_Not_Reach)
    | _ ->
        raise
          (Incorrect_use_of_API "This socket does not support subscription!")

  let unsubscribe t subscription =
    match t.socket_type with
    | SUB | XSUB -> (
        match t.socket_states with
        | Sub { subscriptions } ->
            Utils.Trie.delete subscriptions subscription;
            send_message_to_all_active_connections t.connections
              (unsubscription_frame subscription)
        | Xsub { subscriptions } ->
            Utils.Trie.delete subscriptions subscription;
            send_message_to_all_active_connections t.connections
              (unsubscription_frame subscription)
        | _ -> raise Should_Not_Reach)
    | _ ->
        raise
          (Incorrect_use_of_API "This socket does not support unsubscription!")

  (*
     type queue_fold_type = UNINITIALISED | Result of Connection.t
  *)
  let rec recv t =
    match t.socket_type with
    | REP -> (
        match t.socket_states with
        | Rep _ -> (
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
                          t.socket_states <-
                            Rep
                              {
                                if_received = true;
                                last_received_connection_tag =
                                  Connection.get_tag connection;
                                address_envelope;
                              };
                          Lwt.return (Data (Frame.splice_message_frames frames))
                      )))
        | _ -> raise Should_Not_Reach)
    | REQ -> (
        match t.socket_states with
        | Req { if_sent; last_sent_connection_tag = tag } -> (
            if not if_sent then
              raise
                (Incorrect_use_of_API "Need to send a request before receiving")
            else
              let find_and_send connections =
                let head = Queue.peek connections in
                if tag = Connection.get_tag head then
                  if Connection.get_stage head = TRAFFIC then (
                    let* frame_list = get_frame_list head in
                    match frame_list with
                    | None -> Lwt.return None
                    | Some frames ->
                        t.socket_states <-
                          Req { if_sent = false; last_sent_connection_tag = "" };
                        Lwt.return (Some (Frame.splice_message_frames frames)))
                  else (
                    rotate t.connections true;
                    t.socket_states <-
                      Req { if_sent = false; last_sent_connection_tag = "" };
                    raise Connection_closed)
                else
                  raise
                    (Internal_Error "Receive target no longer at head of queue")
              in
              let* result = find_and_send t.connections in
              match result with
              | Some result ->
                  rotate t.connections false;
                  Lwt.return (Data result)
              | None ->
                  rotate t.connections true;
                  t.socket_states <-
                    Req { if_sent = false; last_sent_connection_tag = "" };
                  raise Connection_closed)
        | _ -> raise Should_Not_Reach)
    | DEALER -> (
        match t.socket_states with
        | Dealer { request_order_queue } -> (
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
                      Lwt.return (Data (Frame.splice_message_frames frames))))
        | _ -> raise Should_Not_Reach)
    | ROUTER -> (
        match t.socket_states with
        | Router -> (
            if Queue.is_empty t.connections then
              let* () = Lwt.pause () in
              recv t
            else
              let* connection =
                find_connection_with_incoming_buffer t.connections
              in
              match connection with
              | None ->
                  let* () = Lwt.pause () in
                  recv t
              | Some connection -> (
                  (* Reconstruct message from the connection *)
                  let* frames = get_frame_list connection in
                  match frames with
                  | None ->
                      Connection.close connection;
                      rotate t.connections true;
                      let* () = Lwt.pause () in
                      recv t
                  | Some frames ->
                      (* Put the received connection at the end of the queue *)
                      rotate t.connections false;
                      Lwt.return
                        (Identity_and_data
                           ( Connection.get_identity connection,
                             Frame.splice_message_frames frames ))))
        | _ -> raise Should_Not_Reach)
    | PUB -> raise (Incorrect_use_of_API "Cannot receive from PUB")
    | SUB -> (
        match t.socket_states with
        | Sub { subscriptions = _ } ->
            receive_and_rotate (t.connections, t.connections_condition)
        | _ -> raise Should_Not_Reach)
    | XPUB -> (
        match t.socket_states with
        | Xpub -> receive_and_rotate (t.connections, t.connections_condition)
        | _ -> raise Should_Not_Reach)
    | XSUB -> (
        match t.socket_states with
        | Xsub { subscriptions = _ } ->
            receive_and_rotate (t.connections, t.connections_condition)
        | _ -> raise Should_Not_Reach)
    | PUSH -> raise (Incorrect_use_of_API "Cannot receive from PUSH")
    | PULL -> (
        match t.socket_states with
        | Pull -> receive_and_rotate (t.connections, t.connections_condition)
        | _ -> raise Should_Not_Reach)
    | PAIR -> (
        match t.socket_states with
        | Pair { connected } ->
            if Queue.is_empty t.connections then raise No_Available_Peers
            else
              let connection = Queue.peek t.connections in
              if connected && Connection.get_stage connection = TRAFFIC then
                let* frames = get_frame_list connection in
                match frames with
                | None ->
                    Connection.close connection;
                    t.socket_states <- Pair { connected = false };
                    rotate t.connections true;
                    raise Connection_closed
                | Some frames ->
                    Lwt.return (Data (Frame.splice_message_frames frames))
              else raise No_Available_Peers
        | _ -> raise Should_Not_Reach)

  let send t msg =
    let frames =
      match msg with
      | Data msg ->
          List.map (fun x -> Message.to_frame x) (Message.list_of_string msg)
      | Identity_and_data (_id, msg) ->
          List.map (fun x -> Message.to_frame x) (Message.list_of_string msg)
    in
    let try_send () =
      match t.socket_type with
      | REP -> (
          match msg with
          | Data _msg -> (
              let state = t.socket_states in
              match state with
              | Rep
                  {
                    if_received;
                    last_received_connection_tag = tag;
                    address_envelope;
                  } ->
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
                              (address_envelope
                              @ (Frame.delimiter_frame :: frames))
                          in
                          t.socket_states <-
                            Rep
                              {
                                if_received = false;
                                last_received_connection_tag = "";
                                address_envelope = [];
                              };
                          rotate t.connections false;
                          Lwt.return_unit)
                        else Lwt.return_unit
                      else
                        raise
                          (Internal_Error
                             "Send target no longer at head of queue")
                    in
                    find_and_send t.connections
              | _ -> raise Should_Not_Reach)
          | _ -> raise (Incorrect_use_of_API "REP sends [Data(string)]"))
      | REQ -> (
          match msg with
          | Data _msg -> (
              let state = t.socket_states in
              match state with
              | Req { if_sent; last_sent_connection_tag = _ } -> (
                  if if_sent then
                    raise
                      (Incorrect_use_of_API
                         "Need to receive a reply before sending another \
                          message")
                  else
                    find_available_connection t.connections |> function
                    | None -> raise No_Available_Peers
                    | Some connection ->
                        (* TODO check re-send is working *)
                        Lwt_stream.get_available
                          (Connection.get_read_buffer connection)
                        |> ignore;
                        let* () =
                          Connection.send connection
                            (Frame.delimiter_frame :: frames)
                        in
                        t.socket_states <-
                          Req
                            {
                              if_sent = true;
                              last_sent_connection_tag =
                                Connection.get_tag connection;
                            };
                        Lwt.return_unit)
              | _ -> raise Should_Not_Reach)
          | _ -> raise (Incorrect_use_of_API "REP sends [Data(string)]"))
      | DEALER -> (
          (* TODO investigate DEALER dropping messages *)
          match msg with
          | Data _msg -> (
              let state = t.socket_states in
              match state with
              | Dealer { request_order_queue } -> (
                  if Queue.is_empty t.connections then raise No_Available_Peers
                  else
                    find_available_connection t.connections |> function
                    | None -> raise No_Available_Peers
                    | Some connection ->
                        let* () =
                          Connection.send connection ~wait_until_sent:true
                            (Frame.delimiter_frame :: frames)
                        in
                        Queue.push
                          (Connection.get_tag connection)
                          request_order_queue;
                        if print_debug_logs then
                          Logs.debug (fun f -> f "Message sent");
                        rotate t.connections false;
                        Lwt.return_unit)
              | _ -> raise Should_Not_Reach)
          | _ -> raise (Incorrect_use_of_API "DEALER sends [Data(string)]"))
      | ROUTER -> (
          match msg with
          | Identity_and_data (id, _msg) -> (
              match t.socket_states with
              | Router -> (
                  find_connection t.connections (fun connection ->
                      Connection.get_identity connection = id)
                  |> function
                  | None -> Lwt.return_unit
                  | Some connection ->
                      let frame_list =
                        if Connection.get_incoming_socket_type connection == REQ
                        then Frame.delimiter_frame :: frames
                        else frames
                      in
                      Connection.send connection frame_list
                  (* TODO: Drop message when queue full *))
              | _ -> raise Should_Not_Reach)
          | _ ->
              raise
                (Incorrect_use_of_API
                   "Sending a message via ROUTER needs a specified receiver \
                    identity!"))
      | PUB -> (
          match msg with
          | Data msg -> (
              match t.socket_states with
              | Pub ->
                  broadcast t.connections msg (fun connection ->
                      Utils.Trie.find
                        (Connection.get_subscriptions connection)
                        msg);
                  Lwt.return_unit
              | _ -> raise Should_Not_Reach)
          | _ -> raise (Incorrect_use_of_API "PUB accepts a message only!"))
      | SUB -> raise (Incorrect_use_of_API "Cannot send via SUB")
      | XPUB -> (
          match msg with
          | Data msg -> (
              match t.socket_states with
              | Xpub ->
                  broadcast t.connections msg (fun connection ->
                      Utils.Trie.find
                        (Connection.get_subscriptions connection)
                        msg);
                  Lwt.return_unit
              | _ -> raise Should_Not_Reach)
          | _ -> raise (Incorrect_use_of_API "XPUB accepts a message only!"))
      | XSUB -> (
          match msg with
          | Data msg -> (
              match t.socket_states with
              | Xsub { subscriptions = _ } ->
                  broadcast t.connections msg (fun _ -> true);
                  Lwt.return_unit
              | _ -> raise Should_Not_Reach)
          | _ -> raise (Incorrect_use_of_API "XSUB accepts a message only!"))
      | PUSH -> (
          match msg with
          | Data _msg -> (
              match t.socket_states with
              | Push -> (
                  if Queue.is_empty t.connections then raise No_Available_Peers
                  else
                    find_available_connection t.connections |> function
                    | None -> raise No_Available_Peers
                    | Some connection ->
                        let+ () = Connection.send connection frames in
                        rotate t.connections false)
              | _ -> raise Should_Not_Reach)
          | _ -> raise (Incorrect_use_of_API "PUSH accepts a message only!"))
      | PULL -> raise (Incorrect_use_of_API "Cannot send via PULL")
      | PAIR -> (
          match msg with
          | Data _msg -> (
              match t.socket_states with
              | Pair { connected } ->
                  if Queue.is_empty t.connections then raise No_Available_Peers
                  else
                    let connection = Queue.peek t.connections in
                    if
                      connected
                      && Connection.get_stage connection = TRAFFIC
                      && not (Connection.if_send_queue_full connection)
                    then Connection.send connection frames
                    else raise No_Available_Peers
              | _ -> raise Should_Not_Reach)
          | _ -> raise (Incorrect_use_of_API "PUSH accepts a message only!"))
    in
    try_send ()

  let rec send_blocking t msg =
    try send t msg
    with No_Available_Peers ->
      Logs.debug (fun f -> f "send_blocking: no available peers");
      let* () = Lwt.pause () in
      send_blocking t msg

  let add_connection t connection =
    Queue.push connection t.connections;
    Lwt_condition.broadcast t.connections_condition ()

  let initial_traffic_messages t =
    match t.socket_type with
    | SUB -> (
        match t.socket_states with
        | Sub { subscriptions } ->
            if not (Utils.Trie.is_empty subscriptions) then
              List.map
                (fun x -> subscription_frame x)
                (Utils.Trie.to_list subscriptions)
            else []
        | _ -> raise Should_Not_Reach)
    | XSUB -> (
        match t.socket_states with
        | Xsub { subscriptions } ->
            if not (Utils.Trie.is_empty subscriptions) then
              List.map
                (fun x -> subscription_frame x)
                (Utils.Trie.to_list subscriptions)
            else []
        | _ -> raise Should_Not_Reach)
    | _ -> []
end

and Connection : sig
  type t
  type action = Write of bytes | Close of string

  val init : Socket.t -> Security_mechanism.t -> string -> t
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

  val get_socket : t -> Socket.t
  val get_identity : t -> string
  val get_subscriptions : t -> Utils.Trie.t
  val get_incoming_socket_type : t -> socket_type
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

  type t = {
    tag : string;
    socket : Socket.t;
    mutable greeting_state : Greeting.t;
    mutable handshake_state : Security_mechanism.t;
    mutable stage : connection_stage;
    mutable expected_bytes_length : int;
    mutable incoming_as_server : bool;
    mutable incoming_socket_type : socket_type;
    mutable incoming_identity : string;
    read_buffer : Frame.t buffer_stream;
    send_buffer : Bytes.t buffer_stream;
    mutable subscriptions : Utils.Trie.t;
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
      socket;
      greeting_state = Greeting.init security_mechanism;
      handshake_state = security_mechanism;
      stage = GREETING;
      expected_bytes_length = 64;
      (* A value of 0 means expecting a frame of any length; starting with expectint the whole greeting *)
      incoming_socket_type = REP;
      incoming_as_server = false;
      incoming_identity = tag;
      read_buffer;
      send_buffer;
      subscriptions = Utils.Trie.create ();
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
        let* () = write_read_buffer t None in
        Lwt.return []
    | Input_data bytes -> (
        match t.stage with
        | GREETING -> (
            let if_pair =
              match Socket.get_socket_type t.socket with
              | PAIR -> true
              | _ -> false
            in
            let if_pair_already_connected =
              match Socket.get_socket_type t.socket with
              | PAIR -> Socket.get_pair_connected t.socket
              | _ -> false
            in
            if print_debug_logs then
              Logs.debug (fun f -> f "Module Connection: Greeting -> FSM");
            if if_pair then Socket.set_pair_connected t.socket true;
            let len = Cstruct.length bytes in
            let rec convert greeting_action_list =
              match greeting_action_list with
              | [] -> []
              | hd :: tl -> (
                  match hd with
                  | Greeting.Set_server b ->
                      t.incoming_as_server <- b;
                      if
                        t.incoming_as_server
                        && Security_mechanism.get_as_server t.handshake_state
                      then [ Close "Both ends cannot be servers" ]
                      else if
                        Security_mechanism.get_as_client t.handshake_state
                        && not t.incoming_as_server
                        (* TODO check validity of the logic *)
                      then convert tl (*[Close "Other end is not a server"]*)
                      else convert tl
                  (* Assume security mechanism is pre-set*)
                  | Greeting.Check_mechanism s ->
                      if
                        s
                        <> Security_mechanism.get_name_string t.handshake_state
                      then [ Close "Security Policy mismatch" ]
                      else convert tl
                  | Greeting.Continue -> convert tl
                  | Greeting.Ok ->
                      if print_debug_logs then
                        Logs.debug (fun f -> f "Module Connection: Greeting OK");
                      t.stage <- HANDSHAKE;
                      if
                        Security_mechanism.if_send_command_after_greeting
                          t.handshake_state
                      then
                        Write
                          (Security_mechanism.first_command t.handshake_state)
                        :: convert tl
                      else convert tl
                  | Greeting.Error s -> [ Close ("Greeting FSM error: " ^ s) ])
            in
            match len with
            (* Hard code the length here. The greeting is either complete or split into 11 + 53 or 10 + 54 *)
            (* Full greeting *)
            | 64 ->
                let state, action_list =
                  Greeting.fsm t.greeting_state
                    [
                      Greeting.Recv_sig
                        (Cstruct.sub bytes 0 10 |> Cstruct.to_bytes);
                      Greeting.Recv_Vmajor
                        (Cstruct.sub bytes 10 1 |> Cstruct.to_bytes);
                      Greeting.Recv_Vminor
                        (Cstruct.sub bytes 11 1 |> Cstruct.to_bytes);
                      Greeting.Recv_Mechanism
                        (Cstruct.sub bytes 12 20 |> Cstruct.to_bytes);
                      Greeting.Recv_as_server
                        (Cstruct.sub bytes 32 1 |> Cstruct.to_bytes);
                      Greeting.Recv_filler;
                    ]
                in
                let connection_action = convert action_list in
                if if_pair_already_connected then
                  Lwt.return [ Close "This PAIR is already connected" ]
                else (
                  t.greeting_state <- state;
                  t.expected_bytes_length <- 0;
                  Lwt.return connection_action)
            (* Signature + version major *)
            | 11 ->
                let state, action_list =
                  Greeting.fsm t.greeting_state
                    [
                      Greeting.Recv_sig
                        (Cstruct.sub bytes 0 10 |> Cstruct.to_bytes);
                      Greeting.Recv_Vmajor
                        (Cstruct.sub bytes 10 1 |> Cstruct.to_bytes);
                    ]
                in
                if if_pair_already_connected then
                  Lwt.return [ Close "This PAIR is already connected" ]
                else (
                  t.greeting_state <- state;
                  t.expected_bytes_length <- 53;
                  Lwt.return (convert action_list))
            (* Signature *)
            | 10 ->
                let state, action =
                  Greeting.fsm_single t.greeting_state
                    (Greeting.Recv_sig (Cstruct.to_bytes bytes))
                in
                if if_pair_already_connected then
                  Lwt.return [ Close "This PAIR is already connected" ]
                else (
                  t.greeting_state <- state;
                  t.expected_bytes_length <- 54;
                  Lwt.return (convert [ action ]))
            (* version minor + rest *)
            | 53 ->
                let state, action_list =
                  Greeting.fsm t.greeting_state
                    [
                      Greeting.Recv_Vminor
                        (Cstruct.sub bytes 0 1 |> Cstruct.to_bytes);
                      Greeting.Recv_Mechanism
                        (Cstruct.sub bytes 1 20 |> Cstruct.to_bytes);
                      Greeting.Recv_as_server
                        (Cstruct.sub bytes 21 1 |> Cstruct.to_bytes);
                      Greeting.Recv_filler;
                    ]
                in
                let connection_action = convert action_list in
                t.greeting_state <- state;
                t.expected_bytes_length <- 0;
                Lwt.return connection_action
            (* version major + rest *)
            | 54 ->
                let state, action_list =
                  Greeting.fsm t.greeting_state
                    [
                      Greeting.Recv_Vmajor
                        (Cstruct.sub bytes 0 1 |> Cstruct.to_bytes);
                      Greeting.Recv_Vminor
                        (Cstruct.sub bytes 1 1 |> Cstruct.to_bytes);
                      Greeting.Recv_Mechanism
                        (Cstruct.sub bytes 2 20 |> Cstruct.to_bytes);
                      Greeting.Recv_as_server
                        (Cstruct.sub bytes 22 1 |> Cstruct.to_bytes);
                      Greeting.Recv_filler;
                    ]
                in
                let connection_action = convert action_list in
                t.greeting_state <- state;
                t.expected_bytes_length <- 0;
                Lwt.return connection_action
            | n ->
                if n < t.expected_bytes_length then
                  Lwt.return [ Close "Message too short" ]
                else
                  let expected_length = t.expected_bytes_length in
                  (* Handle greeting part *)
                  let* action_list_1 =
                    fsm t (Input_data (Cstruct.sub bytes 0 expected_length))
                  in
                  (* Handle handshake part *)
                  let* action_list_2 =
                    fsm t
                      (Input_data
                         (Cstruct.sub bytes expected_length (n - expected_length)))
                  in
                  Lwt.return (action_list_1 @ action_list_2))
        | HANDSHAKE -> (
            if print_debug_logs then
              Logs.debug (fun f -> f "Module Connection: Handshake -> FSM");
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
                let rec convert handshake_action_list =
                  match handshake_action_list with
                  | [] -> []
                  | hd :: tl -> (
                      match hd with
                      | Security_mechanism.Write b -> Write b :: convert tl
                      | Security_mechanism.Continue -> convert tl
                      | Security_mechanism.Ok ->
                          if print_debug_logs then
                            Logs.debug (fun f ->
                                f "Module Connection: Handshake OK");
                          t.stage <- TRAFFIC;
                          let frames =
                            Socket.initial_traffic_messages t.socket
                          in
                          List.map (fun x -> Write (Frame.to_bytes x)) frames
                          @ convert tl
                      | Security_mechanism.Close ->
                          [ Close "Handshake FSM error" ]
                      | Security_mechanism.Received_property (name, value) -> (
                          match name with
                          | "Socket-Type" ->
                              if
                                Socket.if_valid_socket_pair
                                  (Socket.get_socket_type t.socket)
                                  (Socket.socket_type_from_string value)
                              then (
                                t.incoming_socket_type <-
                                  Socket.socket_type_from_string value;
                                convert tl)
                              else [ Close "Socket type mismatch" ]
                          | "Identity" ->
                              t.incoming_identity <- value;
                              convert tl
                          | _ ->
                              if print_debug_logs then
                                Logs.debug (fun f ->
                                    f
                                      "Module Connection: Ignore unknown \
                                       property %s"
                                      name);
                              convert tl))
                in
                let actions = convert actions in
                t.handshake_state <- new_state;
                Lwt.return actions)
        | TRAFFIC -> (
            if print_debug_logs then
              Logs.debug (fun f -> f "Module Connection: TRAFFIC -> FSM");
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
                  (not (Frame.get_if_more frame))
                  && (not (Frame.get_if_command frame))
                  && not (Frame.get_if_long frame)
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
                      Utils.Trie.delete t.subscriptions sub
                  | Subscribe ->
                      let body = Bytes.to_string (Frame.get_body x) in
                      let sub = String.sub body 1 (String.length body - 1) in
                      Utils.Trie.insert t.subscriptions sub
                  | Ignore -> ())
                frames
            in
            let enqueue () =
              (* Put the received frames into the buffer *)
              Lwt_list.iter_s (fun x -> write_read_buffer t (Some x)) frames
            in
            match Socket.get_socket_type t.socket with
            | PUB ->
                manage_subscription ();
                Lwt.return []
            | XPUB ->
                let+ _ = enqueue () in
                manage_subscription ();
                []
            | _ ->
                let+ _ = enqueue () in
                [])
        | CLOSED -> Lwt.return [ Close "Connection FSM error" ])

  let close t =
    let if_pair =
      match Socket.get_socket_type t.socket with PAIR -> true | _ -> false
    in
    if if_pair then Socket.set_pair_connected t.socket false;
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

module Connection_tcp (S : Tcpip.Stack.V4V6) = struct
  (* Start of helper functions *)

  (** Creates a tag for a TCP connection *)
  let tag_of_tcp_connection ipaddr port =
    String.concat "." [ "TCP"; ipaddr; string_of_int port ]

  (** Read input from flow, send the input to FSM and execute FSM actions *)
  let rec process_input flow connection =
    let* res = S.TCP.read flow in
    match res with
    | Ok `Eof ->
        if print_debug_logs then
          Logs.debug (fun f ->
              f "Module Connection_tcp: Closing connection EOF");
        ignore (Connection.fsm connection End_of_connection);
        Connection.close connection;
        Lwt.return_unit
    | Error e ->
        if print_debug_logs then
          Logs.warn (fun f ->
              f
                "Module Connection_tcp: Error reading data from established \
                 connection: %a"
                S.TCP.pp_error e);
        ignore (Connection.fsm connection End_of_connection);
        Connection.close connection;
        Lwt.return_unit
    | Ok (`Data b) ->
        if print_debug_logs then
          Logs.debug (fun f ->
              f "Module Connection_tcp: Read: %d bytes" (Cstruct.length b));
        let act () =
          let* actions = Connection.fsm connection (Input_data b) in
          let rec deal_with_action_list actions =
            match actions with
            | [] -> process_input flow connection
            | hd :: tl -> (
                match hd with
                | Connection.Write b -> (
                    if print_debug_logs then
                      Logs.debug (fun f ->
                          f
                            "Module Connection_tcp: Connection FSM Write %d \
                             bytes"
                            (Bytes.length b));
                    let* write_res = S.TCP.write flow (Cstruct.of_bytes b) in
                    match write_res with
                    | Error _ ->
                        if print_debug_logs then
                          Logs.warn (fun f ->
                              f
                                "Module Connection_tcp: Error writing data to \
                                 established connection.");
                        Lwt.return_unit
                    | Ok () -> deal_with_action_list tl)
                | Connection.Close s ->
                    if print_debug_logs then
                      Logs.debug (fun f ->
                          f
                            "Module Connection_tcp: Connection FSM Close due \
                             to: %s"
                            s);
                    Lwt.return_unit)
          in
          deal_with_action_list actions
        in
        act ()

  (** Check the 'mailbox' and send outgoing data / close connection *)
  let rec process_output flow connection =
    let* action = Lwt_stream.get (Connection.get_write_buffer connection) in
    match action with
    | None ->
        (* Stream closed *)
        if print_debug_logs then
          Logs.debug (fun f ->
              f "Module Connection_tcp: Connection was instructed to close");
        S.TCP.close flow
    | Some data -> (
        if print_debug_logs then
          Logs.debug (fun f ->
              f "Module Connection_tcp: Connection mailbox Write %d bytes"
                (Bytes.length data));
        let* write_res = S.TCP.write flow (Cstruct.of_bytes data) in
        match write_res with
        | Error _ ->
            if print_debug_logs then
              Logs.warn (fun f ->
                  f
                    "Module Connection_tcp: Error writing data to established \
                     connection.");
            let+ () = Connection.write_read_buffer connection None in
            Connection.close connection
        | Ok () -> process_output flow connection)

  let process_input flow connection =
    Lwt.catch
      (fun () -> process_input flow connection)
      (fun exn ->
        (* TODO: close *)
        Logs.err (fun f -> f "Exception: %a" Fmt.exn exn);
        Lwt.fail exn)

  let process_output flow connection =
    Lwt.catch
      (fun () -> process_output flow connection)
      (fun exn ->
        (* TODO: close *)
        Logs.err (fun f -> f "Exception: %a" Fmt.exn exn);
        Lwt.fail exn)

  (* End of helper functions *)

  let start_connection flow connection =
    let* write_res =
      S.TCP.write flow
        (Cstruct.of_bytes (Connection.greeting_message connection))
    in
    match write_res with
    | Error _ ->
        if print_debug_logs then
          Logs.warn (fun f ->
              f
                "Module Connection_tcp: Error writing data to established \
                 connection.");
        Connection.write_read_buffer connection None
    | Ok () -> process_input flow connection

  let listen s port socket =
    S.TCP.listen (S.tcp s) ~port (fun flow ->
        Logs.info (fun f -> f "client");
        let dst, dst_port = S.TCP.dst flow in
        if print_debug_logs then
          Logs.debug (fun f ->
              f
                "Module Connection_tcp: New tcp connection from IP %s on port \
                 %d"
                (Ipaddr.to_string dst) dst_port);
        let connection =
          Connection.init socket
            (Security_mechanism.init
               (Socket.get_security_data socket)
               (Socket.get_metadata socket))
            (tag_of_tcp_connection (Ipaddr.to_string dst) dst_port)
        in
        if
          Socket.if_has_outgoing_queue
            (Socket.get_socket_type (Connection.get_socket connection))
        then (
          Socket.add_connection socket connection;
          Lwt.join
            [ start_connection flow connection; process_output flow connection ])
        else (
          Socket.add_connection socket connection;
          start_connection flow connection));
    S.listen s

  let rec connect s addr port connection =
    let ipaddr = Ipaddr.of_string_exn addr in
    let* flow = S.TCP.create_connection (S.tcp s) (ipaddr, port) in
    match flow with
    | Ok flow ->
        let socket_type =
          Socket.get_socket_type (Connection.get_socket connection)
        in
        if Socket.if_has_outgoing_queue socket_type then
          Lwt.async (fun () ->
              Lwt.join
                [
                  start_connection flow connection;
                  process_output flow connection;
                ])
        else Lwt.async (fun () -> start_connection flow connection);
        let rec wait_until_traffic () =
          if Connection.get_stage connection <> TRAFFIC then
            let* () = Lwt.pause () in
            wait_until_traffic ()
          else Lwt.return_unit
        in
        wait_until_traffic ()
    | Error e ->
        if print_debug_logs then
          Logs.warn (fun f ->
              f
                "Module Connection_tcp: Error establishing connection: %a, \
                 retrying"
                S.TCP.pp_error e);
        connect s addr port connection
end

module Socket_tcp (S : Tcpip.Stack.V4V6) : sig
  type t

  val create_socket : Context.t -> ?mechanism:Security_mechanism.mechanism_type -> socket_type -> t
  (** Create a socket from the given context, mechanism and type *)

  val set_plain_credentials : t -> string -> string -> unit
  (** Set username and password for PLAIN client *)

  val set_plain_user_list : t -> (string * string) list -> unit
  (** Set password list for PLAIN server *)

  val set_identity : t -> string -> unit
  (** Set identity string of a socket if applicable *)

  val set_incoming_queue_size : t -> int -> unit
  (** Set the maximum capacity of the incoming queue *)

  val set_outgoing_queue_size : t -> int -> unit
  (** Set the maximum capacity of the outgoing queue *)

  val subscribe : t -> string -> unit
  val unsubscribe : t -> string -> unit

  val recv : t -> message_type Lwt.t
  (** Receive a msg from the underlying connections, according to the  semantics of the socket type *)

  val send : t -> message_type -> unit Lwt.t
  (** Send a msg to the underlying connections, according to the semantics of the socket type *)

  val send_blocking : t -> message_type -> unit Lwt.t
  (** Send a msg to the underlying connections. It blocks until a peer is available *)

  val bind : t -> int -> S.t -> unit
  (** Bind a local TCP port to the socket so the socket will accept incoming connections *)

  val connect : t -> string -> int -> S.t -> unit Lwt.t
  (** Bind a connection to a remote TCP port to the socket *)
end = struct
  (* type transport_info = Tcp of string * int
 *)
  type t = { socket : Socket.t }

  let create_socket context ?(mechanism = Security_mechanism.NULL) socket_type =
    { socket = Socket.create_socket context ~mechanism socket_type }

  let set_plain_credentials t username password =
    Socket.set_plain_credentials t.socket username password

  let set_plain_user_list t list = Socket.set_plain_user_list t.socket list
  let set_identity t identity = Socket.set_identity t.socket identity

  let set_incoming_queue_size t size =
    Socket.set_incoming_queue_size t.socket size

  let set_outgoing_queue_size t size =
    Socket.set_outgoing_queue_size t.socket size

  let subscribe t subscription = Socket.subscribe t.socket subscription
  let unsubscribe t subscription = Socket.unsubscribe t.socket subscription
  let recv t = Socket.recv t.socket
  let send t msg = Socket.send t.socket msg
  let send_blocking t msg = Socket.send_blocking t.socket msg

  let bind t port s =
    let module C_tcp = Connection_tcp (S) in
    Lwt.async (fun () -> C_tcp.listen s port t.socket)

  let connect t ipaddr port s =
    let module C_tcp = Connection_tcp (S) in
    let connection =
      Connection.init t.socket
        (Security_mechanism.init
           (Socket.get_security_data t.socket)
           (Socket.get_metadata t.socket))
        (C_tcp.tag_of_tcp_connection ipaddr port)
    in
    Socket.add_connection t.socket connection;
    C_tcp.connect s ipaddr port connection
end
