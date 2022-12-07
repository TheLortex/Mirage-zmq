open Socket_type.T
open Lwt.Syntax

let src = Logs.Src.create "zmq.socket" ~doc:"ZeroMQ"

module Log = (val Logs.src_log src : Logs.LOG)

type identity_and_data = { identity : string; data : Message.t list }

module Sub_management = struct
  let subscription_msg content =
    Message.make (String.cat (String.make 1 (Char.chr 1)) content) ~more:false

  let unsubscription_msg content =
    Message.make (String.cat (String.make 1 (Char.chr 0)) content) ~more:false
end

type rep_state = {
  if_received : bool;
  last_received_connection_tag : string;
  address_envelope : Message.t list;
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

type conn_rep_state = Building of Message.t list | Ready of Message.t list

type 's connection_state =
  | CSXpub : sub_state -> xpub connection_state
  | CSPub : sub_state -> pub connection_state
  | CSRep : conn_rep_state -> rep connection_state
  | CSReq : req connection_state
  | CSDealer : dealer connection_state
  | CSRouter : router connection_state
  | CSSub : sub connection_state
  | CSXsub : xsub connection_state
  | CSPush : push connection_state
  | CSPull : pull connection_state
  | CSPair : pair connection_state

type frame_state =
  | Nothing
  | Frames of Message.t list
  | Message of Message.t list

type 's connection = {
  conn : 's Raw_connection.t;
  mutable conn_state : 's connection_state;
  mutable frame_state : frame_state;
  cond : unit Lwt_condition.t;
}

type (!'s, 'a) t = {
  mutable metadata : Security_mechanism.socket_metadata;
  security_mechanism : Security_mechanism.mechanism_type;
  mutable security_info : Security_mechanism.security_data;
  mutable connections : 's connection List.t;
  connections_condition : unit Lwt_condition.t;
  socket_type : ('s, 'a) Socket_type.t;
  mutable socket_states : ('s, 'a) socket_state;
  mutable incoming_queue_size : int option;
  mutable outgoing_queue_size : int option;
}

let security_info t = t.security_info
let metadata t = t.metadata
let typ' t = t.socket_type

let set_plain_credentials t name password =
  if t.security_mechanism = PLAIN then
    t.security_info <- Plain_client (name, password)
  else invalid_arg "not in PLAIN mode"

let set_plain_user_list t list =
  if t.security_mechanism = PLAIN then (
    let hashtable = Hashtbl.create (List.length list) in
    List.iter
      (fun (username, password) -> Hashtbl.add hashtable username password)
      list;
    t.security_info <- Plain_server hashtable)
  else invalid_arg "not in PLAIN mode"

let set_identity t identity =
  let set (name, value) =
    if name = "Identity" then (name, identity) else (name, value)
  in
  if List.fold_left (fun b (name, _) -> b || name = "Identity") false t.metadata
  then t.metadata <- List.map set t.metadata
  else t.metadata <- t.metadata @ [ ("Identity", identity) ]

let set_incoming_queue_size t size = t.incoming_queue_size <- Some size
let set_outgoing_queue_size t size = t.outgoing_queue_size <- Some size

(* END SETTERS *)

let create_socket context ?(mechanism = Security_mechanism.NULL) (type s r)
    (socket_type : (s, r) Socket_type.t) =
  match socket_type with
  | Rep ->
      ({
         socket_type;
         metadata = [ ("Socket-Type", "REP") ];
         security_mechanism = mechanism;
         security_info = Null;
         connections = [];
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
        connections = [];
        connections_condition = Lwt_condition.create ();
        socket_states = SReq { if_sent = false; last_sent_connection_tag = "" };
        incoming_queue_size = None;
        outgoing_queue_size = None;
      }
  | Dealer ->
      {
        socket_type;
        metadata = [ ("Socket-Type", "DEALER"); ("Identity", "") ];
        security_mechanism = mechanism;
        security_info = Null;
        connections = [];
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
        connections = [];
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
        connections = [];
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
        connections = [];
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
        connections = [];
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
        connections = [];
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
        connections = [];
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
        connections = [];
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
        connections = [];
        connections_condition = Lwt_condition.create ();
        socket_states = SPair { connected = false };
        incoming_queue_size = Some (Context.get_default_queue_size context);
        outgoing_queue_size = Some (Context.get_default_queue_size context);
      }

(* CONNECTION *)

let initial_traffic_messages (type a b) (t : (a, b) t) =
  match t.socket_states with
  | SSub { subscriptions } ->
      if not (Trie.is_empty subscriptions) then
        List.map Sub_management.subscription_msg (Trie.to_list subscriptions)
      else []
  | SXsub { subscriptions } ->
      if not (Trie.is_empty subscriptions) then
        List.map Sub_management.subscription_msg (Trie.to_list subscriptions)
      else []
  | _ -> []

type action = Subscribe | Unsubscribe | Ignore

let match_subscription_signature (msg : Message.t) =
  if not msg.more then
    let first_char = msg.content.[0] in
    if first_char = Char.chr 1 then Subscribe
    else if first_char = Char.chr 0 then Unsubscribe
    else Ignore
  else Ignore

let manage_subscription (type a) (t : a connection) msg =
  match t.conn_state with
  | CSPub s | CSXpub s -> (
      match match_subscription_signature msg with
      | Unsubscribe ->
          let body = msg.content in
          let sub = String.sub body 1 (String.length body - 1) in
          Trie.delete s.subscriptions sub
      | Subscribe ->
          let body = msg.content in
          let sub = String.sub body 1 (String.length body - 1) in
          Trie.insert s.subscriptions sub
      | Ignore -> ())
  | _ -> ()

let manage_frame_message v msg =
  let all_msgs =
    match v.frame_state with
    | Message _ -> assert false
    | Nothing -> [ msg ]
    | Frames msgs -> msg :: msgs
  in
  if msg.more then v.frame_state <- Frames all_msgs
  else (
    Log.info (fun f -> f "Message ready");
    v.frame_state <- Message (List.rev all_msgs);
    Lwt_condition.broadcast v.cond ())

let manage_rep lst v frame =
  if Message.is_delimiter frame then
    v.conn_state <- CSRep (Ready (List.rev lst))
  else v.conn_state <- CSRep (Building (frame :: lst))

let default_conn_state : type a s. (a, s) t -> a connection_state =
 fun t ->
  match t.socket_type with
  | Pub -> CSPub { subscriptions = Trie.create () }
  | Xpub -> CSXpub { subscriptions = Trie.create () }
  | Rep -> CSRep (Building [])
  | Req -> CSReq
  | Dealer -> CSDealer
  | Router -> CSRouter
  | Sub -> CSSub
  | Xsub -> CSXsub
  | Push -> CSPush
  | Pull -> CSPull
  | Pair -> CSPair

let handle_frame (type a s) (ty : (a, s) Socket_type.t) (v : a connection) frame
    =
  match (ty, v.conn_state) with
  | Pub, _ -> manage_subscription v frame
  | Xpub, _ ->
      manage_frame_message v frame;
      manage_subscription v frame
  | Rep, CSRep (Building lst) -> manage_rep lst v frame
  | _, _ -> manage_frame_message v frame

let remove_connection t connection =
  t.connections <-
    List.filter (fun { conn; _ } -> conn != connection) t.connections;
  Lwt_condition.broadcast t.connections_condition ()

let add_connection t connection =
  let* ready = Raw_connection.wait_until_ready connection in
  if not ready then (* cancelled *)
    Lwt.return_unit
  else
    let* () = Raw_connection.write connection (initial_traffic_messages t) in
    let v =
      {
        conn = connection;
        conn_state = default_conn_state t;
        cond = Lwt_condition.create ();
        frame_state = Nothing;
      }
    in
    t.connections <- v :: t.connections;
    Lwt_condition.broadcast t.connections_condition ();
    let exception Closed of string in
    let rec input_loop () =
      let* () =
        match v.frame_state with
        | Message _ -> Lwt_condition.wait v.cond
        | _ -> (
            let+ frame = Raw_connection.read connection in
            match frame with
            | Error (`Closed e) -> raise (Closed e)
            | Ok frame -> handle_frame t.socket_type v frame)
      in
      input_loop ()
    in
    Lwt.finalize
      (fun () ->
        Lwt.catch
          (fun () -> input_loop ())
          (function
            | Closed msg ->
                Log.info (fun f -> f "Socket: Connection closed: %s" msg);
                Lwt.return_unit
            | Lwt.Canceled ->
                Log.info (fun f -> f "Socket: Connection cancelled");
                Lwt.return_unit
            | e ->
                Log.err (fun f ->
                    f "Socket: Connection lost because of exception: %s"
                      (Printexc.to_string e));
                raise e))
      (fun () ->
        (match v.frame_state with
        | Message _ ->
            () (* do nothing, the message getter will take care of it *)
        | _ -> remove_connection t connection);
        Lwt.return_unit)

(* RECV *)

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

let rec receive_message t =
  match
    List.find_map
      (fun t ->
        match t.frame_state with
        | Message msg -> Some (t, msg, t.cond)
        | _ -> None)
      t.connections
  with
  | None ->
      (* wait for an event somewhere *)
      let* () =
        t.connections_condition :: List.map (fun t -> t.cond) t.connections
        |> List.map Lwt_condition.wait
        |> Lwt.choose
      in
      receive_message t
  | Some (c, msg, cond) ->
      if Raw_connection.is_closed c.conn then remove_connection t c.conn
      else (
        c.frame_state <- Nothing;
        Lwt_condition.broadcast cond ());
      Lwt.return (c, msg)

let rec receive_message_rep t =
  match
    List.find_map
      (fun t ->
        match t.frame_state with
        | Message msg -> Some (t, msg, t.cond, t.conn_state)
        | _ -> None)
      t.connections
  with
  | None ->
      (* wait for an event somewhere *)
      let* () =
        t.connections_condition :: List.map (fun t -> t.cond) t.connections
        |> List.map Lwt_condition.wait
        |> Lwt.choose
      in
      receive_message_rep t
  | Some (c, msg, cond, CSRep (Ready envelope)) ->
      if Raw_connection.is_closed c.conn then remove_connection t c.conn
      else (
        c.frame_state <- Nothing;
        c.conn_state <- CSRep (Building []);
        Lwt_condition.broadcast cond ());
      Lwt.return (c, envelope, msg)
  | _ -> assert false

(* TODO disconnect. *)
let rec receive_message_conn t c =
  match c.frame_state with
  | Message msgs ->
      if Raw_connection.is_closed c.conn then remove_connection t c.conn
      else (
        c.frame_state <- Nothing;
        Lwt_condition.broadcast c.cond ());
      Lwt.return msgs
  | _ ->
      let* () = Lwt_condition.wait c.cond in
      receive_message_conn t c

let recv_multipart : type a. (a, [> `Recv ]) t -> Message.t list Lwt.t =
 fun t ->
  let (Yes_recv (socket_state, socket_state_update)) = can_recv t in
  match socket_state with
  | SPull | SSub _ | SXpub | SXsub _ -> receive_message t |> Lwt.map snd
  | SRep _ ->
      let+ c, address_envelope, msg = receive_message_rep t in
      socket_state_update
        (SRep
           {
             if_received = true;
             last_received_connection_tag = Raw_connection.tag c.conn;
             address_envelope;
           });
      msg
  | SReq { if_sent; last_sent_connection_tag = tag } ->
      if not if_sent then invalid_arg "Need to send a request before receiving"
      else
        let c =
          List.find (fun c -> Raw_connection.tag c.conn = tag) t.connections
        in
        let+ msg = receive_message_conn t c in
        socket_state_update
          (SReq { if_sent = false; last_sent_connection_tag = "" });
        msg
  | SDealer { request_order_queue } ->
      if Queue.is_empty request_order_queue then
        invalid_arg "Dealer: you need to send requests first!"
      else
        let tag = Queue.pop request_order_queue in
        (* what if connection disappeared ? *)
        let c =
          List.find (fun c -> Raw_connection.tag c.conn = tag) t.connections
        in
        receive_message_conn t c
  | SPair _ -> (
      match t.connections with
      | [] -> raise Not_found
      | _ :: _ :: _ -> assert false
      | [ c ] -> receive_message_conn t c)

let recv s =
  let+ msgs = recv_multipart s in
  Message.merge msgs

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

let recv_from : type a. (a, [> `Recv_from ]) t -> identity_and_data Lwt.t =
 fun t ->
  let (Yes_recv_from SRouter) = can_recv_from t in
  let+ c, msg = receive_message t in
  { identity = Raw_connection.incoming_identity c.conn; data = msg }

(* SEND *)

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

let find_available_connection connections =
  List.find_opt
    (fun c ->
      Raw_connection.is_ready c.conn
      && not (Raw_connection.is_send_queue_full c.conn))
    connections

let send_multipart : type a. (a, [> `Send ]) t -> Message.t list -> unit Lwt.t =
 fun t msgs ->
  let (Yes_send (socket_state, socket_state_update)) = can_send t in
  assert (List.length msgs > 0);
  match socket_state with
  | SPush -> (
      match find_available_connection t.connections with
      | None -> raise Not_found
      | Some c -> Raw_connection.write c.conn msgs)
  | SPub ->
      Lwt_list.iter_s
        (fun connection ->
          let (CSPub { subscriptions }) = connection.conn_state in
          if Trie.find subscriptions (List.hd msgs).content then
            Raw_connection.write connection.conn msgs
          else Lwt.return_unit)
        t.connections
  | SXpub ->
      Lwt_list.iter_s
        (fun connection ->
          let (CSXpub { subscriptions }) = connection.conn_state in
          if Trie.find subscriptions (List.hd msgs).content then
            Raw_connection.write connection.conn msgs
          else Lwt.return_unit)
        t.connections
  | SXsub _ ->
      Lwt_list.iter_s
        (fun connection -> Raw_connection.write connection.conn msgs)
        t.connections
  | SRep { if_received; last_received_connection_tag = tag; address_envelope }
    ->
      if not if_received then
        invalid_arg "Need to receive a request before sending a message"
      else
        let c =
          List.find (fun c -> Raw_connection.tag c.conn = tag) t.connections
        in
        let* () =
          Raw_connection.write c.conn
            (address_envelope @ (Message.delimiter :: msgs))
        in
        socket_state_update
          (SRep
             {
               if_received = false;
               last_received_connection_tag = "";
               address_envelope = [];
             });
        Lwt.return_unit
  | SReq { if_sent; last_sent_connection_tag = _ } -> (
      if if_sent then
        invalid_arg "Need to receive a reply before sending another message"
      else
        match find_available_connection t.connections with
        | None -> raise Not_found
        | Some c ->
            socket_state_update
              (SReq
                 {
                   if_sent = true;
                   last_sent_connection_tag = Raw_connection.tag c.conn;
                 });
            Raw_connection.write c.conn (Message.delimiter :: msgs))
  | SDealer { request_order_queue } -> (
      match find_available_connection t.connections with
      | None -> raise Not_found
      | Some c ->
          Queue.push (Raw_connection.tag c.conn) request_order_queue;
          Raw_connection.write c.conn (Message.delimiter :: msgs))
  | SPair _ -> failwith "unimplemented"

let send t msg =
  let msgs = [ Message.make msg ~more:false ] in
  send_multipart t msgs

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

let send_to : type a. (a, [> `Send_to ]) t -> identity_and_data -> unit Lwt.t =
 fun t { identity; data } ->
  let (Yes_send_to SRouter) = can_send_to t in
  let c =
    List.find
      (fun c -> Raw_connection.incoming_identity c.conn = identity)
      t.connections
  in
  let msgs =
    match Raw_connection.incoming_socket_type c.conn with
    | Socket_type.U Req -> Message.delimiter :: data
    | _ -> data
  in
  Raw_connection.write c.conn msgs

let send_blocking _t = failwith "unimplemented"

(* SUBSCRIBE *)

type ('a, 'b) can_subscribe =
  | Yes_subscribe : ('a, [ `Sub ]) socket_state -> ('a, [> `Sub ]) can_subscribe
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

let send_message_to_all_active_connections connections frame =
  List.iter
    (fun t ->
      if Raw_connection.is_ready t.conn then
        Lwt.dont_wait (fun () -> Raw_connection.write t.conn [ frame ]) raise)
    connections

let subscribe (type a) (t : (a, [> `Sub ]) t) subscription =
  let (Yes_subscribe socket_state) = can_subscribe t in
  match socket_state with
  | SSub { subscriptions } ->
      Trie.insert subscriptions subscription;
      send_message_to_all_active_connections t.connections
        (Sub_management.subscription_msg subscription)
  | SXsub { subscriptions } ->
      Trie.insert subscriptions subscription;
      send_message_to_all_active_connections t.connections
        (Sub_management.subscription_msg subscription)

let unsubscribe (type a) (t : (a, [> `Sub ]) t) subscription =
  let (Yes_subscribe socket_state) = can_subscribe t in
  match socket_state with
  | SSub { subscriptions } ->
      Trie.delete subscriptions subscription;
      send_message_to_all_active_connections t.connections
        (Sub_management.unsubscription_msg subscription)
  | SXsub { subscriptions } ->
      Trie.delete subscriptions subscription;
      send_message_to_all_active_connections t.connections
        (Sub_management.unsubscription_msg subscription)
