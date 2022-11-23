open Socket_type.T

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
  connections : Raw_connection.t Queue.t;
  connections_condition : unit Lwt_condition.t;
  socket_type : ('s, 'a) Socket_type.t;
  mutable socket_states : ('s, 'a) socket_state;
  mutable incoming_queue_size : int option;
  mutable outgoing_queue_size : int option;
}

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
