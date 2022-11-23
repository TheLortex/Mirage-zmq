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
  | SRouter : (router, [< `Send | `Recv ]) socket_state
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
  connections : Raw_connection.t Lwt_dllist.t;
  connections_condition : unit Lwt_condition.t;
  socket_type : ('s, 'a) Socket_type.t;
  mutable socket_states : ('s, 'a) socket_state;
  mutable incoming_queue_size : int option;
  mutable outgoing_queue_size : int option;
}
