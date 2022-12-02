module T = struct
  type req = private TReq
  type rep = private TRep
  type dealer = private TDealer
  type router = private TRouter
  type pub = private TPub
  type sub = private TSub
  type xpub = private TXpub
  type xsub = private TXsub
  type push = private TPush
  type pull = private TPull
  type pair = private TPair
end

open T

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

type any = U : _ t -> any

exception Socket_Name_Not_Recognised

(** Returns the socket type from string *)
let of_string : string -> any = function
  | "REQ" -> U Req
  | "REP" -> U Rep
  | "DEALER" -> U Dealer
  | "ROUTER" -> U Router
  | "PUB" -> U Pub
  | "XPUB" -> U Xpub
  | "SUB" -> U Sub
  | "XSUB" -> U Xsub
  | "PUSH" -> U Push
  | "PULL" -> U Pull
  | "PAIR" -> U Pair
  | _ -> raise Socket_Name_Not_Recognised

(** Checks if the pair is valid as specified by 23/ZMTP *)
let valid_socket_pair (type a b e f) (a : (a, b) t) (b : (e, f) t) =
  match (U a, U b) with
  | U Req, U Rep
  | U Req, U Router
  | U Rep, U Req
  | U Rep, U Dealer
  | U Dealer, U Rep
  | U Dealer, U Dealer
  | U Dealer, U Router
  | U Router, U Req
  | U Router, U Dealer
  | U Router, U Router
  | U Pub, U Sub
  | U Pub, U Xsub
  | U Xpub, U Sub
  | U Xpub, U Xsub
  | U Sub, U Pub
  | U Sub, U Xpub
  | U Xsub, U Pub
  | U Xsub, U Xpub
  | U Push, U Pull
  | U Pull, U Push
  | U Pair, U Pair ->
      true
  | _ -> false

(** Whether the socket type has an outgoing queue *)
let has_outgoing_queue socket =
  match U socket with
  | U Rep
  | U Req
  | U Dealer
  | U Router
  | U Pub
  | U Sub
  | U Xpub
  | U Xsub
  | U Push
  | U Pair ->
      true
  | U Pull -> false
