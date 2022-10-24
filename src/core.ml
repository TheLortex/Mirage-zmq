type send = TS
type recv = TR
type all = TA

type ('s, 'p) typ =
  | Send : (send, ([< `Send ] as 'p)) typ
  | Recv : (recv, ([< `Recv ] as 'p)) typ
  | All : (all, ([< `Send | `Recv ] as 'p)) typ

module type S = sig
  type ('s, 'p) t

  val make : ('s, 'p) typ -> ('s, 'p) t
  val send : ('s, [> `Send ]) t -> string -> unit
  val recv : ('s, [> `Recv ]) t -> string
  val typ : ('s, 'p) t -> ('s, 'p) typ
end

module T : S = struct
  type ('s, 'p) handler =
    | HSend : (send, [< `Send ]) handler
    | HRecv : (recv, [< `Recv ]) handler
    | HAll : (all, [< `Send | `Recv ]) handler

  type (!'a, !'p) t = { h : ('a, 'p) handler; typ : ('a, 'p) typ }

  let make : type s p. (s, p) typ -> (s, p) t = function
    | Send -> { h = HSend; typ = Send }
    | Recv -> { h = HRecv; typ = Recv }
    | All -> { h = HAll; typ = All }

  type 'a sendable = [> `Send ] as 'a

  let send : type s. (s, [> `Send ]) t -> string -> unit =
   fun t msg -> match t.h with HSend -> ignore msg | HAll -> ignore msg

  let send : type s. (s, [> `Send ]) t -> string -> unit =
   fun t msg -> send (Obj.magic t) msg

  let recv : type s. (s, [> `Recv ]) t -> string =
   fun t -> match t.h with HRecv -> "ignore msg " | HAll -> "ignore msg"

  let recv : type s. (s, [> `Recv ]) t -> string = fun t -> recv (Obj.magic t)
  let typ t = t.typ
end

module V (S : S) = struct
  let recv () =
    let a = S.make All in
    S.send a "bonjour";
    S.recv a

  let recv_or_default : type a b. (a, b) S.t -> string =
   fun v -> match S.typ v with Send -> "" | Recv -> S.recv v | All -> S.recv v
end

(* type 'a a = [> `A ] as 'a
   type 'a b = [> `B ] as 'a
   type 'a c = [> `C ] as 'a
   type !'a typ = A : 'a a typ | B : 'a b typ | C : 'a c typ
   type !'a clo = CA : [ `A ] clo | CB : [ `B ] clo | CC : [ `C ] clo

   type !'a sta =
     | SA : unit -> 'a a sta
     | SB : int -> 'a b sta
     | SC : int -> 'a c sta

   type !'a state = { sta : 'a sta }
   type intable = [ `B | `C ]

   let int : intable state -> int =
    fun t -> match t.sta with SB n -> n | SC n -> n

   type !'a ok = 'a constraint 'a = [< `A | `B | `C ]

   let v : 'a ok -> 'a state = function
     | `B -> { sta = SB 12 }
     | `A -> { sta = SA () }
     | `C -> { sta = SC 13 }

   let ty : type a. a state -> a typ =
    fun t ->
     match t.sta with
     | SB _ -> B
     | SA _ -> A
     | SC _ -> C

   let int : type a. a state -> int =
    fun v ->
     match ty v with
     | A -> 0
     | B -> int (v :> [ `B ] state)
     | C -> int v
*)
