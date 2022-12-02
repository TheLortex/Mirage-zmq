open Lwt.Syntax

type 'a t = {
  mutable max_size : int;
  mutable closed : string option;
  data : 'a Queue.t;
  wait : unit Lwt_condition.t;
}

type 'a or_closed = ('a, [ `Closed of string ]) result

let v ?(max_size = max_int) () =
  {
    max_size;
    data = Queue.create ();
    wait = Lwt_condition.create ();
    closed = None;
  }

let is_full t = Queue.length t.data >= t.max_size

let push_or_drop { max_size; data; wait; closed } v =
  if Option.is_some closed then raise End_of_file
  else if Queue.length data >= max_size then (* drop message *)
    ()
  else (
    Queue.add v data;
    Lwt_condition.broadcast wait ())

let rec push ({ max_size; data; wait; closed } as t) v =
  if Option.is_some closed then raise End_of_file
  else if Queue.length data >= max_size then
    (* wait for pop *)
    let* () = Lwt_condition.wait wait in
    push t v
  else (
    Queue.add v data;
    Lwt_condition.broadcast wait ();
    Lwt.return_unit)

let rec pop t =
  match t.closed with
  | Some s -> Lwt.return (Error (`Closed s))
  | None -> (
      match Queue.pop t.data with
      | v -> Lwt.return (Ok v)
      | exception Queue.Empty ->
          let* () = Lwt_condition.wait t.wait in
          pop t)

let close t msg =
  if Option.is_some t.closed then invalid_arg "Pipe is already closed"
  else t.closed <- Some msg;
  Lwt_condition.broadcast t.wait ()
