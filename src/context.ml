let default_queue_size = 1000

type t = { mutable default_queue_size : int }

let create_context () = { default_queue_size }
let set_default_queue_size context size = context.default_queue_size <- size
let get_default_queue_size context = context.default_queue_size
(*
    (* TODO close all connections of sockets in the context *)
    let destroy_context (_t : t) = () *)
