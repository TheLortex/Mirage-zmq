type t = {
  flags : int;
  (* Known issue: size is limited by max_int; may not be able to reach 2^63-1 *)
  size : int;
  body : string;
}

let is_more t = t.flags land 1 = 1
let is_long t = t.flags land 2 = 2
let is_command t = t.flags land 4 = 4

let make body ~more ~command =
  let f = ref 0 in
  let len = String.length body in
  if more then f := !f + 1;
  if command then f := !f + 4;
  if len > 255 then f := !f + 2;
  { flags = !f; size = len; body }

let serialize f t =
  let open Faraday in
  write_uint8 f t.flags;
  if t.flags land 2 = 2 then (* long *)
    BE.write_uint64 f (Int64.of_int t.size)
  else write_uint8 f t.size;
  write_string f t.body

let src = Logs.Src.create "zmq.frame.parser" ~doc:"ZeroMQ Frame Parser"

module Log = (val Logs.src_log src : Logs.LOG)

let parser =
  let open Angstrom in
  let ( let* ) a f = bind a ~f in
  let ( let+ ) a f = map a ~f in
  let* flags = any_uint8 in
  let if_long = flags land 2 = 2 in
  let* content_length =
    if if_long then
      let+ content_length = BE.any_int64 in
      Int64.to_int content_length
    else any_uint8
  in
  Log.debug (fun f -> f "Frame length: %d" content_length);
  let+ body = take content_length in
  { flags; size = content_length; body }

let get_body t = t.body
let is_delimiter_frame t = is_more t && t.size = 0
let delimiter_frame = { flags = 1; size = 0; body = "" }
