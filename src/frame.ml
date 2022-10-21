type t = {
  flags : char;
  (* Known issue: size is limited by max_int; may not be able to reach 2^63-1 *)
  size : int;
  body : bytes;
}

let size_to_bytes size if_long =
  if not if_long then Bytes.make 1 (Char.chr size)
  else Utils.int_to_network_order size 8

let make_frame body ~if_more ~if_command =
  let f = ref 0 in
  let len = Bytes.length body in
  if if_more then f := !f + 1;
  if if_command then f := !f + 4;
  if len > 255 then f := !f + 2;
  { flags = Char.chr !f; size = len; body }

let to_bytes t =
  Bytes.concat Bytes.empty
    [
      Bytes.make 1 t.flags;
      size_to_bytes t.size (Char.code t.flags land 2 = 2);
      t.body;
    ]

let parser =
  let open Angstrom in
  let ( let* ) a f = bind a ~f in
  let ( let+ ) a f = map a ~f in
  let* flag = any_uint8 in
  let if_long = flag land 2 = 2 in
  let* content_length =
    if if_long then
      let+ content_length = BE.any_int64 in
      Int64.to_int content_length
    else any_uint8
  in
  let+ body = take content_length in
  {
    flags = Char.chr flag;
    size = content_length;
    body = Bytes.unsafe_of_string body;
  }

let get_if_more t = Char.code t.flags land 1 = 1
let get_if_long t = Char.code t.flags land 2 = 2
let get_if_command t = Char.code t.flags land 4 = 4
let get_body t = t.body
let is_delimiter_frame t = get_if_more t && t.size = 0
let delimiter_frame = { flags = Char.chr 1; size = 0; body = Bytes.empty }

let splice_message_frames list =
  List.rev_map get_body list |> List.rev |> Bytes.concat Bytes.empty
  |> Bytes.unsafe_to_string
