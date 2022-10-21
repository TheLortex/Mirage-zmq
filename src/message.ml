
type t = { size : int; if_long : bool; if_more : bool; body : bytes }

let of_string ?(if_long = false) ?(if_more = false) msg =
  let length = String.length msg in
  if length > 255 && not if_long then
    invalid_arg "Must be long message"
  else { size = length; if_long; if_more; body = Bytes.of_string msg }

let list_of_string msg =
  let length = String.length msg in
  (* Assume Sys.max_string_length < max_int *)
  if length > 255 then
    (* Make a LONG message *)
    [ of_string msg ~if_long:true ~if_more:false ]
  else
    (* Make short messages *)
    [ of_string msg ~if_long:false ~if_more:false ]

let to_frame t = Frame.make_frame t.body ~if_more:t.if_more ~if_command:false
