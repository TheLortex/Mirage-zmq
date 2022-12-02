type t = { content : string; more : bool }

let make content ~more = { content; more }
let to_frame t = Frame.make t.content ~more:t.more ~command:false

let of_frame frame =
  let content = Frame.get_body frame in
  let more = Frame.is_more frame in
  assert (not (Frame.is_command frame));
  { content; more }

let is_delimiter t = String.length t.content = 0
let delimiter = make "" ~more:true
let merge lst = List.map (fun t -> t.content) lst |> String.concat ""