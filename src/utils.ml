(** Convert a int to big-endian bytes of length n *)
let int_to_network_order n length =
  Bytes.init length (fun i ->
      Char.chr ((n lsr (8 * (length - i - 1))) land 255))

let cstruct_to_bigstringaf (cstruct : Cstruct.t) =
  Bigstringaf.sub cstruct.buffer ~off:cstruct.off ~len:cstruct.len

let rec consume_parser ?(res = []) state parser =
  match state with
  | Angstrom.Buffered.Done ({ buf; off; len }, data) ->
      let state =
        Angstrom.Buffered.(
          feed (parse parser) (`Bigstring (Bigstringaf.copy buf ~off ~len)))
      in
      consume_parser ~res:(data :: res) state parser
  | _ -> (List.rev res, state)
