type state =
  | START
  | SIGNATURE
  | VERSION_MAJOR
  | VERSION_MINOR
  | MECHANISM
  | AS_SERVER
  | SUCCESS
  | ERROR

type t = { security : Security_mechanism.t; state : state }

type event =
  | Recv_sig of bytes
  | Recv_Vmajor of bytes
  | Recv_Vminor of bytes
  | Recv_Mechanism of bytes
  | Recv_as_server of bytes
  | Recv_filler
  | Init of string

type action =
  | Check_mechanism of string
  | Set_server of bool
  | Continue
  | Ok
  | Error of string

type version = { major : bytes; minor : bytes }
(*
 type greeting =
   { signature: bytes
   ; version: version
   ; mechanism: string
   ; as_server: bool
   ; filler: bytes }
*)
(* Start of helper functions *)

(** Make the signature bytes *)
let signature =
  let s = Bytes.make 10 (Char.chr 0) in
  Bytes.set s 0 (Char.chr 255);
  Bytes.set s 9 (Char.chr 127);
  s

(** The default version of this implementation is 3.0 (RFC 23/ZMTP) *)
let version =
  { major = Bytes.make 1 (Char.chr 3); minor = Bytes.make 1 (Char.chr 0) }

(** Pad the mechanism string to 20 bytes *)
let pad_mechanism m =
  let b = Bytes.of_string m in
  if Bytes.length b < 20 then
    Bytes.cat b (Bytes.make (20 - Bytes.length b) (Char.chr 0))
  else b

(** Get the actual mechanism from null padded string *)
let trim_mechanism m =
  let len = ref (Bytes.length m) in
  while Bytes.get m (!len - 1) = Char.chr 0 do
    len := !len - 1
  done;
  Bytes.sub m 0 !len

(** Makes the filler bytes *)
let filler = Bytes.make 31 (Char.chr 0)

(** Generates a new greeting *)
let new_greeting security =
  Bytes.concat Bytes.empty
    [
      signature;
      version.major;
      version.minor;
      pad_mechanism (Security_mechanism.get_name_string security);
      (if Security_mechanism.get_as_server security then
       Bytes.make 1 (Char.chr 1)
      else Bytes.make 1 (Char.chr 0));
      filler;
    ]

(* End of helper functions *)

let init security_t = { security = security_t; state = START }

let fsm_single t event =
  match (t.state, event) with
  | START, Recv_sig b ->
      if Bytes.get b 0 = Char.chr 255 && Bytes.get b 9 = Char.chr 127 then
        ({ t with state = SIGNATURE }, Continue)
      else ({ t with state = ERROR }, Error "Protocol Signature not detected.")
  | SIGNATURE, Recv_Vmajor b ->
      if Bytes.get b 0 = Char.chr 3 then
        ({ t with state = VERSION_MAJOR }, Continue)
      else ({ t with state = ERROR }, Error "Version-major is not 3.")
  | VERSION_MAJOR, Recv_Vminor _b -> ({ t with state = VERSION_MINOR }, Continue)
  | VERSION_MINOR, Recv_Mechanism b ->
      ( { t with state = MECHANISM },
        Check_mechanism (Bytes.to_string (trim_mechanism b)) )
  | MECHANISM, Recv_as_server b ->
      if Bytes.get b 0 = Char.chr 0 then
        ({ t with state = AS_SERVER }, Set_server false)
      else ({ t with state = AS_SERVER }, Set_server true)
  | AS_SERVER, Recv_filler -> ({ t with state = SUCCESS }, Ok)
  | _ -> ({ t with state = ERROR }, Error "Unexpected event.")

let fsm t event_list =
  let rec fsm_accumulator t event_list action_list =
    match event_list with
    | [] -> (
        match t.state with
        | ERROR -> ({ t with state = ERROR }, [ List.hd action_list ])
        | _ -> (t, List.rev action_list))
    | hd :: tl -> (
        match t.state with
        | ERROR -> ({ t with state = ERROR }, [ List.hd action_list ])
        | _ ->
            let new_state, action = fsm_single t hd in
            fsm_accumulator new_state tl (action :: action_list))
  in
  fsm_accumulator t event_list []
