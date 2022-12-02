type state = START | START_SERVER | START_CLIENT | WELCOME | INITIATE | OK

type action =
  | Write of Frame.t
  | Continue
  | Close
  | Received_property of string * string
  | Ok

type security_data =
  | Null
  | Plain_client of string * string
  | Plain_server of (string, string) Hashtbl.t

type mechanism_type = NULL | PLAIN
type socket_metadata = (string * string) list

type t = {
  mechanism_type : mechanism_type;
  socket_metadata : socket_metadata;
  state : state;
  data : security_data;
  as_server : bool;
  as_client : bool;
}

(* Start of helper functions *)

(** Makes an ERROR command *)
let error error_reason =
  Command.to_frame
    (Command.make ~name:"ERROR"
       ~data:
         (String.cat
            (String.make 1 (Char.chr (String.length error_reason)))
            error_reason))

(** Extracts metadata from a command *)
let rec extract_metadata s =
  let len = String.length s in
  if len = 0 then []
  else
    let name_length = Char.code (String.get s 0) in
    let name = String.sub s 1 name_length in
    let property_length =
      String.get_int32_be s (name_length + 1) |> Int32.to_int
    in
    let property = String.sub s (name_length + 1 + 4) property_length in
    (name, property)
    :: extract_metadata
         (String.sub s
            (name_length + 1 + 4 + property_length)
            (len - (name_length + 1 + 4 + property_length)))

(** Extracts username and password from HELLO *)
let extract_username_password s =
  let username_length = Char.code (String.get s 0) in
  let username = String.sub s 1 username_length in
  let password_length = Char.code (String.get s (1 + username_length)) in
  let password = String.sub s (2 + username_length) password_length in
  (username, password)

(** Makes a WELCOME command for a PLAIN server *)
let welcome = Command.to_frame (Command.make ~name:"WELCOME" ~data:"")

(** Makes a HELLO command for a PLAIN client *)
let hello username password =
  Command.to_frame
    (Command.make ~name:"HELLO"
       ~data:
         (String.concat ""
            [
              String.make 1 (Char.chr (String.length username));
              username;
              String.make 1 (Char.chr (String.length password));
              password;
            ]))

let string_of_metadata (name, property) =
  let name_length = String.length name in
  let property_length = String.length property in
  String.concat ""
    [
      String.make 1 (Char.chr name_length);
      name;
      Utils.int_to_network_order property_length 4 |> Bytes.unsafe_to_string;
      property;
    ]

(** Makes a new handshake for NULL mechanism *)
let new_handshake_null metadata =
  Command.to_frame
    (Command.make ~name:"READY"
       ~data:(List.map string_of_metadata metadata |> String.concat ""))

(** Makes a metadata command (command = "INITIATE"/"READY") for PLAIN mechanism *)
let metadata_command command metadata =
  Command.to_frame
    (Command.make ~name:command
       ~data:(List.map string_of_metadata metadata |> String.concat ""))

(* End of helper functions *)

let get_name_string t =
  match t.mechanism_type with NULL -> "NULL" | PLAIN -> "PLAIN"

let init security_data socket_metadata =
  match security_data with
  | Null ->
      {
        mechanism_type = NULL;
        socket_metadata;
        state = START;
        data = security_data;
        as_server = false;
        as_client = false;
      }
  | Plain_client _ ->
      {
        mechanism_type = PLAIN;
        socket_metadata;
        state = START_CLIENT;
        data = security_data;
        as_server = false;
        as_client = true;
      }
  | Plain_server _ ->
      {
        mechanism_type = PLAIN;
        socket_metadata;
        state = START_SERVER;
        data = security_data;
        as_server = true;
        as_client = false;
      }

let client_first_message t =
  match t.mechanism_type with
  | NULL -> invalid_arg "client_first_message NULL"
  | PLAIN ->
      if t.as_client then
        match t.data with
        | Plain_client (u, p) -> hello u p
        | _ -> invalid_arg "Security mechanism mismatch"
      else invalid_arg "client_first_message PLAIN"

let fsm t command =
  let name = Command.get_name command in
  let data = Command.get_data command in
  match t.mechanism_type with
  | NULL -> (
      match t.state with
      | START -> (
          match name with
          | "READY" ->
              ( { t with state = OK },
                List.map
                  (fun (name, property) -> Received_property (name, property))
                  (extract_metadata data)
                @ [ Ok ] )
          | "ERROR" ->
              ( { t with state = OK },
                [ Write (error "ERROR command received"); Close ] )
          | _ ->
              ( { t with state = OK },
                [ Write (error "unknown command received"); Close ] ))
      | _ -> assert false)
  | PLAIN -> (
      match t.state with
      | START_SERVER ->
          if name = "HELLO" then
            let username, password = extract_username_password data in
            match t.data with
            | Plain_client _ -> invalid_arg "Server data expected"
            | Plain_server hashtable -> (
                match Hashtbl.find_opt hashtable username with
                | Some valid_password ->
                    if valid_password = password then
                      ({ t with state = WELCOME }, [ Write welcome ])
                    else
                      ( { t with state = OK },
                        [ Write (error "Handshake error"); Close ] )
                | None ->
                    ( { t with state = OK },
                      [ Write (error "Handshake error"); Close ] ))
            | _ -> invalid_arg "Security type mismatch"
          else
            ({ t with state = OK }, [ Write (error "Handshake error"); Close ])
      | START_CLIENT ->
          if name = "WELCOME" then
            ( { t with state = INITIATE },
              [ Write (metadata_command "INITIATE" t.socket_metadata) ] )
          else invalid_arg "Wrong credentials"
      | WELCOME ->
          if name = "INITIATE" then
            ( { t with state = OK },
              List.map
                (fun (name, property) -> Received_property (name, property))
                (extract_metadata data)
              @ [ Write (metadata_command "READY" t.socket_metadata); Ok ] )
          else
            ({ t with state = OK }, [ Write (error "Handshake error"); Close ])
      | INITIATE ->
          if name = "READY" then
            ( { t with state = OK },
              List.map
                (fun (name, property) -> Received_property (name, property))
                (extract_metadata data)
              @ [ Ok ] )
          else
            ({ t with state = OK }, [ Write (error "Handshake error"); Close ])
      | _ -> assert false)

let get_as_server t = t.as_server
let get_as_client t = t.as_client
let send_command_after_greeting t = t.as_client || t.mechanism_type = NULL

let first_command t =
  if t.mechanism_type = NULL then new_handshake_null t.socket_metadata
  else if t.mechanism_type = PLAIN then client_first_message t
  else invalid_arg "This socket should not send the command first."
