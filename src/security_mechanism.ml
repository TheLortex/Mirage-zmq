type state = START | START_SERVER | START_CLIENT | WELCOME | INITIATE | OK

type action =
  | Write of bytes
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
  Frame.to_bytes
    (Command.to_frame
       (Command.make_command "ERROR"
          (Bytes.cat
             (Bytes.make 1 (Char.chr (String.length error_reason)))
             (Bytes.of_string error_reason))))

(** Extracts metadata from a command *)
let rec extract_metadata bytes =
  let len = Bytes.length bytes in
  if len = 0 then []
  else
    let name_length = Char.code (Bytes.get bytes 0) in
    let name = Bytes.sub_string bytes 1 name_length in
    let property_length =
      Bytes.get_int32_be bytes (name_length + 1) |> Int32.to_int
    in
    let property =
      Bytes.sub_string bytes (name_length + 1 + 4) property_length
    in
    (name, property)
    :: extract_metadata
         (Bytes.sub bytes
            (name_length + 1 + 4 + property_length)
            (len - (name_length + 1 + 4 + property_length)))

(** Extracts username and password from HELLO *)
let extract_username_password bytes =
  let username_length = Char.code (Bytes.get bytes 0) in
  let username = Bytes.sub bytes 1 username_length in
  let password_length = Char.code (Bytes.get bytes (1 + username_length)) in
  let password = Bytes.sub bytes (2 + username_length) password_length in
  (username, password)

(** Makes a WELCOME command for a PLAIN server *)
let welcome =
  Frame.to_bytes (Command.to_frame (Command.make_command "WELCOME" Bytes.empty))

(** Makes a HELLO command for a PLAIN client *)
let hello username password =
  Frame.to_bytes
    (Command.to_frame
       (Command.make_command "HELLO"
          (Bytes.concat Bytes.empty
             [
               Bytes.make 1 (Char.chr (String.length username));
               Bytes.of_string username;
               Bytes.make 1 (Char.chr (String.length password));
               Bytes.of_string password;
             ])))

(** Makes a new handshake for NULL mechanism *)
let new_handshake_null metadata =
  let bytes_of_metadata (name, property) =
    let name_length = String.length name in
    let property_length = String.length property in
    Bytes.cat
      (Bytes.cat (Bytes.make 1 (Char.chr name_length)) (Bytes.of_string name))
      (Bytes.cat
         (Utils.int_to_network_order property_length 4)
         (Bytes.of_string property))
  in
  let rec convert_metadata = function
    | [] -> Bytes.empty
    | hd :: tl -> Bytes.cat (bytes_of_metadata hd) (convert_metadata tl)
  in
  Frame.to_bytes
    (Command.to_frame
       (Command.make_command "READY" (convert_metadata metadata)))

(** Makes a metadata command (command = "INITIATE"/"READY") for PLAIN mechanism *)
let metadata_command command metadata =
  let bytes_of_metadata (name, property) =
    let name_length = String.length name in
    let property_length = String.length property in
    Bytes.cat
      (Bytes.cat (Bytes.make 1 (Char.chr name_length)) (Bytes.of_string name))
      (Bytes.cat
         (Utils.int_to_network_order property_length 4)
         (Bytes.of_string property))
  in
  let rec convert_metadata = function
    | [] -> Bytes.empty
    | hd :: tl -> Bytes.cat (bytes_of_metadata hd) (convert_metadata tl)
  in
  Frame.to_bytes
    (Command.to_frame
       (Command.make_command command (convert_metadata metadata)))

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
  | NULL -> Bytes.empty
  | PLAIN ->
      if t.as_client then
        match t.data with
        | Plain_client (u, p) -> hello u p
        | _ -> invalid_arg "Security mechanism mismatch"
      else Bytes.empty

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
                match Hashtbl.find_opt hashtable (Bytes.to_string username) with
                | Some valid_password ->
                    if valid_password = Bytes.to_string password then
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
let if_send_command_after_greeting t = t.as_client || t.mechanism_type = NULL

let first_command t =
  if t.mechanism_type = NULL then new_handshake_null t.socket_metadata
  else if t.mechanism_type = PLAIN then client_first_message t
  else invalid_arg "This socket should not send the command first."
