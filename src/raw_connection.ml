open Lwt.Syntax

let src = Logs.Src.create "zmq.raw_connection" ~doc:"ZeroMQ"

module Log = (val Logs.src_log src : Logs.LOG)

module type S = sig
  type 'a t

  val wait_until_ready : _ t -> bool Lwt.t
  val is_ready : _ t -> bool
  val write : _ t -> Frame.t list -> unit Lwt.t
  val read : _ t -> Frame.t Pipe.or_closed Lwt.t

  (* notify Eof from network *)
  val close_input : _ t -> unit

  (* notify Eof to network *)
  val close_output : _ t -> unit
end

type greeting_stage = G0 | G1 of { incoming_as_server : bool }

type handshake_stage =
  | H0
  | H1 : { incoming_socket_type : ('b, 'c) Socket_type.t } -> handshake_stage
  | H2 : { incoming_identity : string } -> handshake_stage
  | H3 : {
      incoming_socket_type : ('b, 'c) Socket_type.t;
      incoming_identity : string;
    }
      -> handshake_stage

type connection_stage =
  | Greeting of { state : Greeting.t; stage : greeting_stage }
  | Handshake of {
      state : Security_mechanism.t;
      stage : handshake_stage;
      incoming_as_server : bool;
    }
  | Traffic : {
      incoming_as_server : bool;
      incoming_socket_type : ('b, 'c) Socket_type.t;
      incoming_identity : string;
    }
      -> connection_stage
  | Close

type action = Data of Cstruct.t | Close of string

type 's t =
  | St : {
      tag : string;
      socket_type : ('s, 'a) Socket_type.t;
      security_mechanism : Security_mechanism.t;
      mutable stage : connection_stage;
      ready_condition : unit Lwt_condition.t;
      read_frame : Frame.t Pipe.t;
      send_buffer : Cstruct.t list Pipe.t;
      mutable fragments_parser : Frame.t Angstrom.Buffered.state;
    }
      -> 's t

let init socket_type ?incoming_queue_size ?outgoing_queue_size
    security_mechanism tag =
  let read_frame =
    match incoming_queue_size with
    | None -> Pipe.v ()
    | Some v -> Pipe.v ~max_size:v ()
  in
  let send_buffer =
    match outgoing_queue_size with
    | None -> Pipe.v ()
    | Some v -> Pipe.v ~max_size:v ()
  in
  let greeting_init = Greeting.init security_mechanism in
  Pipe.push_or_drop send_buffer
    [ Greeting.new_greeting security_mechanism |> Cstruct.of_bytes ];
  St
    {
      tag;
      socket_type;
      security_mechanism;
      stage = Greeting { state = greeting_init; stage = G0 };
      ready_condition = Lwt_condition.create ();
      read_frame;
      send_buffer;
      fragments_parser = Angstrom.Buffered.parse Frame.parser;
    }

let handle_greeting_action (St t) action =
  match (t.stage, action) with
  | Greeting { state; stage = G0 }, Greeting.Set_server b ->
      t.stage <- Greeting { state; stage = G1 { incoming_as_server = b } };
      if b && Security_mechanism.get_as_server t.security_mechanism then
        Some (Error (`Closed "Both ends cannot be servers"))
      else None
  | Greeting _, Check_mechanism s ->
      if s <> Security_mechanism.get_name_string t.security_mechanism then
        Some (Error (`Closed "Security Policy mismatch"))
      else None
  | Greeting _, Continue -> None
  | Greeting { stage = G1 { incoming_as_server }; _ }, Ok ->
      Log.debug (fun f -> f "Module Connection: Greeting OK");
      t.stage <-
        Handshake
          { state = t.security_mechanism; stage = H0; incoming_as_server };
      if Security_mechanism.send_command_after_greeting t.security_mechanism
      then
        Some
          (Ok
             [
               Security_mechanism.first_command t.security_mechanism
               |> Frame.to_cstruct;
             ])
      else None
  | _ -> assert false

let handle_handshake_actions (St t) action =
  match (t.stage, action) with
  | Handshake _, Security_mechanism.Write frame ->
      Some (Ok [ Frame.to_cstruct frame ])
  | Handshake _, Continue -> None
  | ( Handshake
        {
          stage = H3 { incoming_identity; incoming_socket_type };
          incoming_as_server;
          _;
        },
      Ok ) ->
      (* todo: wake socket *)
      Log.debug (fun f -> f "Module Connection: Handshake OK");
      t.stage <-
        Traffic { incoming_identity; incoming_as_server; incoming_socket_type };
      Lwt_condition.broadcast t.ready_condition ();
      None
  | Handshake { stage = H1 { incoming_socket_type }; incoming_as_server; _ }, Ok
    ->
      (* todo: wake socket *)
      Log.debug (fun f -> f "Module Connection: Handshake OK");
      t.stage <-
        Traffic
          { incoming_identity = ""; incoming_as_server; incoming_socket_type };
      Lwt_condition.broadcast t.ready_condition ();
      None
  | Handshake _, Close -> Some (Error (`Closed "Handshake FSM error"))
  | ( Handshake ({ stage = H0 | H2 _; _ } as hs),
      Received_property ("Socket-Type", value) ) ->
      let socket_type = t.socket_type in
      let (U incoming_socket_type) = Socket_type.of_string value in
      if Socket_type.valid_socket_pair socket_type incoming_socket_type then (
        let stage =
          match hs.stage with
          | H0 -> H1 { incoming_socket_type }
          | H2 { incoming_identity } ->
              H3 { incoming_identity; incoming_socket_type }
          | _ -> assert false
        in
        t.stage <- Handshake { hs with stage };
        None)
      else None
  | ( Handshake ({ stage = H0 | H1 _; _ } as hs),
      Received_property ("Identity", value) ) ->
      let stage =
        match hs.stage with
        | H0 -> H2 { incoming_identity = value }
        | H1 { incoming_socket_type } ->
            H3 { incoming_identity = value; incoming_socket_type }
        | _ -> assert false
      in
      t.stage <- Handshake { hs with stage };
      None
  | Handshake _, Received_property (name, _) ->
      Log.debug (fun f ->
          f "Module Connection: Ignore unknown property %s" name);
      None
  | Handshake _, Ok ->
      Log.err (fun f ->
          f "Module Connection: Got OK but not ready (missing socket type)");
      assert false
  | _ -> assert false

let pipe_result_pair a b =
  match (a, b) with
  | Error (`Closed msg), Error (`Closed msg2) ->
      Error (`Closed (msg ^ "; " ^ msg2))
  | Error (`Closed msg), _ | _, Error (`Closed msg) -> Error (`Closed msg)
  | Ok a, Ok b -> Ok (a @ b)

let rec input_bytes (St t) bytes =
  match t.stage with
  | Greeting ({ state; _ } as greeting_state) ->
      Log.debug (fun f -> f "Module Connection: Greeting -> FSM");
      let next_state, actions, rest = Greeting.input state bytes in

      t.stage <- Greeting { greeting_state with state = next_state };
      let action =
        List.filter_map (handle_greeting_action (St t)) actions
        |> List.fold_left pipe_result_pair (Ok [])
      in
      if Cstruct.length rest > 0 then
        pipe_result_pair action (input_bytes (St t) rest)
      else action
  | Handshake ({ state; _ } as handshake_stage) -> (
      Log.debug (fun f -> f "Module Connection: Handshake -> FSM");
      t.fragments_parser <-
        Angstrom.Buffered.feed t.fragments_parser
          (`Bigstring (Utils.cstruct_to_bigstringaf bytes));
      match Utils.consume_parser t.fragments_parser Frame.parser with
      | [], state ->
          t.fragments_parser <- state;
          Ok []
      | [ frame ], parser_state ->
          t.fragments_parser <- parser_state;
          let command = Command.of_frame frame in
          let next_state, actions = Security_mechanism.fsm state command in
          t.stage <- Handshake { handshake_stage with state = next_state };
          List.filter_map (handle_handshake_actions (St t)) actions
          |> List.fold_left pipe_result_pair (Ok [])
      | _ -> assert false)
  | Traffic _ ->
      Log.debug (fun f -> f "Module Connection: TRAFFIC -> FSM");
      t.fragments_parser <-
        Angstrom.Buffered.feed t.fragments_parser
          (`Bigstring (Utils.cstruct_to_bigstringaf bytes));
      let frames, next_state =
        Utils.consume_parser t.fragments_parser Frame.parser
      in
      t.fragments_parser <- next_state;
      List.iter (Pipe.push_or_drop t.read_frame) frames;
      Ok []
  | Close -> Error (`Closed "Connection FSM error")

let input (St t) = function
  | Close msg ->
      t.stage <- Close;
      Pipe.close t.read_frame msg;
      Ok []
  | Data bytes -> input_bytes (St t) bytes

let output (St t) = Pipe.pop t.send_buffer

let rec wait_until_ready (St t) =
  match t.stage with
  | Traffic _ -> Lwt.return_true
  | Close -> Lwt.return_false
  | _ ->
      let* () = Lwt_condition.wait t.ready_condition in
      wait_until_ready (St t)

let is_ready (St t) = match t.stage with Traffic _ -> true | _ -> false
let tag (St t) = t.tag
let close_output (St t) = Pipe.close t.send_buffer "closed"
let close_input (St t) = Pipe.close t.read_frame "closed"
let is_send_queue_full (St t) = Pipe.is_full t.send_buffer

let write (St t) frames =
  List.map Frame.to_cstruct frames |> Pipe.push t.send_buffer

let read (St t) = Pipe.pop t.read_frame
