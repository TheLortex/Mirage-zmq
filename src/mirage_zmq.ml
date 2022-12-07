(*
 * Copyright 2018-2019 Huiyao Zheng <huiyaozheng@gmail.com>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)
open Lwt.Syntax

exception No_Available_Peers
exception Incorrect_use_of_API of string
exception Connection_closed

include Socket

(* CURVE not implemented *)
(* type connection_stage = GREETING | HANDSHAKE | TRAFFIC | CLOSED
   type connection_fsm_data = Input_data of Cstruct.t | End_of_connection
*)
module Context = Context
module Message = Message
include Security_mechanism

let src = Logs.Src.create "zmq" ~doc:"ZeroMQ"

module Log = (val Logs.src_log src : Logs.LOG)

module Socket_type = struct
  include Socket_type
  include Socket_type.T
end

module Connection_tcp (S : Tcpip.Stack.V4V6) = struct
  (* Start of helper functions *)

  (** Creates a tag for a TCP connection *)
  let tag_of_tcp_connection ipaddr port = Fmt.str "TCP.%s.%d" ipaddr port

  (** Read input from flow, send the input to FSM and execute FSM actions *)
  let rec process_input ?(tag = "") flow connection =
    let* res = S.TCP.read flow in
    let* continue =
      match res with
      | Ok `Eof ->
          Log.info (fun f ->
              f "Module Connection_tcp(%s): Closing connection EOF" tag);
          Lwt.return_false
      | Error e ->
          Log.warn (fun f ->
              f
                "Module Raw_connection_tcp(%s): Error reading data from \
                 established connection: %a"
                tag S.TCP.pp_error e);
          Lwt.return_false
      | Ok (`Data b) -> (
          Log.info (fun f ->
              f "Module Connection_tcp(%s): Read: %d bytes" tag
                (Cstruct.length b));
          match Raw_connection.input connection (Data b) with
          | Result.Ok [] -> Lwt.return_true
          | Ok b ->
              Log.debug (fun f ->
                  f "Module Connection_tcp(%s): Connection FSM Write %d bytes"
                    tag (Cstruct.lenv b));
              let+ write_res = S.TCP.writev flow b in
              (match write_res with
              | Error _ ->
                  Log.warn (fun f ->
                      f
                        "Module Connection_tcp(%s): Error writing data to \
                         established connection."
                        tag)
              | Ok () -> ());
              true
          | Error (`Closed s) ->
              Log.warn (fun f ->
                  f "Module Connection_tcp(%s): Connection FSM Close due to: %s"
                    tag s);
              Lwt.return_false)
    in
    if continue then process_input ~tag flow connection else Lwt.return_unit

  (** Check the 'mailbox' and send outgoing data / close connection *)
  let rec process_output ?(tag = "") flow connection =
    let* actions = Raw_connection.output connection in
    match actions with
    | Error (`Closed msg) ->
        Log.info (fun f ->
            f
              "Module Connection_tcp(%s): Connection was instructed to close: \
               %s"
              tag msg);
        Lwt.return_unit
    | Ok [] -> process_output ~tag flow connection
    | Ok data -> (
        Log.debug (fun f ->
            f "Module Connection_tcp(%s): Connection mailbox Write %d bytes" tag
              (Cstruct.lenv data));
        let* write_res = S.TCP.writev flow data in
        match write_res with
        | Error _ ->
            Log.warn (fun f ->
                f
                  "Module Connection_tcp(%s): Error writing data to \
                   established connection."
                  tag);
            Lwt.return_unit
        | Ok () -> process_output ~tag flow connection)

  (* End of helper functions *)

  let listen s port socket =
    S.TCP.listen (S.tcp s) ~port (fun flow ->
        let dst, dst_port = S.TCP.dst flow in
        Log.info (fun f ->
            f "Module Connection_tcp: New tcp connection from IP %s on port %d"
              (Ipaddr.to_string dst) dst_port);
        let connection =
          Raw_connection.init (Socket.typ' socket)
            (Security_mechanism.init
               (Socket.security_info socket)
               (Socket.metadata socket))
            (tag_of_tcp_connection (Ipaddr.to_string dst) dst_port)
        in
        let* () =
          Lwt.pick
            [
              Socket.add_connection socket connection;
              process_input ~tag:"server" flow connection;
              process_output ~tag:"server" flow connection;
            ]
        in
        Log.info (fun f ->
            f "Module Connection_tcp(server): client %s:%d disconnected "
              (Ipaddr.to_string dst) dst_port);
        S.TCP.close flow);
    S.listen s

  let rec connect s addr port socket =
    let connection =
      Raw_connection.init (Socket.typ' socket)
        (Security_mechanism.init
           (Socket.security_info socket)
           (Socket.metadata socket))
        (tag_of_tcp_connection addr port)
    in
    let ipaddr = Ipaddr.of_string_exn addr in
    (* TODO what if connection lost after successful one: should transparently work *)
    let* flow = S.TCP.create_connection (S.tcp s) (ipaddr, port) in
    let disconnect = Lwt_switch.create () in
    match flow with
    | Ok flow ->
        Lwt_switch.add_hook (Some disconnect) (fun () ->
            Log.info (fun f ->
                f
                  "Module Connection_tcp(client): %a:%d asking for \
                   disconnection"
                  Ipaddr.pp ipaddr port);
            Raw_connection.close_output connection;
            S.TCP.close flow);
        Lwt.dont_wait
          (fun () ->
            let+ () =
              Lwt.join
                [
                  Socket.add_connection socket connection;
                  (let+ () = process_input ~tag:"client" flow connection in
                   Log.info (fun f ->
                       f "Module Connection_tcp(client): process_input finished");
                   Raw_connection.close_input connection);
                  (let+ () = process_output ~tag:"client" flow connection in
                   Log.info (fun f ->
                       f
                         "Module Connection_tcp(client): process_output \
                          finished"));
                ]
            in
            Log.info (fun f ->
                f "Module Connection_tcp(client): server %s:%d disconnected "
                  (Ipaddr.to_string ipaddr) port))
          raise;
        Log.info (fun f -> f "Spawned threads");
        let+ _ = Raw_connection.wait_until_ready connection in
        Log.info (fun f -> f "Traffic");
        disconnect
    | Error e ->
        Log.warn (fun f ->
            f
              "Module Connection_tcp: Error establishing connection: %a, \
               retrying"
              S.TCP.pp_error e);
        connect s addr port socket

  let disconnect s = Lwt_switch.turn_off s
end

module Socket_tcp (S : Tcpip.Stack.V4V6) = struct
  (* type transport_info = Tcp of string * int
 *)
  type 'b t = U : ('a, 'b) Socket.t -> 'b t

  let create_socket context ?(mechanism = Security_mechanism.NULL) socket_type =
    U (Socket.create_socket context ~mechanism socket_type)

  let set_plain_credentials (U socket) username password =
    Socket.set_plain_credentials socket username password

  let set_plain_user_list (U socket) list =
    Socket.set_plain_user_list socket list

  let set_identity (U socket) identity = Socket.set_identity socket identity

  let set_incoming_queue_size (U socket) size =
    Socket.set_incoming_queue_size socket size

  let set_outgoing_queue_size (U socket) size =
    Socket.set_outgoing_queue_size socket size

  let subscribe (U socket) subscription = Socket.subscribe socket subscription

  let unsubscribe (U socket) subscription =
    Socket.unsubscribe socket subscription

  let recv (U socket) = Socket.recv socket
  let recv_multipart (U socket) = Socket.recv_multipart socket
  let send (U socket) msg = Socket.send socket msg
  let send_multipart (U socket) msg = Socket.send_multipart socket msg
  let recv_from (U socket) = Socket.recv_from socket
  let send_to (U socket) msg = Socket.send_to socket msg
  let send_blocking (U socket) msg = Socket.send_blocking socket msg

  module C_tcp = Connection_tcp (S)

  let bind (U socket) port s = Lwt.async (fun () -> C_tcp.listen s port socket)

  type flow = Lwt_switch.t

  let connect (U socket) ipaddr port s = C_tcp.connect s ipaddr port socket
  let disconnect f = C_tcp.disconnect f
end
