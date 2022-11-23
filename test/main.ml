open Lwt.Syntax
module Socket = Mirage_zmq.Socket_tcp (Tcpip_stack_socket.V4V6)

(* helper *)

let context = Mirage_zmq.Context.create_context ()

let stack =
  let* udp =
    Tcpip_stack_socket.V4V6.UDP.connect ~ipv4_only:false ~ipv6_only:false
      (Ipaddr.V4.Prefix.of_string_exn "127.0.0.1/24")
      None
  in
  let* tcp =
    Tcpip_stack_socket.V4V6.TCP.connect ~ipv4_only:false ~ipv6_only:false
      (Ipaddr.V4.Prefix.of_string_exn "127.0.0.1/24")
      None
  in
  Tcpip_stack_socket.V4V6.connect udp tcp

let pub_server port =
  let+ stack = stack in
  let s = Socket.create_socket context Pub in
  Socket.bind s port stack;
  s

let sub_client port =
  let* stack = stack in
  let s = Socket.create_socket context Sub in
  let+ _f = Socket.connect s "127.0.0.1" port stack in
  s

let get_data = function Mirage_zmq.Data d -> d | Identity_and_data (_, d) -> d

let or_timeout lwt =
  Lwt.pick
    [
      lwt;
      (let* () = Lwt_unix.sleep 1. in
       Lwt.fail_with "timeout");
    ]

(* test cases *)

let pub_sub_simple () =
  let* pub = pub_server 4000 in
  let* sub1 = sub_client 4000 in
  Logs.info (fun f -> f "SUB 1");
  let* sub2 = sub_client 4000 in
  Logs.info (fun f -> f "SUB 2");
  let* () = Socket.send pub (Data "bonjour") in
  Logs.info (fun f -> f "bonjour");
  let* a = Socket.recv sub1 |> or_timeout in
  Alcotest.(check string) "1: recv bonjour" "bonjour" (get_data a);
  let* b = Socket.recv sub2 |> or_timeout in
  Alcotest.(check string) "2: recv bonjour" "bonjour" (get_data b);

  Lwt.return_unit

(* execute *)
let tests = [ ("pub-sub", [ ("Simple", `Quick, pub_sub_simple) ]) ]
let () =
  Logs.set_level (Some Debug);
  Logs.set_reporter (Logs_fmt.reporter ());
Lwt_main.run @@ Alcotest_lwt.run "mirage-zmq" tests
