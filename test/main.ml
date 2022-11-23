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

let pull_server port =
  let+ stack = stack in
  let s = Socket.create_socket context Pull in
  Socket.bind s port stack;
  s

let sub_client port =
  let* stack = stack in
  let s = Socket.create_socket context Sub in
  Socket.subscribe s "";
  let+ f = Socket.connect s "127.0.0.1" port stack in
  (s, f)

let push_client port =
  let* stack = stack in
  let s = Socket.create_socket context Push in
  let+ f = Socket.connect s "127.0.0.1" port stack in
  (s, f)

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
  let* sub1, f1 = sub_client 4000 in
  Logs.info (fun f -> f "SUB 1");
  let* sub2, _f2 = sub_client 4000 in
  Logs.info (fun f -> f "SUB 2");
  let* () = Lwt_unix.sleep 0.1 in
  let* () = Socket.send pub "bonjour" in
  Logs.info (fun f -> f "bonjour");
  let* a = Socket.recv sub1 |> or_timeout in
  Alcotest.(check string) "1: recv bonjour" "bonjour" a;
  let* b = Socket.recv sub2 |> or_timeout in
  Alcotest.(check string) "2: recv bonjour" "bonjour" b;
  let* () = Socket.disconnect f1 in
  let* () = Socket.send pub "bonjour2" in
  let* c = Socket.recv sub2 |> or_timeout in
  Alcotest.(check string) "3: recv bonjour" "bonjour2" c;
  Lwt.return_unit

let push_pull_simple () =
  let* pull = pull_server 4001 in
  let* push1, f1 = push_client 4001 in
  Logs.info (fun f -> f "PUSH 1");
  let* () = Lwt_unix.sleep 0.1 in
  let* () = Socket.send push1 "bonjour" in
  let* () = Lwt_unix.sleep 0.1 in
  Logs.info (fun f -> f "bonjour");
  let* a = Socket.recv pull |> or_timeout in
  Alcotest.(check string) "1: recv bonjour" "bonjour" a;

  let* push2, _f2 = push_client 4001 in
  let* () = Socket.disconnect f1 in
  let* () = Socket.send push2 "bonjour2" in
  let* b = Socket.recv pull |> or_timeout in
  Alcotest.(check string) "2: recv bonjour" "bonjour2" b;
  Lwt.return_unit

(* execute *)
let tests =
  [
    ("pub-sub", [ ("Simple", `Quick, pub_sub_simple) ]);
    ("push-pull", [ ("Simple", `Quick, push_pull_simple) ]);
  ]

let () =
  Logs.set_level (Some Debug);
  Logs.set_reporter (Logs_fmt.reporter ());
  Lwt_main.run @@ Alcotest_lwt.run "mirage-zmq" tests
