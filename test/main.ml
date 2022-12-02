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

let server kind port =
  let+ stack = stack in
  let s = Socket.create_socket context kind in
  Socket.bind s port stack;
  s

let client kind port =
  let* stack = stack in
  let s = Socket.create_socket context kind in
  let+ f = Socket.connect s "127.0.0.1" port stack in
  (s, f)

let pub_server = server Pub
let pull_server = server Pull

let sub_client port =
  let+ s, f = client Sub port in
  Socket.subscribe s "";
  (s, f)

let push_client = client Push

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

let req_rep_simple () =
  let* rep = server Rep 4002 in
  let* req1, f1 = client Req 4002 in
  Logs.app (fun f -> f "REQ 1");
  let* () = Lwt_unix.sleep 0.1 in
  let* () = Socket.send req1 "bonjour" in
  let* () = Lwt_unix.sleep 0.1 in
  Logs.app (fun f -> f "bonjour");
  let* a = Socket.recv rep |> or_timeout in
  Alcotest.(check string) "1: recv bonjour" "bonjour" a;
  let* () = Socket.send rep "hello" in
  let* v = Socket.recv req1 |> or_timeout in
  Alcotest.(check string) "1.5: recv hello" "hello" v;

  let* req2, _f2 = client Req 4002 in
  Logs.app (fun f -> f "client 1 disconnects");
  let* () = Socket.disconnect f1 in
  let* () = Socket.send req2 "bonjour2" in
  let* b = Socket.recv rep |> or_timeout in
  Alcotest.(check string) "2.1: recv bonjour" "bonjour2" b;
  let* () = Socket.send rep "hello2" in
  let* v = Socket.recv req2 |> or_timeout in
  Alcotest.(check string) "2.5: recv hello2" "hello2" v;
  Lwt.return_unit

let req_router_dealer_rep_simple () =
  let* router = server Router 4003 in
  let* dealer = server Dealer 4004 in
  let* req, _ = client Req 4003 in
  let* rep, _ = client Rep 4004 in

  let* () = Socket.send req "hello" in
  let* identity_data = Socket.recv_from router in
  let* () = Socket.send_multipart dealer identity_data.data in
  let* data = Socket.recv rep in
  Alcotest.(check string) "rep: recv hello" "hello" data;
  let* () = Socket.send rep "yes" in
  let* dealer_yes = Socket.recv dealer in
  let* () =
    Socket.send_to router
      {
        identity = identity_data.identity;
        data = [ { content = dealer_yes; more = false } ];
      }
  in
  let+ req_yes = Socket.recv req in
  Alcotest.(check string) "req: recv yes" "yes" req_yes

(* execute *)
let tests =
  [
    ("pub-sub", [ ("Simple", `Quick, pub_sub_simple) ]);
    ("push-pull", [ ("Simple", `Quick, push_pull_simple) ]);
    ("req-rep", [ ("Simple", `Quick, req_rep_simple) ]);
    ( "req-router-dealer-rep",
      [ ("Simple", `Quick, req_router_dealer_rep_simple) ] );
  ]

let () =
  Logs.set_level (Some Info);
  Logs.set_reporter (Logs_fmt.reporter ());
  Lwt_main.run @@ Alcotest_lwt.run "mirage-zmq" tests
