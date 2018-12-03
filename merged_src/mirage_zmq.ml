open Lwt
open Mirage_stack_lwt
open Mirage


exception Not_Implemented
type socket_type = REQ | REP | DEALER | ROUTER
type mechanism_type = NULL | PLAIN

module type Socket = sig
    val name : string
end

module type Connection = sig
    type stage
    type t
    type action =
    | Write of bytes
    | Continue
    | Close
    val connection_fsm : t -> Bytes.t -> t * action list
    val new_connection : unit -> t
end

module type Security_Mechanism = sig
    type t
    val name : string
end

module NULL : Security_Mechanism = struct
    type t
    let name = "NULL"
end

module PLAIN : Security_Mechanism = struct
    type t
    let name = "PLAIN"
end

module type Greeting = sig
    type t
    type event =
        | Recv_sig of bytes
        | Recv_Vmajor of bytes
        | Recv_Vminor of bytes
        | Recv_Mechanism of bytes
        | Recv_as_server of bytes
        | Recv_filler
        | Init of string
    type action =
        | Send_bytes of bytes
        | Set_mechanism of string
        | Set_server of bool
        | Continue
        | Ok
        | Error of string
    val handle : t * event -> t * action
    val handle_list : t * event list -> action list -> t * action list
end 

module New_Connection (S : Socket) (M : Security_Mechanism) : Connection = struct 
    type stage = 
    | GREETING
    | HANDSHAKE
    | TRAFFIC
    | ERROR

    type action =
    | Write of bytes
    | Continue
    | Close
    
    type t = {
        mutable stage : stage;
        mutable as_server : bool;
        counter : int;
        greeting_state : Greeting.t;
        security_policy : string
    }

    let new_connection () = {
        stage = GREETING;
        as_server = false;
        counter = 0;
        greeting_state = START;
        (* Set custom policy *)
        security_policy = M.name
    }

    let convert greeting_action_list =
        match greeting_action_list with
            | [] -> []
            | (hd::tl) ->
                match hd with 
                    | Send_bytes(b) -> (Write(b)::(convert tl))
                    | Set_server(b) -> t.as_server <- b; convert tl
                    | Set_mechanism(s) -> t.security_policy <- s; convert tl
                    | Continue -> convert tl
                    | Ok -> t.stage <- HANDSHAKE; convert tl
                    | Error(s) -> [Close]

    let connection_fsm t bytes = 
        match (t.stage) with
            | GREETING -> (let len = Bytes.length bytes in
                            (* Hard code the length here. The greeting is either complete or split into 11 + 53 or 10 + 54*)
                            match len with
                                (* Full greeting *)
                                | 64 -> let (state, action_list) = Greeting.handle_list (t.greeting_state, 
                                            [Recv_sig(Bytes.sub b 0 10); 
                                             Recv_Vmajor(Bytes.sub b 10 1);
                                             Recv_Vminor(Bytes.sub b 11 1);
                                             Recv_Mechanism(Bytes.sub b 12 20);
                                             Recv_as_server(Bytes.sub b 32 1);
                                             Recv_filler(Bytes.sub b 33 31)   
                                            ]) [] in
                                        let connection_action = convert action in 
                                        ({t with greeting_state = state}, convert action)
                                (* Signature + version major *)
                                | 11 -> let (state, action_list) = Greeting.handle_list (t.greeting_state, [Recv_sig(Bytes.sub b 0 10); Recv_Vmajor(Bytes.sub b 10 1)]) [] in
                                        ({t with greeting_state = state}, convert action)
                                (* Signature *)
                                | 10 -> let (state, action_list) = Greeting.handle (t.greeting_state, Recv_sig(b)) in
                                        ({t with greeting_state = state}, convert action)
                                (* version minor + rest *)
                                | 53 -> let (state, action_list) = Greeting.handle_list (t.greeting_state, 
                                            [Recv_Vminor(Bytes.sub b 0 1);
                                             Recv_Mechanism(Bytes.sub b 1 20);
                                             Recv_as_server(Bytes.sub b 21 1);
                                             Recv_filler(Bytes.sub b 22 31)   
                                            ]) [] in
                                        let connection_action = convert action in 
                                        ({t with greeting_state = state}, convert action)
                                (* version major + rest *)
                                | 54 -> let (state, action_list) = Greeting.handle_list (t.greeting_state, 
                                            [Recv_Major(Bytes.sub 0 1)
                                             Recv_Vminor(Bytes.sub b 1 1);
                                             Recv_Mechanism(Bytes.sub b 2 20);
                                             Recv_as_server(Bytes.sub b 22 1);
                                             Recv_filler(Bytes.sub b 23 31)   
                                            ]) [] in
                                        let connection_action = convert action in 
                                        ({t with greeting_state = state}, convert action)
                                | _ ->  {ERROR, [CLOSE]}
                        )
            | HANDSHAKE -> (t, [])
            | TRAFFIC -> (t, [])
            | ERROR -> (t, [])

end

module Connection_tcp (S: Mirage_stack_lwt.V4) (C: Connection) = struct
        let buffer_to_string data = 
        let content = ref [] in
            Bytes.iter (fun b -> content := Char.code b :: !content) data;
            String.concat " "  (List.map (fun x -> string_of_int x)  (List.rev !content))

        let connect s port =
        let rec read_and_print flow t = 
            S.TCPV4.read flow >>= (function
            | Ok `Eof -> Logs.info (fun f -> f "Closing connection!");  Lwt.return_unit
            | Error e -> Logs.warn (fun f -> f "Error reading data from established connection: %a" S.TCPV4.pp_error e); Lwt.return_unit
            | Ok (`Data b) -> (Logs.debug (fun f -> f "read: %d bytes:\n%s" (Cstruct.len b)  (buffer_to_string (Cstruct.to_bytes b))); 
                                let (new_t, action_list) = C.connection_fsm t (Cstruct.to_bytes b) in
                                let rec act actions = 
                                    (match actions with
                                        | [] -> read_and_print flow new_t
                                        | (hd::tl) -> 
                                        (match hd with
                                            | C.Write(b) -> (S.TCPV4.write flow (Cstruct.of_bytes b) >>= function
                                                            | Error _ -> (Logs.warn (fun f -> f "Error writing data to established connection."); Lwt.return_unit)
                                                            | Ok () -> act tl)
                                            | C.Continue -> act tl
                                            | C.Close -> Lwt.return_unit))
                                in
                                    act action_list))
        in
            S.listen_tcpv4 s ~port (
                fun flow ->
                    let dst, dst_port = S.TCPV4.dst flow in
                    Logs.info (fun f -> f "new tcp connection from IP %s on port %d" (Ipaddr.V4.to_string dst) dst_port);
                    read_and_print flow (C.new_connection ())
            );
            S.listen s
end

module Context = struct
    type t = {options : int}
    let create_context () = {options = 0}
end

module Socket = struct
    type transport_type = TCP | INPROC
    type transport_info = Tcp of string * int | Inproc
    type t = {
        socket_type : socket_type;
        mutable transport_type : transport_type;
        mutable transport_info : transport_info;
        mutable security_mechanism : mechanism_type
    }
    let default_t = {
        socket_type = REP;
        transport_type = TCP;
        transport_info = Tcp("", 0);
        security_mechanism = NULL
    }
    let create_socket context socket_type =
        match socket_type with
            | REP -> {default_t with socket_type = REP}
            | REQ -> {default_t with socket_type = REQ}
            | DEALER -> {default_t with socket_type = DEALER}
            | ROUTER -> {default_t with socket_type = ROUTER}
    
    (* Bind to a local port 
    let bind t transport = 
        let parts = String.split_on_char '/' transport in
        match parts with 
            | [] -> ()
            | hd::tl -> 
                let ttype = String.lowercase_ascii hd in
                match ttype with 
                    | "tcp:" -> (t.transport_type <- TCP;
                                let ip_address = String.split_on_char ':' (List.hd (List.tl tl)) in
                                    match ip_address with
                                        | [] -> assert false
                                        | ip::port -> (t.transport_info <- Tcp(ip, int_of_string (List.hd port));
                                        
                                            module tcp_worker = Connection_tcp S (initialised outside by user)
                                            module C = New_Connection (socket defind in t) (Mechanism defined in t) 
                                            C.connect s t.port
                                            Start listening on port port with cb 
                                            want something like                                        
                                            Tcp.connect s t.port
                                            t contains socket type, mechanism
                                        
                                        
                                        )
                                )
                    | "inproc:" -> raise Not_Implemented
                    | _ -> assert false *)
    let bind_tcp t port tcp_stack s =
    let module C = (match t.socket_type with
                | REP -> (match t.security_mechanism with
                            | NULL -> New_Connection REP NULL
                            | PLAIN -> New_Connection REP PLAIN
                        )
                | _ -> (match t.security_mechanism with
                            | NULL -> New_Connection REP NULL
                            | PLAIN -> New_Connection REP PLAIN
                        ))
    in
        module Tcp = (val tcp_stack : Mirage_stack_lwt);
        module C_tcp = Connection_tcp Tcp C;
        C_tcp.connect s port



    let connect t transport = raise Not_Implemented
    let set_mechanism t mechanism = ()
end


module Frame : sig
    type t
    val make_frame : bytes -> bool -> bool -> t
    val to_bytes : t -> bytes
end = struct
    type t = { flag : char; size : int; body : bytes}

    (** TODO network bytes order *)
    let size_to_bytes size = 
        if size > 255 then
            Bytes.make 1 (Char.chr size)
        else
            Bytes.init 8 (fun i -> Char.chr ((size land (255 lsl (i - 1) * 8)) lsr ((i - 1) * 8)))

    let make_frame body ifMore ifCommand = 
        let f = ref 0 in
        let len = Bytes.length body in
            if ifMore then f := !f + 1;
            if ifCommand then f := !f + 4;
            if len > 255 then f := !f + 2;
            {flag = (Char.chr (!f)); size = len; body}

    let to_bytes t =
        Bytes.concat Bytes.empty [Bytes.make 1 t.flag; size_to_bytes t.size; t.body]
end

module Command : sig
    type t
    val to_frame : t -> Frame.t
end = struct
    type t = { name : string; data : bytes }
    let to_frame t = Frame.make_frame (Bytes.concat Bytes.empty [Bytes.of_string t.name; t.data]) false true 
end


module New_Greeting (M : Security_Mechanism) : Greeting = struct
    type t =
        | START
        | SIGNATURE
        | VERSION_MAJOR
        | VERSION_MINOR
        | MECHANISM
        | AS_SERVER
        | FILLER
        | SUCCESS
        | ERROR
    type event =
        | Recv_sig of bytes
        | Recv_Vmajor of bytes
        | Recv_Vminor of bytes
        | Recv_Mechanism of bytes
        | Recv_as_server of bytes
        | Recv_filler
        | Init of string
    type action =
        | Send_bytes of bytes
        | Set_mechanism of string
        | Set_server of bool
        | Continue
        | Ok
        | Error of string
    type version = { major : bytes; minor : bytes}

    type greeting = { 
        signature  : bytes;
        version    : version;
        mechanism  : string;
        filler     : bytes
    }

    let signature = 
    let s = Bytes.make 10 (Char.chr 0) in
        Bytes.set s 0 (Char.chr 255);
        Bytes.set s 9 (Char.chr 127);
        s

    let version = {major = Bytes.make 1 (Char.chr 3); minor = Bytes.make 1 (Char.chr 0)}

    let filler = Bytes.make 31 (Char.chr 0)

    let pad_mechanism m =
    let b = Bytes.of_string m in
        if Bytes.length b < 20 then
            Bytes.cat b (Bytes.make (20 - Bytes.length b) (Char.chr 0))
        else
            b

    let to_bytes g = 
        Bytes.concat Bytes.empty [g.signature; g.version.major; g.version.minor; pad_mechanism g.mechanism; g.filler]

    let new_greeting mechanism = {signature; version; mechanism; filler}

    let handle (current_state, event) = 
    match (current_state , event) with
        | (START, Recv_sig(b)) -> 
            if (Bytes.get b 0) = (Char.chr 255) && (Bytes.get b 7) = (Char.chr 127) 
            then (SIGNATURE, Send_bytes (new_greeting M.name |> to_bytes)) 
            else (ERROR, Error("Protocol Signature not detected."))
        | (SIGNATURE, Recv_Vmajor(b)) ->
            if (Bytes.get b 0) = (Char.chr 3) 
            then (VERSION_MAJOR, Continue) 
            else (ERROR, Error("Version-major is not 3."))
        | (VERSION_MINOR, Recv_Vmajor(b)) ->
            if (Bytes.get b 0) = (Char.chr 0) 
            then (MECHANISM, Continue) 
            else (ERROR, Error("Version-minor is not 0."))
        | (MECHANISM, Recv_Mechanism(b)) ->
            (AS_SERVER, Set_mechanism(Bytes.to_string b))
        | (AS_SERVER, Recv_as_server(b)) ->
            if (Bytes.get b 0) = (Char.chr 0)
            then (FILLER, Set_server(false))
            else (FILLER, Set_server(true))
        | (FILLER, Recv_filler) -> (SUCCESS, Ok)
        | _ -> (ERROR, Error("Unexpected event."))

    let rec handle_list (current_state, event_list) action_list =
    match event_list with
        | [] -> (match current_state with 
                    | ERROR -> (ERROR, [List.hd action_list])
                    | _ -> (current_state, List.rev action_list))
        | hd::tl ->
        match current_state with
            | ERROR -> (ERROR, [List.hd action_list])
            | _ -> let (new_state, action) = handle (current_state, hd) in
                handle_list (new_state, tl) (action::action_list)
end

module type Traffic = sig
    type t
end

module REP : Socket = struct
    let name = "REP"
end

module REQ : Socket = struct
    let name = "REQ"
end

module DEALER : Socket = struct
    let name = "DEALER"
end

module ROUTER : Socket = struct
    let name = "ROUTER"
end

