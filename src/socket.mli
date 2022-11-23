type ('s, 'a) t

val create_socket :
  Context.t ->
  ?mechanism:Security_mechanism.mechanism_type ->
  ('a, 'b) Socket_type.t ->
  ('a, 'b) t
(** Create a socket from the given context, mechanism and type *)
