type t = { name : string; data : bytes }

let to_frame t =
  Frame.make_frame
    (Bytes.concat Bytes.empty
        [
          Bytes.make 1 (Char.chr (String.length t.name));
          Bytes.of_string t.name;
          t.data;
        ])
    ~if_more:false ~if_command:true

let get_name t = t.name
let get_data t = t.data

let of_frame frame =
  let data = Frame.get_body frame in
  let name_length = Char.code (Bytes.get data 0) in
  {
    name = Bytes.sub_string data 1 name_length;
    data =
      Bytes.sub data (name_length + 1) (Bytes.length data - 1 - name_length);
  }

let make_command command_name data = { name = command_name; data }
