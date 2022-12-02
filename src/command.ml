type t = { name : string; data : string }

let to_frame t =
  Frame.make
    (String.concat ""
       [ String.make 1 (Char.chr (String.length t.name)); t.name; t.data ])
    ~more:false ~command:true

let get_name t = t.name
let get_data t = t.data

let of_frame frame =
  let data = Frame.get_body frame in
  let name_length = Char.code (String.get data 0) in
  {
    name = String.sub data 1 name_length;
    data =
      String.sub data (name_length + 1) (String.length data - 1 - name_length);
  }

let make ~name ~data = { name; data }
