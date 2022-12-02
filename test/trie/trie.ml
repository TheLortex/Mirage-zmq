let sort list = List.sort String.compare list
let trie = Trie.create ()
let _ = Trie.insert trie ""
let _ = assert (not (Trie.is_empty trie))
let _ = assert (Trie.find trie "ABC")
let _ = assert (Trie.find trie "A")
let _ = assert (sort (Trie.to_list trie) = [ "" ])
let _ = Trie.delete trie ""
let _ = assert (sort (Trie.to_list trie) = [])
let _ = assert (Trie.is_empty trie)
let _ = assert (not (Trie.find trie "ABC"))
let _ = Trie.insert trie "A"
let _ = assert (sort (Trie.to_list trie) = [ "A" ])
let _ = Trie.insert trie "A"
let _ = assert (sort (Trie.to_list trie) = [ "A"; "A" ])
let _ = assert (not (Trie.is_empty trie))
let _ = assert (Trie.find trie "ABC")
let _ = assert (Trie.find trie "A")
let _ = assert (Trie.find trie "AB")
let _ = Trie.delete trie "A"
let _ = assert (sort (Trie.to_list trie) = [ "A" ])
let _ = assert (not (Trie.is_empty trie))
let _ = assert (Trie.find trie "ABC")
let _ = assert (Trie.find trie "AB")
let _ = Trie.delete trie "A"
let _ = assert (sort (Trie.to_list trie) = [])
let _ = assert (Trie.is_empty trie)
let _ = assert (not (Trie.find trie "ABC"))
let _ = assert (not (Trie.find trie "AB"))
let _ = assert (not (Trie.find trie "A"))
let _ = Trie.insert trie "AB"
let _ = assert (sort (Trie.to_list trie) = [ "AB" ])
let _ = assert (not (Trie.is_empty trie))
let _ = assert (Trie.find trie "ABC")
let _ = Trie.insert trie "AD"
let _ = assert (sort (Trie.to_list trie) = [ "AB"; "AD" ])
let _ = assert (not (Trie.is_empty trie))
let _ = assert (Trie.find trie "ABC")
let _ = assert (Trie.find trie "ADCC")
let _ = assert (not (Trie.find trie "AEA"))
let _ = Trie.delete trie "AB"
let _ = assert (sort (Trie.to_list trie) = [ "AD" ])
let _ = assert (not (Trie.is_empty trie))
let _ = assert (not (Trie.find trie "ABC"))
let _ = assert (Trie.find trie "ADCC")
let _ = Trie.insert trie "ABD"
let _ = assert (sort (Trie.to_list trie) = [ "ABD"; "AD" ])
let _ = Trie.delete trie "ABD"
let _ = assert (sort (Trie.to_list trie) = [ "AD" ])
let _ = assert (Trie.find trie "AD")
let _ = assert (not (Trie.is_empty trie))
