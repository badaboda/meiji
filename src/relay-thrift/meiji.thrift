namespace py meiji

service RouterProxy {
  string ping(1: string Msg),
  void create(1: i32 Channel),
  void destroy(1: i32 Channel)
}

