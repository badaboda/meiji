namespace py meiji

service RouterProxy {
  string ping(1: string Msg),
  void create(1: string Channel),
  void destroy(1: string Channel)
  void send_as_raw(1: string Channel, 2: string Msg)
  void send(1: string Channel, 2: string Msg)
}

