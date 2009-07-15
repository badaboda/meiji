-module(router_stub).
-export([start/0,stop/1,handle_function/2]).
-export([create/1,destroy/1,send/2,send_as_raw/2]).

create(Channel) ->
    router:create(Channel).

destroy(Channel) ->
    router:destroy(Channel).

send(Channel,Msg) ->
    router:send(Channel,Msg).

send_as_raw(Channel,Msg) ->
    router:send_as_raw(Channel,Msg).

%%

start() ->
    start(9999).

start(Port) ->
    Handler   = ?MODULE,
    thrift_socket_server:start([{handler, Handler},             
                                {service, routerProxy_thrift},   
                                {port, Port},                   
                                {name, routerProxy_server}]).                                                                                                        
stop(Server) ->
    thrift_socket_server:stop(Server).                                                                                                                            
handle_function(Function, Args) when is_atom(Function), is_tuple(Args) ->
    case apply(?MODULE, Function, tuple_to_list(Args)) of
        ok -> ok;
        Reply -> {reply, Reply}
    end.
