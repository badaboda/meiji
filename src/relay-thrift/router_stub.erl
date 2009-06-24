-module(router_stub).
-export([start/0,stop/1,handle_function/2]).
-export([ping/1,create/1,destroy/1]).

ping(Msg) ->
    Msg.

create(Channel) ->
    router:create(Channel).

destroy(Channel) ->
    router:destroy(Channel).

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
