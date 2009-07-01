-module(router_stub).
-export([start/0,stop/1,handle_function/2]).
-export([ping/1,create/1,destroy/1,send/2,send_as_raw/2]).

ping(Msg) ->
    Msg.
to_string(BitStr) ->
    case is_bitstring(BitStr) of
        true ->
            bitstring_to_list(BitStr);
        _ ->
            BitStr
    end.

create(Channel) ->
    router:create(to_string(Channel)).

destroy(Channel) ->
    router:destroy(to_string(Channel)).

send(Channel,Msg) ->
    router:send(to_string(Channel),Msg).

send_as_raw(Channel,Msg) ->
    router:send_as_raw(to_string(Channel),Msg).
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
