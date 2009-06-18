% vim: set ts=4 sts=4 sw=4 et ai:
-module(mochiconntest_web).
-export([start/1, stop/0, loop/2]).
%% External API
start(Options) ->
    {DocRoot, Options1} = get_option(docroot, Options),
    Loop = fun (Req) ->
                   ?MODULE:loop(Req, DocRoot)
           end,
    % we’ll set our maximum to 1 million connections. (default: 2048)
    mochiweb_http:start([{max, 1000000}, {name, ?MODULE}, {loop, Loop} | Options1]).
 
stop() ->
    mochiweb_http:stop(?MODULE).
 
loop(Req, DocRoot) ->
    "/" ++ Path = Req:get(path),
    io:format("~s connected\n",[Path]),
    case Req:get(method) of
        Method when Method =:= 'GET'; Method =:= 'HEAD' ->
            case Path of "test/" ++ Id ->
                    {IdInt, _} = string:to_integer(Id),
                    %try router:login(IdInt, self()) catch throw:X -> io:format("~s\n",[X])end,
                    Status =router:login(IdInt, self()), 
                    io:format("return value : ~s\n",[Status]),
                    if 
                        Status =:= ok -> 
                            Response = Req:ok({"text/html; charset=utf-8", [{"Server","Mochiweb-Test"}], chunked}),
                            Response:write_chunk(string:copies(" ", 1024) ++ 
                                                 "Mochiconntest welcomes you! Your Id2: " ++ Id ++ "<br />\n"),
                            io:format("sended welcome message",[]),
                            % login using an integer rather than a string
                            feed(Response, IdInt, 1);
                        true ->
                            io:format("404",[]),
                            Response = Req:not_found()
                    end;
                _ ->
                    Req:not_found()
            end;
        'POST' ->
            case Path of
                _ ->
                    Req:not_found()
            end;
        _ ->
            Req:respond({501, [], []})
    end.
 
feed(Response, Path, N) ->
    receive
        {router_msg, Msg} ->
            Html = io_lib:format("~s", [Msg]),
            Response:write_chunk(Html)
    end,
    feed(Response, Path, N+1).
 
%% Internal API
get_option(Option, Options) ->
    {proplists:get_value(Option, Options), proplists:delete(Option, Options)}.

