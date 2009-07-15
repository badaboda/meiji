% vim: set ts=4 sts=4 sw=4 et ai:
-module(meiji_web).
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

format_as_js_callback(Msg) ->
    iolist_to_binary(io_lib:format("<script>parent.callback(\"~s\")</script>~n", [Msg])).
 
loop(Req, DocRoot) ->
    "/" ++ Path = Req:get(path),
    case Req:get(method) of
        Method when Method =:= 'GET'; Method =:= 'HEAD' ->
            case Path of 
                "static/" ++ StaticPath ->
                    Req:serve_file(StaticPath, DocRoot);
                "meiji/" ++ Id ->
                    BinId=list_to_binary(Id),
                    Status=router:login(BinId, self()), 
                    if 
                        Status =:= ok -> 
                            Response = Req:ok({"text/html; charset=utf-8", [{"Server","mochiweb-r101"}], chunked}),
                            Response:write_chunk([list_to_binary(string:copies(" ", 1024)), "meiji id: ", BinId, "\n"]),
                            feed(Response, BinId, 1);
                        true ->
                            Response = Req:not_found()
                    end;
                "xhr-multipart/" ++ Id ->
                    BinId=list_to_binary(Id),
                    Status=router:login(BinId, self()), 
                    if 
                        Status =:= ok -> 
                            Response = Req:ok({"multipart/x-mixed-replace; boundary=xstringx", [{"Server","mochiweb-r101"}], chunked}),
                            Response:write_chunk("--xstringx\r\nContent-Type: text/html\r\n\r\nsome messages\n"),
                            feed(Response, BinId, 1);
                        true ->
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
        {callback_msg, Msg} ->
            Response:write_chunk(format_as_js_callback(Msg)),
            feed(Response, Path, N+1);
        {raw_msg, Msg} ->
            Response:write_chunk(Msg),
            feed(Response, Path, N+1);
        stop ->
            exit(normal);
        Other ->
            io:format("ignored messages: ~p~n", [Other])
    end.
 
%% Internal API
get_option(Option, Options) ->
    {proplists:get_value(Option, Options), proplists:delete(Option, Options)}.

