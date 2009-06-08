
-module(mochiconntest_web).

 

-export([start/1, stop/0, loop/2, feed/3]).

 

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

    case Req:get(method) of

        Method when Method =:= 'GET'; Method =:= 'HEAD' ->

            case Path of

                "test/" ++ IdStr ->

                    Response = Req:ok({"text/html; charset=utf-8",

                                      [{"Server","Mochiweb-Test"}],

                                      chunked}),

                    {Id, _} = string:to_integer(IdStr),

                    router:login(Id, self()),

                    % Hibernate this process until it receives a message:

                    proc_lib:hibernate(?MODULE, feed, [Response, Id, 1]);

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

 

feed(Response, Id, N) ->

    receive

    {router_msg, Msg} ->

        Html = io_lib:format("Recvd msg #~w: ‘~w’<br/>", [N, Msg]),

        Response:write_chunk(Html)

    end,

    % Hibernate this process until it receives a message:

    proc_lib:hibernate(?MODULE, feed, [Response, Id, N+1]).

 

 

%% Internal API

 

get_option(Option, Options) ->

    {proplists:get_value(Option, Options), proplists:delete(Option, Options)}.


