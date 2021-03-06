-module(floodtest2).

-compile(export_all).

-define(SERVERADDR, "10.99.99.24"). % where mochiweb is running

-define(SERVERPORT, 8000).

-export([start/5]).

 

% Generate the config in bash like so (chose some available address space):

% EACH=62000; for i in `seq 1 17`; do echo "{{10,0,0,$i}, $((($i-1)*$EACH+1)), $(($i*$EACH))}, "; done

 



start(Interval) ->

        Monitor = monitor(),


        spawn(fun() -> floodtest2:start(1, 62000, '10.99.99.23', Interval, Monitor) end),

        ok.
 


 

start(LowerID, UpperID, _, _, _) when LowerID == UpperID -> done;

start(LowerID, UpperID, LocalIP, Interval, Monitor) ->

	io:format("xxxx ~s, ~s,  ~s, ~s, ~s~n", [LowerID, UpperID, LocalIP, Interval, Monitor]),

        spawn(?MODULE, connect, [?SERVERADDR, ?SERVERPORT, LocalIP, "/test/"++LowerID, Monitor]),

        receive after Interval -> start(LowerID + 1, UpperID, LocalIP, Interval, Monitor) end.

 

connect(ServerAddr, ServerPort, ClientIP, Path, Monitor) ->

        Opts = [binary, {packet, 0}, {ip, ClientIP},  {reuseaddr, true}, {active, false}],

        {ok, Sock} = gen_tcp:connect(ServerAddr, ServerPort, Opts),

        Monitor ! open,

        ReqL = io_lib:format("GET ~s\r\nHost: ~s\r\n\r\n", [Path, ServerAddr]),

        Req = list_to_binary(ReqL),

        ok = gen_tcp:send(Sock, [Req]),

        do_recv(Sock, Monitor),

        (catch gen_tcp:close(Sock)),

        ok.

 

do_recv(Sock, Monitor)->

        case gen_tcp:recv(Sock, 0) of

                {ok, B} ->

                        Monitor ! {bytes, size(B)},

                        io:format("Recvd ~s\n", [ binary_to_list(B)]),

                        io:format("Recvd ~w bytes\n", [size(B)]),

                        do_recv(Sock, Monitor);

                {error, closed} ->

                        Monitor ! closed,

                        closed;

                Other ->

                        Monitor ! closed,

                        io:format("Other:~w\n",[Other])

        end.

 

% Monitor process receives stats and reports how much data we received etc:

monitor() ->

        Pid = spawn(?MODULE, monitor0, [{0,0,0,0}]),

        timer:send_interval(10000, Pid, report),

        Pid.

 

monitor0({Open, Closed, Chunks, Bytes}=S) ->

        receive

                report  -> io:format("{Open, Closed, Chunks, Bytes} = ~w\n",[S]);

                open    -> monitor0({Open + 1, Closed, Chunks, Bytes});

                closed  -> monitor0({Open, Closed + 1, Chunks, Bytes});

                chunk   -> monitor0({Open, Closed, Chunks + 1, Bytes});

                {bytes, B} -> monitor0({Open, Closed, Chunks, Bytes + B})

        end.


