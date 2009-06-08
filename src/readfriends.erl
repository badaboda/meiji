
-module(readfriends).

-export([load/1]).

-record(subscription, {subscriber, subscribee}).

 

load(Filename) ->

    for_each_line_in_file(Filename,

        fun(Line, Acc) ->

            [As, Bs] = string:tokens(string:strip(Line, right, $\n), " "),

            {A, _} = string:to_integer(As),

            {B, _} = string:to_integer(Bs),

            [ #subscription{subscriber=A, subscribee=B} | Acc ]

        end, [read], []).

 

% via: http://www.trapexit.org/Reading_Lines_from_a_File

for_each_line_in_file(Name, Proc, Mode, Accum0) ->

    {ok, Device} = file:open(Name, Mode),

    for_each_line(Device, Proc, Accum0).

 

for_each_line(Device, Proc, Accum) ->

    case io:get_line(Device, "") of

        eof  -> file:close(Device), Accum;

        Line -> NewAccum = Proc(Line, Accum),

                    for_each_line(Device, Proc, NewAccum)

    end.


