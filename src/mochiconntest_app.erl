%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc Callbacks for the mochiconntest application.

-module(mochiconntest_app).
-author('author <author@example.com>').

-behaviour(application).
-export([start/2,stop/1]).


%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for mochiconntest.
start(_Type, _StartArgs) ->
    mochiconntest_deps:ensure(),
    mochiconntest_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for mochiconntest.
stop(_State) ->
    ok.
