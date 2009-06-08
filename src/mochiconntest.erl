%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc TEMPLATE.

-module(mochiconntest).
-author('author <author@example.com>').
-export([start/0, stop/0]).

ensure_started(App) ->
    case application:start(App) of
        ok ->
            ok;
        {error, {already_started, App}} ->
            ok
    end.
        
%% @spec start() -> ok
%% @doc Start the mochiconntest server.
start() ->
    mochiconntest_deps:ensure(),
    ensure_started(crypto),
    application:start(mochiconntest).

%% @spec stop() -> ok
%% @doc Stop the mochiconntest server.
stop() ->
    Res = application:stop(mochiconntest),
    application:stop(crypto),
    Res.
