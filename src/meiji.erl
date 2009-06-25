%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc TEMPLATE.

-module(meiji).
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
%% @doc Start the meiji server.
start() ->
    meiji_deps:ensure(),
    ensure_started(crypto),
    application:start(meiji).

%% @spec stop() -> ok
%% @doc Stop the meiji server.
stop() ->
    Res = application:stop(meiji),
    application:stop(crypto),
    Res.
