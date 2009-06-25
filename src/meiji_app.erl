%% @author author <author@example.com>
%% @copyright YYYY author.

%% @doc Callbacks for the meiji application.

-module(meiji_app).
-author('author <author@example.com>').

-behaviour(application).
-export([start/2,stop/1]).


%% @spec start(_Type, _StartArgs) -> ServerRet
%% @doc application start callback for meiji.
start(_Type, _StartArgs) ->
    meiji_deps:ensure(),
    meiji_sup:start_link().

%% @spec stop(_State) -> ServerRet
%% @doc application stop callback for meiji.
stop(_State) ->
    ok.
