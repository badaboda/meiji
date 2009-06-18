% vim: set sts=4 sw=4 ts=4 et ai:
-module(router).
-behaviour(gen_server).
 
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).
 
-export([send/2, login/2, logout/1]).

-export([create/2]).
 
-define(SERVER, global:whereis_name(?MODULE)).
 
% will hold bidirectional mapping between id <–> pid
-record(state, {pid2id, id2pid}).
 
start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).
 
% sends Msg to anyone subscribed to Id
send(Id, Msg) ->
    gen_server:call(?SERVER, {send, Id, Msg}).
 
login(Id, Pid) when is_pid(Pid) ->
    %throw("thorw"),
    gen_server:call(?SERVER, {login, Id, Pid}).
 
logout(Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {logout, Pid}).

create(Channel, Pid) ->
    io:format("call create",[]),
    gen_server:call(?SERVER, {create, Channel, Pid}).
    
%%
 
init([]) ->
    % set this so we can catch death of logged in pids:
    process_flag(trap_exit, true),
    % use ets for routing tables
    {ok, #state{
                pid2id = ets:new(?MODULE, [bag]),
                id2pid = ets:new(?MODULE, [bag])
               }
    }.
 
handle_call({login, Id, Pid}, _From, State) when is_pid(Pid) ->
    io:format("handle_call login ~w",[State#state.pid2id]),
    PidRows=ets:lookup(State#state.id2pid, Id),
    case length(PidRows) of
        0 ->
            io:format("channel not found : ~w\n",[Id]),
            {reply, notfound, State};
        _ ->
            ets:insert(State#state.pid2id, {Pid, Id}),
            ets:insert(State#state.id2pid, {Id, Pid}),
            link(Pid), % tell us if they exit, so we can log them out
            io:format("~w logged in as ~w\n",[Pid, Id]),
            {reply, ok, State}
    end;


handle_call({logout, Pid}, _From, State) when is_pid(Pid) ->
    unlink(Pid),
    PidRows = ets:lookup(State#state.pid2id, Pid),
    case PidRows of
        [] ->
            ok;
        _ ->
            IdRows = [ {I,P} || {P,I} <- PidRows ], % invert tuples
            ets:delete(State#state.pid2id, Pid),   % delete all pid->id entries
            [ ets:delete_object(State#state.id2pid, Obj) || Obj <- IdRows ] % and all id->pid
    end,
    io:format("pid ~w logged out\n",[Pid]),
    {reply, ok, State};


handle_call({create, Channel, Pid}, _From, State) ->
    io:format("handle_call create : ~w\n", [Channel]),
    ets:insert(State#state.id2pid, {Channel, Pid}),
    ets:insert(State#state.pid2id, {Pid, Channel}),
    {reply, ok, State};

handle_call({send, Id, Msg}, _From, State) ->
    % get pids who are logged in as this Id
    Pids = [ P || { _Id, P } <- ets:lookup(State#state.id2pid, Id) ],
    io:format("pids are  ~w \n",[Pids]),
    % send Msg to them all
    M = {router_msg, Msg},
    [ Pid ! M || Pid <- Pids ],
    {reply, ok, State}.

% handle death and cleanup of logged in processes
handle_info(Info, State) ->
    case Info of
        {'EXIT', Pid, _Why} ->
            handle_call({logout, Pid}, blah, State);
        Wtf ->
            io:format("Caught unhandled message: ~w\n", [Wtf])
    end,
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

