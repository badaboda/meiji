% vim: set sts=4 sw=4 ts=4 et ai:
-module(router).
-behaviour(gen_server).
 
-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
     terminate/2, code_change/3]).
 
-export([send/2, send_as_raw/2, login/2, logout/1]).

-export([create/1, destroy/1, dump/0, get_state/0]).
 
-define(SERVER, global:whereis_name(?MODULE)).
 
% will hold bidirectional mapping between id <–> pid
-record(state, {pid2id, id2pid}).
 
start_link() ->
    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).
 
% sends Msg to anyone subscribed to Id
send(Id, Msg) ->
    gen_server:call(?SERVER, {send, Id, Msg}, infinity).

send_as_raw(Id, Msg) ->
    gen_server:call(?SERVER, {send_as_raw, Id, Msg}, infinity).
 
login(Id, Pid) when is_pid(Pid) ->
    %throw("thorw"),
    gen_server:call(?SERVER, {login, Id, Pid}).
 
logout(Pid) when is_pid(Pid) ->
    gen_server:call(?SERVER, {logout, Pid}).

dummy() ->
    receive 
        stop ->
            ok;
        _Other ->
            dummy()
    end.

create(Channel) ->
    gen_server:call(?SERVER, {create, Channel, spawn(fun dummy/0)}).

destroy(Channel) ->
    gen_server:call(?SERVER, {destroy, Channel}).

get_state() ->
    gen_server:call(?SERVER, {get_state}).

dump() ->
    gen_server:cast(?SERVER, dump).
    
%%
 
init([]) ->
    % set this so we can catch death of logged in pids:
    process_flag(trap_exit, true),
    % use ets for routing tables
    {ok, #state{
                pid2id = ets:new(?MODULE, [duplicate_bag]),
                id2pid = ets:new(?MODULE, [duplicate_bag])
               }
    }.
 
handle_call({login, Id, Pid}, _From, State) when is_pid(Pid) ->
    case ets:member(State#state.id2pid, Id) of
        false ->
            %io:format("[login] channel not found : ~p\n",[Id]),
            {reply, notfound, State};
        true ->
            ets:insert(State#state.pid2id, {Pid, Id}),
            ets:insert(State#state.id2pid, {Id, Pid}),
            link(Pid), % tell us if they exit, so we can log them out
            %io:format("[login] ~p logged in as ~p\n",[Pid, Id]),
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
    %io:format("pid ~p logged out\n",[Pid]),
    {reply, ok, State};


handle_call({create, Channel, DummyPid}, _From, State) ->
    case ets:member(State#state.id2pid,Channel) of
        false ->
            ets:insert(State#state.id2pid, {Channel, DummyPid}),
            ets:insert(State#state.pid2id, {DummyPid, Channel}),
            link(DummyPid), % tell us if they exit, so we can log them out
            io:format("[create] ~p created in as ~p\n",[DummyPid, Channel]),
            {reply, ok, State};
        true ->
            io:format("[create] ~p already created in as ~p\n",[DummyPid, Channel]),
            {reply, ok, State}
    end;
        

handle_call({destroy, Channel}, _From, State) ->
    io:format("[destroy] call destroy\n",[]),
    IdRows = ets:lookup(State#state.id2pid, Channel),
    case IdRows of
        [] ->
            ok;
        _ ->
            PidRows = [ {P,I} || {I,P} <- IdRows ], % invert tuples
            [P ! stop || {P, I} <- PidRows]
    end,
    {reply, ok, State};

handle_call({get_state}, _From, State) ->
    {reply, {ok, State}, State};

handle_call({send_as_raw, Id, Msg}, _From, State) ->
    M = {raw_msg, Msg},
    lists:foreach(fun({_Id, Pid}) -> Pid ! M end, ets:lookup(State#state.id2pid, Id)),
    {reply, ok, State};

handle_call({send, Id, Msg}, _From, State) ->
    M = {callback_msg, Msg},
    lists:foreach(fun({_Id, Pid}) -> Pid ! M end, ets:lookup(State#state.id2pid, Id)),
    {reply, ok, State}.

% handle death and cleanup of logged in processes
handle_info(dump, State) ->
    io:format("id2pid : ~p~n", [ets:tab2list(State#state.id2pid)]),
    io:format("pid2id : ~p~n", [ets:tab2list(State#state.pid2id)]),
    {noreply, State};

handle_info(Info, State) ->
    case Info of
        {'EXIT', Pid, _Why} ->
            handle_call({logout, Pid}, blah, State);
        Wtf ->
            io:format("Caught unhandled message: ~p\n", [Wtf])
    end,
    {noreply, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

