-module(subsmanager).
-behaviour(gen_server).
-include("/usr/local/lib/erlang/lib/stdlib-1.16.1/include/qlc.hrl").

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-export([add_subscriptions/1,

         remove_subscriptions/1,

         get_subscribers/1,

         first_run/0,

         stop/0,

         start_link/0]).

-record(subscription, {subscriber, subscribee}).

-record(state, {}). % state is all in mnesia

-define(SERVER, global:whereis_name(?MODULE)).

 

start_link() ->

    gen_server:start_link({global, ?MODULE}, ?MODULE, [], []).

 

stop() ->

    gen_server:call(?SERVER, {stop}).

 

add_subscriptions(SubsList) ->

    gen_server:call(?SERVER, {add_subscriptions, SubsList}, infinity).

 

remove_subscriptions(SubsList) ->

    gen_server:call(?SERVER, {remove_subscriptions, SubsList}, infinity).

 

get_subscribers(User) ->

    gen_server:call(?SERVER, {get_subscribers, User}).

 

%%

 

init([]) ->

    ok = mnesia:start(),

    io:format("Waiting on mnesia tables..\n",[]),

    mnesia:wait_for_tables([subscription], 30000),

    Info = mnesia:table_info(subscription, all),

    io:format("OK. Subscription table info: \n~w\n\n",[Info]),

    {ok, #state{}}.

 

handle_call({stop}, _From, State) ->

    {stop, stop, State};

 

handle_call({add_subscriptions, SubsList}, _From, State) ->

    % Transactionally is slower:

    % F = fun() ->

    %         [ ok = mnesia:write(S) || S <- SubsList ]

    %     end,

    % mnesia:transaction(F),

    [ mnesia:dirty_write(S) || S <- SubsList ],

    {reply, ok, State};

 

handle_call({remove_subscriptions, SubsList}, _From, State) ->

    F = fun() ->

        [ ok = mnesia:delete_object(S) || S <- SubsList ]

    end,

    mnesia:transaction(F),

    {reply, ok, State};

 

handle_call({get_subscribers, User}, From, State) ->

    F = fun() ->

        Subs = mnesia:dirty_match_object(#subscription{subscriber='_', subscribee=User}),

        Users = [Dude || #subscription{subscriber=Dude, subscribee=_} <- Subs],

        gen_server:reply(From, Users)

    end,

    spawn(F),

    {noreply, State}.

 

handle_cast(_Msg, State) -> {noreply, State}.

handle_info(_Msg, State) -> {noreply, State}.

 

terminate(_Reason, _State) ->

    mnesia:stop(),

    ok.

 

code_change(_OldVersion, State, _Extra) ->

    io:format("Reloading code for ?MODULE\n",[]),

    {ok, State}.

 

%%

 

first_run() ->

    mnesia:create_schema([node()]),

    ok = mnesia:start(),

    Ret = mnesia:create_table(subscription,

    [

     {disc_copies, [node()]},

     {attributes, record_info(fields, subscription)},

     {index, [subscribee]}, %index subscribee too

     {type, bag}

    ]),

    Ret.

