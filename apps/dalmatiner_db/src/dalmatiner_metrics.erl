%%%-------------------------------------------------------------------
%%% @author Heinz Nikolaus Gies <heinz@licenser.net>
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc
%%%
%%% @end
%%% Created : 13 Jun 2014 by Heinz Nikolaus Gies <heinz@licenser.net>
%%%-------------------------------------------------------------------
-module(dalmatiner_metrics).

-behaviour(gen_server).
-include_lib("dproto/include/dproto.hrl").
-include_lib("mstore/include/mstore.hrl").


%% API
-export([start_link/0, inc/1, inc/0]).

-ignore_xref([start_link/0, inc/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(INTERVAL, 1000).
-define(BUCKET, <<"dalmatinerdb">>).
-define(COUNTERS_MPS, ddb_counters_mps).


-record(state, {dict, prefix}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).


inc() ->
    inc(1).

inc(N) ->
    try
        ets:update_counter(?COUNTERS_MPS, self(), N)
    catch
        error:badarg ->
            ets:insert(?COUNTERS_MPS, {self(), N})
    end,
    ok.


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    %% We want a high priority so we don't get scheduled back and have false
    %% reporting.
    process_flag(priority, high),
    ets:new(?COUNTERS_MPS,
            [named_table, set, public, {write_concurrency, true}]),
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    erlang:send_after(?INTERVAL, self(), tick),
    lager:info("[metrics] Initializing metric watcher with N: ~p, W: ~p at an "
               "interval of ~pms.", [N, W, ?INTERVAL]),
    Dict = bkt_dict:new(?BUCKET, N, W),
    {ok, #state{dict = Dict, prefix = list_to_binary(atom_to_list(node()))}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------

handle_info(tick, State = #state{prefix = Prefix, dict = Dict}) ->
    Dict1 = bkt_dict:update_chash(Dict),
    Time = timestamp(),
    Spec = folsom_metrics:get_metrics_info(),

    MPS = ets:tab2list(?COUNTERS_MPS),
    ets:delete_all_objects(?COUNTERS_MPS),
    P = lists:sum([Cnt || {_, Cnt} <- MPS]),

    Dict2 = add_to_dict([Prefix, <<"mps">>], Time, P, Dict1),
    Dict3 = do_metrics(Prefix, Time, Spec, Dict2),
    Dict4 = bkt_dict:flush(Dict3),

    erlang:send_after(?INTERVAL, self(), tick),
    {noreply, State#state{dict = Dict4}};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%do_metrics(CBin, Time, Specs, Acc) ->
do_metrics(_Prefix, _Time, [], Acc) ->
    Acc;

do_metrics(Prefix, Time, [{N, [{type, histogram}]} | Spec], Acc) ->
    Stats = folsom_metrics:get_histogram_statistics(N),
    Prefix1 = [Prefix, metric_name(N)],
    Acc1 = build_histogram(Stats, Prefix1, Time, Acc),
    do_metrics(Prefix, Time, Spec, Acc1);

do_metrics(Prefix, Time, [{N, [{type, spiral}]} | Spec], Acc) ->
    [{count, Count}, {one, One}] = folsom_metrics:get_metric_value(N),
    K = metric_name(N),
    Acc1 = add_metric(Prefix, [K, <<"count">>], Time, Count, Acc),
    Acc2 = add_metric(Prefix, [K, <<"one">>], Time, One, Acc1),
    do_metrics(Prefix, Time, Spec, Acc2);


do_metrics(Prefix, Time, [{N, [{type, counter}]} | Spec], Acc) ->
    Count = folsom_metrics:get_metric_value(N),
    Acc1 = add_metric(Prefix, N, Time, Count, Acc),
    do_metrics(Prefix, Time, Spec, Acc1);

do_metrics(Prefix, Time, [{N, [{type, duration}]} | Spec], Acc) ->
    Stats = folsom_metrics:get_metric_value(N),
    Prefix1 = [Prefix, metric_name(N)],
    Acc1 = build_histogram(Stats, Prefix1, Time, Acc),
    do_metrics(Prefix, Time, Spec, Acc1);


do_metrics(Prefix, Time, [{N, [{type, meter}]} | Spec], Acc) ->
    Prefix1 = [Prefix, metric_name(N)],
    [{count, Count},
     {one, One},
     {five, Five},
     {fifteen, Fifteen},
     {day, Day},
     {mean, Mean},
     {acceleration,
      [{one_to_five, OneToFive},
       {five_to_fifteen, FiveToFifteen},
       {one_to_fifteen, OneToFifteen}]}]
        = folsom_metrics:get_metric_value(N),
    Acc1 = add_metric(Prefix1, [<<"count">>], Time, Count, Acc),
    Acc2 = add_metric(Prefix1, [<<"one">>], Time, One, Acc1),
    Acc3 = add_metric(Prefix1, [<<"five">>], Time, Five, Acc2),
    Acc4 = add_metric(Prefix1, [<<"fifteen">>], Time, Fifteen, Acc3),
    Acc5 = add_metric(Prefix1, [<<"day">>], Time, Day, Acc4),
    Acc6 = add_metric(Prefix1, [<<"mean">>], Time, Mean, Acc5),
    Acc7 = add_metric(Prefix1, [<<"one_to_five">>], Time, OneToFive, Acc6),
    Acc8 = add_metric(Prefix1,
                      [<<"five_to_fifteen">>], Time, FiveToFifteen, Acc7),
    Acc9 = add_metric(Prefix1,
                      [<<"one_to_fifteen">>], Time, OneToFifteen, Acc8),
    do_metrics(Prefix, Time, Spec, Acc9).

add_metric(Prefix, Name, Time, Value, Acc) when is_integer(Value) ->
    add_to_dict([Prefix, metric_name(Name)], Time, Value, Acc);

add_metric(Prefix, Name, Time, Value, Acc) when is_float(Value) ->
    Scale = 1000*1000,
    add_to_dict([Prefix, metric_name(Name)], Time, round(Value*Scale), Acc).

add_to_dict(Metric, Time, Value, Dict) ->
    Metric1 = dproto:metric_from_list(lists:flatten(Metric)),
    bkt_dict:add(Metric1, Time, mmath_bin:from_list([Value]), Dict).

timestamp() ->
    {Meg, S, _} = os:timestamp(),
    Meg*1000000 + S.

metric_name(B) when is_binary(B) ->
    B;
metric_name(L) when is_list(L) ->
    erlang:list_to_binary(L);
metric_name(N1) when
      is_atom(N1) ->
    a2b(N1);
metric_name({N1, N2}) when
      is_atom(N1), is_atom(N2) ->
    [a2b(N1), a2b(N2)];
metric_name({N1, N2, N3}) when
      is_atom(N1), is_atom(N2), is_atom(N3) ->
    [a2b(N1), a2b(N2), a2b(N3)];
metric_name({N1, N2, N3, N4}) when
      is_atom(N1), is_atom(N2), is_atom(N3), is_atom(N4) ->
    [a2b(N1), a2b(N2), a2b(N3), a2b(N4)];
metric_name(T) when is_tuple(T) ->
    [metric_name(E) || E <- tuple_to_list(T)].

a2b(A) ->
    erlang:atom_to_binary(A, utf8).

% build_histogram(Stats, Prefix, Time, Acc)
build_histogram([], _Prefix, _Time, Acc) ->
    Acc;

build_histogram([{min, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"min">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{max, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"max">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{arithmetic_mean, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"arithmetic_mean">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{geometric_mean, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"geometric_mean">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{harmonic_mean, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"harmonic_mean">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{median, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"median">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{variance, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"variance">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{standard_deviation, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"standard_deviation">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{skewness, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"skewness">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{kurtosis, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"kurtosis">>, Time, round(V), Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([{percentile,
                  [{50, P50}, {75, P75}, {90, P90}, {95, P95}, {99, P99},
                   {999, P999}]} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"p50">>, Time, round(P50), Acc),
    Acc2 = add_metric(Prefix, <<"p75">>, Time, round(P75), Acc1),
    Acc3 = add_metric(Prefix, <<"p90">>, Time, round(P90), Acc2),
    Acc4 = add_metric(Prefix, <<"p95">>, Time, round(P95), Acc3),
    Acc5 = add_metric(Prefix, <<"p99">>, Time, round(P99), Acc4),
    Acc6 = add_metric(Prefix, <<"p999">>, Time, round(P999), Acc5),
    build_histogram(H, Prefix, Time, Acc6);

build_histogram([{n, V} | H], Prefix, Time, Acc) ->
    Acc1 = add_metric(Prefix, <<"count">>, Time, V, Acc),
    build_histogram(H, Prefix, Time, Acc1);

build_histogram([_ | H], Prefix, Time, Acc) ->
    build_histogram(H, Prefix, Time, Acc).
