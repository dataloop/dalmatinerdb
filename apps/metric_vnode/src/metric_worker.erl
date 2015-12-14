-module(metric_worker).

-behaviour(riak_core_vnode_worker).

-export([init_worker/3,
         handle_work/3]).

-record(state, {index}).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Initialize the worker. Currently only the VNode index
%% parameter is used.
init_worker(VNodeIndex, _Args, _Props) ->
    {ok, #state{index=VNodeIndex}}.

%% @doc Perform the asynchronous fold operation.
%% This could involve a lot of I/O that is why it is run here
handle_work({fold, FoldFun, FinishFun}, _Sender, State) ->
    try
        %% Try and add some kind of tracing ot the output of the foldFun here
        %% Print self() to get the process id of the current worker.  THis is
        %% useful to associate the values of the FoldFun, to associate what
        %% happens with the particular worker.
        FinishFun(FoldFun())
    catch
        receiver_down -> ok;
        stop_fold -> ok
    end,
    {noreply, State}.
