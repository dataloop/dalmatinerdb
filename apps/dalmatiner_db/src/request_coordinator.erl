%%%-------------------------------------------------------------------
%%% @copyright (C) 2014, Heinz Nikolaus Gies
%%% @doc the local view of all metrics for a bucket
%%%
%%% Typically, there are many more writes than reads in any metric store. It is
%%% not performant for tens of thousands of metrics to spawn a write FSM, which
%%% is typically spawned per write request.  An alternative would be to have a
%%% pool of FSMs, but this also has a serious impact on performance.
%%%
%%% This module coordinates a request among W nodes, and fulfills the
%%% purpose of an FSM.
%%% @end
%%%-------------------------------------------------------------------
-module(request_coordinator).

-export([do_wait/2]).

do_wait(0, _ReqID) ->
    ok;

%% Block in receive loop for responses.  The pattern inside the receive block
%% will only match for RequestIDs that match the RequestID in the function
%% header.
do_wait(W, ReqID) ->
    receive
        {ReqID, ok} ->
            do_wait(W - 1, ReqID)
    after
        5000 ->
            {error, timeout}
    end.
