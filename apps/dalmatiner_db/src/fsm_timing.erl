%% Common code for timing fsm states, inspired by riak_kv.
-module(fsm_timing).

-export([add_timing/2]).

%% @doc Add timing information of `{StateInfo, erlang:system_time(), Delta}' to
%% the state transition timings, stored in reverse order.
%% The `Delta' is the difference in milli_seconds between now and the previous
%% state transition.
add_timing(StateInfo, [T1|Timings])
  when is_list(Timings) ->
    {_StateInfo, PreviousTs, _Delta} = T1,
    Ts = erlang:system_time(milli_seconds),
    T0 = {StateInfo, Ts, Ts - PreviousTs},
    [T0,T1|Timings];
add_timing(StateInfo, Timings) when is_list(Timings) ->
    T1 = {StateInfo, erlang:system_time(milli_seconds), 0},
    [T1|Timings].
