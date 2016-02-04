%% @doc Supervise the metric_index FSM.
-module(metric_index_fsm_sup).
-behavior(supervisor).

-export([start_index_fsm/1,
         start_link/0]).

-export([init/1]).

-ignore_xref([init/1,
              start_link/0]).

start_index_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    IndexFsm =
        {undefined,
         {metric_index_fsm, start_link, []},
         temporary, 5000, worker, [metric_index_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [IndexFsm]}}.
