%%%-------------------------------------------------------------------
%%% @doc A utility to assist with read repair verificatio in ddb
%%% @end
%%%-------------------------------------------------------------------
-module(read_repair).
-behaviour(gen_server).

-include_lib("dproto/include/dproto.hrl").

% API
-export([start_link/0,
         get_state/0,
         set_state/1,
         read/1,
         write/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-ignore_xref([
         start_link/0,
         get_state/0,
         set_state/1,
         read/1,
         write/2,
         init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%% the number of points to keep in the metric vnode ets cache
-define(CP, 20).
-define(HOST, "localhost").
-define(DELAY, 2).
-define(SERVER, ?MODULE).
-define(SYSTEM, "metric").
-define(BUCKET, <<"read_repair_bucket">>).
-define(METRIC, <<4, "read", 6, "repair", 6, "metric">>).
-define(EPOCH, 62167219200).

-type socket() :: port().

-record(ddb_connection,
        {socket :: socket() | undefined,
         host,
         port,
         mode = normal,
         bucket,
         error = none,
         delay = 1,
         batch = false}).

-record(state, {sentinelTime = 0}).

%%--------------------------------------------------------------------
%% @doc
%%
%% CASE 1
%% Cluster: No cluster
%% Write: |CP| odd values on node 1 at time=T
%% Write: |CP| even values on node 2 at time=T
%%
%% Cluster: N=2, R=2, W=1
%% Read: |CP| values at time=T
%%
%% @end
%%--------------------------------------------------------------------

%%%===================================================================
%%% API
%%%===================================================================
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

get_state() ->
    gen_server:call(?SERVER, get_state).

set_state(Time) ->
    gen_server:call(?SERVER, {set_state, Time}).

write(NodeN, Parity) ->
    gen_server:call(?SERVER, {write, NodeN, Parity}).

read(NodeN) ->
    gen_server:call(?SERVER, {read, NodeN, canonical}).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    {ok, #state{sentinelTime = get_timestamp()}}.

%%--------------------------------------------------------------------
%% @doc Gets the internal state of this server, used for inspection purposes.
%% @end
%%--------------------------------------------------------------------
handle_call(get_state, _From, State) ->
    Reply = State,
    {reply, Reply, State};

handle_call({set_state, Time}, _From, State) ->
    State1 = State#state{sentinelTime = Time},
    Reply = State1,
    {reply, Reply, State1};

%%--------------------------------------------------------------------
%% @doc Gets the preflist for the Key = {B, M} -used for inspection purposes.
%% @end
%%--------------------------------------------------------------------
%% handle_call(get_preflist, _From, State) ->
%%     DocIdx = riak_core_util:chash_key({?BUCKET, ?METRIC}),
%%     Preflist = riak_core_apl:get_primary_apl(DocIdx, 1, ?SYSTEM),
%%     Reply = Preflist,
%%     {reply, Reply, State};

handle_call({write, NodeN, Parity}, _From, State) ->
    Reply = run(write, NodeN, Parity, State),
    {reply, Reply, State};

handle_call({read, NodeN, Parity}, _From, State) ->
    Reply = run(read, NodeN, Parity, State),
    {reply, Reply, State};

%% No-op
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
%%%
open_conn(NodeN) ->
    Port =
        case NodeN of
            1 -> 5155;
            2 -> 5255;
            3 -> 5355;
            4 -> 5455
        end,
    {ok, Conn} = ddb_tcp:connect(?HOST, Port),
    Conn.

close_conn(Conn) ->
    ddb_tcp:close(Conn).

run(Op, NodeN, Parity, #state{sentinelTime = Time}) ->
    Points = get_points(Parity),
    case Op of
        read -> get(NodeN, Time);
        write -> send(NodeN, Time, Points)
    end.

send(NodeN, Time, Points) ->
    Conn = open_conn(NodeN),
    {ok, SConn} = ddb_tcp:stream_mode(?BUCKET, ?DELAY, Conn),
    PointsB = mmath_bin:from_list(Points),
    ddb_tcp:send(?METRIC, Time, PointsB, SConn),
    Sock = SConn#ddb_connection.socket,
    gen_tcp:send(Sock, <<6>>), %% ensure connection is flushed
    close_conn(Conn),
    Points.

get(NodeN, Time) ->
    Conn = open_conn(NodeN),
    {ok, {_Res, PointsB}, _Con} = ddb_tcp:get(?BUCKET,
                                              ?METRIC,
                                              Time,
                                              ?CP, Conn),
    close_conn(Conn),
    mmath_bin:to_list(PointsB).

get_timestamp() ->
    %% Timestamp in seconds as unix epoch
    Datetime = calendar:local_time(),
    calendar:datetime_to_gregorian_seconds(Datetime) - ?EPOCH.

get_points(Parity) ->
    case Parity of
        even -> gen_even();
        odd -> gen_odd();
        canonical -> gen_canonical()
    end.

gen_canonical() ->
    lists:seq(1, ?CP).

gen_even() ->
    [0,2,0,4,0,6,0,8,0,10,0,12,0,14,0,16,0,18,0,20].

gen_odd() ->
    [1,0,3,0,5,0,7,0,9,0,11,0,13,0,15,0,17,0,19,0].
