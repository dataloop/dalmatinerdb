%%%-------------------------------------------------------------------
%%% @doc the local view of all metrics for a bucket
%%%
%%% The metric index avoids the cost of coverage queries where possible to
%%% improve the cost of list operations.
%%% The index is a `btrie' stored per Bucket, as space occupancy is minimized
%%% by common prefix sharing among nodes.  As well as being permormant, the
%%% `btrie' preserves ordering. 
%%% All indexes and bucket pairs are stored using `gb_trees', forming
%%% a forest. `gb_trees' allow for efficient lookups that are comparable to
%%% ETS lookups in efficiency.
%%%
%%% TODO: Add quickcheck tests for this module
%%% @end
%%%-------------------------------------------------------------------
-module(metric_index).

-behaviour(gen_server).

%% API
-export([start_link/1, propagate_metric/4, update/3, get/2, repair/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).
-define(INDEX_BUCKET, <<"metric_index">>).

-record(index, {
          last_repair_ts,
          metrics }).

-record(state, {
          partition,
          indices=gb_trees:empty()
         }).

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
start_link(Partition) ->
    gen_server:start_link(?MODULE, [Partition], []).

get(Pid, Bucket) ->
    gen_server:call(Pid, {get, Bucket}).

update(Pid, Bucket, Metric) ->
    gen_server:cast(Pid, {update, Bucket, Metric}).

propagate_metric(Pid, Bucket, Metric, N) ->
    gen_server:cast(Pid, {update, Bucket, Metric, N}).

repair(Pid, Bucket, MetricKeys) ->
    gen_server:cast(Pid, {repair, Bucket, MetricKeys}).

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
init([Partition]) ->
    process_flag(trap_exit, true),
    {ok, #state { partition = Partition }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Gets the list of metrics for the specified `Bucket', returns `undefined' 
%% in the case that no index exists.
%% @end
%%--------------------------------------------------------------------
handle_call({get, Bucket}, _From, State=#state{indices = Indices}) ->
    Idx = case gb_trees:lookup(Bucket, Indices) of
              {value, Idx0} ->
                  Idx0;
              none ->
                  undefined
          end,
    {reply, {ok, Idx}, State};

handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Updates the metrics index for `Bucket' with the given `Metric'.
%% @end
%%--------------------------------------------------------------------
handle_cast({update, Bucket, Metric}, State) ->
    State1 = do_update(Bucket, Metric, State),
    {noreply, State1};

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Sends the bucket/metric pair to all metadata vnodes on the preference list
%% @end
%%--------------------------------------------------------------------
handle_cast({propagate_metric, Bucket, Metric, N}, State) ->
    ok = do_propagate_metric(Bucket, Metric, N),
    {noreply, State};

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Replacs the current index for the `Bucket' with a quorum index.
%% @end
%%--------------------------------------------------------------------
handle_cast({repair, Bucket, MetricKeys}, State) ->
    {ok, State1} = do_repair(Bucket, MetricKeys, State),
    {noreply, State1};

handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%% @end
%%--------------------------------------------------------------------
handle_info({'EXIT', _From, _Reason}, State) ->
    {stop, normal, State};

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

do_repair(Bucket, MetricKeys, State0=#state{indices=Indices0})
  when is_list(MetricKeys) ->
    I0 = empty_index(),
    Metrics = btrie:new(MetricKeys),
    Idx = I0#index{ metrics = Metrics },
    Indices = gb_trees:enter(Bucket, Idx, Indices0),
    {ok, State0#state{indices = Indices}}.

do_update(Bucket, Metric, State0=#state{indices=Indices0})
  when is_binary(Bucket), is_binary(Metric) ->
    Idx1 = case gb_trees:lookup(Indices0, Bucket) of
               {value, Idx0} ->
                   Idx0;
               none ->
                   empty_index()
           end,

    Metrics = update_metrics(Metric, Idx1#index.metrics),
    Idx = Idx1#index{ metrics = Metrics },
    Indices = gb_trees:enter(Bucket, Idx, Indices0),
    {ok, State0#state{indices = Indices}}.

update_metrics(Metric, Metrics) ->
    case btrie:is_key(Metric, Metrics) of
        true ->
            Metrics;
        false ->
            btrie:store(Metric, Metrics)
    end.

%% @private
%% @doc Send an update to the index on replicas.  The operation is 
%% asynchronous, due to the cast operation. Normally, commands in riak_core 
%% are asynchronous already.  This extra level of indirection ensures that the
%% master is not blocked, however, which seems to add additional latency to 
%% writes.
%% TODO: Should the write_coordinator ensure that the update happens on W
%% replicas?
%% TODO: Should an alternative command be specified via the master?
%% http://lists.basho.com/pipermail/riak-users_lists.basho.com/2011-December/006982.html
do_propagate_metric(Bucket, Metric, N) ->
    DocIdx = riak_core_util:chash_key({?INDEX_BUCKET, Bucket}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, metric_metadata),
    ReqID = make_ref(),
    metric_vnode:update_index(Preflist, ReqID, Bucket, Metric),
    ok.

%% @private
%% @doc
%% Create a new index for the given bucket.
%% During a repair, the old values are discarded which may result in a lot of
%% garbage.
empty_index() ->
    #index{
          last_repair_ts = erlang:system_time(milli_seconds),
          metrics = btrie:new() }.
