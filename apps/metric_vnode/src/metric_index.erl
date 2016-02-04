%%%-------------------------------------------------------------------
%%% @doc the local view of all metrics for a bucket
%%%
%%% The metric index avoids the cost of coverage queries where possible to
%%% improve the cost of list operations.
%%% The index is a `btrie' stored per Bucket, as space occupancy is minimized
%%% by common prefix sharing among nodes.  As well as being permormant, the
%%% `btrie' preserves ordering. An SBF (scalable bloom filter) `bloom' is used
%%% to store previously seen {Bucket, Metric} pairs to avoid tree traversal
%%% and updates where possible.
%%% This allows for minimal overhead in managing the index.
%%% Finally, all indexes and bucket pairs are stored using `gb_trees', forming
%%% a forest.  `gb_trees' allow for efficient lookups that are comparable to
%%% ETS lookups in efficiency.
%%% @end
%%%-------------------------------------------------------------------
-module(metric_index).

-behaviour(gen_server).

%% API
-export([start_link/1, propagate_metric/4, update/3, sync/3, get/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

%% SDF Bloom filter settings
-define(INITIAL_CAPACITY, 100000).

%% TODO: Enhance the state to use an entry:
%%
%% -record(entry, {
%%           last_sync_ts,
%%           bloom,
%%           metrics = btrie:new()
%%          }).
%%
%% -opaque entry() :: #entry{}.
-record(state, {
          partition,
          bloom,
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

sync(Pid, Bucket, Metrics) ->
    gen_server:cast(Pid, {sync, Bucket, Metrics}).

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
    {ok, #state{ partition = Partition,
                 bloom = bloom:sbf(?INITIAL_CAPACITY) }}.

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

%% Include any prefix search here
handle_call({get, Bucket}, _From, State=#state{indices = Indices}) ->
    Index = case gb_trees:lookup(Bucket, Indices) of
        {value, Index0} ->
            Index0;
        none ->
            btrie:new()
    end,
    {reply, {ok, Index}, State};

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
handle_cast({update, Bucket, Metric}, State) ->
    State1 = do_update(Bucket, Metric, State),
    {noreply, State1};

handle_cast({propagate_metric, Bucket, Metric, N}, State) ->
    ok = do_propagate_metric(Bucket, Metric, N),
    {noreply, State};

handle_cast({sync, Bucket, Metrics}, State) ->
    State1 = do_sync(Bucket, Metrics, State),
    {noreply, State1};

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

do_sync(Bucket, Metrics, State) when is_list(Metrics) ->
    AggrState = lists:foldl(
                  fun (M, S) ->
                          do_update(Bucket, M, S)
                  end, State, Metrics),
    AggrState.

do_update(Bucket, Metric, State=#state{bloom=BF})
  when is_binary(Bucket), is_binary(Metric) ->
    Entry = <<Bucket/binary, Metric/binary>>,

    case bloom:member(Entry, BF) of
        true ->
            State;
        false ->
            %% A state monad could be useful to compose these S/E functions
            {ok, State2} = update_bloom(BF, Entry, State),
            {ok, State3} = update_index(Bucket, Metric, State2),
            State3
        end.

update_bloom(BF, Entry, State) ->
    BF1 = bloom:add(Entry, BF),
    {ok, State#state{bloom = BF1}}.

%% TODO: Due to a small false positive probability in the bloom, the metric may
%% already exist in its corresponding btrie. The btrie operation should guard
%% against this possibility.
update_index(Bucket, Metric, State=#state{indices=Indices}) ->
    Indices1 = case gb_trees:lookup(Bucket, Indices) of
                   {value, Index} ->
                       Trie = btrie:store(Metric, Index),
                       gb_trees:update(Bucket, Trie, Indices);
                   none ->
                       Trie = btrie:new([Metric]),
                       gb_trees:insert(Bucket, Trie, Indices)
               end,
    {ok, State#state{indices = Indices1}}.


%% Send an update to the index on replicas.  The operation is asynchronous, due
%% to the cast operation. Normally, commands in riak_core are asynchronous
%% already.  This extra level of indirection ensures that the master is not
%% blocked, however, which seems to add additional latency to writes.
%% TODO: Should the write_coordinator ensure that the update happens on W
%% replicas?
%% TODO: Should an alternative command be specified via the master?
%% http://lists.basho.com/pipermail/riak-users_lists.basho.com/2011-December/006982.html
do_propagate_metric(Bucket, Metric, N) ->
    DocIdx = riak_core_util:chash_key({Bucket, Bucket}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, metric),
    ReqID = make_ref(),
    metric_vnode:update_index(Preflist, ReqID, Bucket, Metric),
    ok.
