-module(metadata_vnode).
-behaviour(riak_core_vnode).

-include_lib("riak_core/include/riak_core_vnode.hrl").


-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_info/2,
         handle_exit/3]).

-export([get_index/3, update_index/4, repair_index/3]).

-ignore_xref([
              start_vnode/1,
              update_index/4,
              repair_index/3,
              handle_info/2
             ]).

-record(state, {
          partition,
          n,
          w,
          node,
          io,
          index
         }).

-define(MASTER, metadata_vnode_master).
-define(MAX_Q_LEN, 20).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

get_index(Preflist, ReqID, Bucket) ->
    riak_core_vnode_master:command(Preflist,
                                   {get_index, ReqID, Bucket},
                                   {fsm, undefined, self()},
                                   ?MASTER).

update_index(Preflist, ReqID, Bucket, Metric) ->
    riak_core_vnode_master:command(Preflist,
                                   {update_index, ReqID, Bucket, Metric},
                                   {raw, ReqID, self()},
                                   ?MASTER).

repair_index(IdxNode, Bucket, Index) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair_index, Bucket, Index},
                                   ignore,
                                   ?MASTER).

init([Partition]) ->
    Timestamp = erlang:system_time(milli_seconds),
    process_flag(trap_exit, true),
    random:seed(erlang:phash2([node()]),
                erlang:monotonic_time(),
                erlang:unique_integer()),
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    {ok, IO} = metric_io:start_link(Partition),
    {ok, MIdx} = metric_index:start_link(Partition),
    {ok, #state{
            now = Timestamp,
            partition = Partition,
            n = N,
            w = W,
            node = node(),
            io = IO,
            index = MIdx,
           },
     [FoldWorkerPool]}.

handle_command({update_index, _ReqID, Bucket, Metric}, _Sender,
               State=#state{index=MIdx}) ->
    ok = metric_index:update(MIdx, Bucket, Metric),
    {reply, ok, State};

handle_command({get_index, ReqID, Bucket}, _Sender,
               State=#state{partition = Idx, index=MIdx}) ->
    {ok, Metrics} = metric_index:get(MIdx, Bucket),
    {reply, {ok, ReqID, Idx, Metrics}, State};

handle_handoff_command(?FOLD_REQ{foldfun=Fun, acc0=Acc0}, Sender,
                       State=#state{tbl=T, io = IO}) ->
    ets:foldl(fun({{Bucket, Metric}, Start, Size, _, Array}, _) ->
                      Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
                      k6_bytea:delete(Array),
                      metric_io:write(IO, Bucket, Metric, Start, Bin)
              end, ok, T),
    ets:delete_all_objects(T),
    FinishFun =
        fun(Acc) ->
                riak_core_vnode:reply(Sender, Acc)
        end,
    case metric_io:fold(IO, Fun, Acc0) of
        {ok, AsyncWork} ->
            {async, {fold, AsyncWork, FinishFun}, Sender, State};
        empty ->
            {async, {fold, fun() -> Acc0 end, FinishFun}, Sender, State}
    end;

%% We want to forward all the other handoff commands
handle_handoff_command(_Message, _Sender, State) ->
    {forward, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {{Bucket, Metric}, ValList} = binary_to_term(Data),
    true = is_binary(Bucket),
    true = is_binary(Metric),
    State1 = lists:foldl(fun ({T, Bin}, StateAcc) ->
                                 do_put(Bucket, Metric, T, Bin, StateAcc, 2)
                         end, State, ValList),
    {reply, ok, State1}.

encode_handoff_item(Key, Value) ->
    term_to_binary({Key, Value}).

handle_coverage({metrics, Bucket}, _KS, Sender, State = #state{io = IO}) ->
    AsyncWork = fun() ->
                        {ok, Ms} = metric_io:metrics(IO, Bucket),
                        Ms
                end,
    FinishFun = fun(Data) ->
                        reply(Data, Sender, State)
                end,
    {async, {fold, AsyncWork, FinishFun}, Sender, State};

handle_info({'EXIT', IO, normal}, State = #state{io = IO}) ->
    {ok, State};

handle_info({'EXIT', IO, E}, State = #state{io = IO}) ->
    {stop, E, State};

handle_info(_, State) ->
    {ok, State}.

handle_exit(IO, normal, State = #state{io = IO}) ->
    {ok, State};

handle_exit(IO, E, State = #state{io = IO}) ->
    {stop, E, State};

handle_exit(_PID, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{tbl = T, io = IO}) ->
    ets:foldl(fun({{Bucket, Metric}, Start, Size, _, Array}, _) ->
                      Bin = k6_bytea:get(Array, 0, Size * ?DATA_SIZE),
                      k6_bytea:delete(Array),
                      metric_io:write(IO, Bucket, Metric, Start, Bin)
              end, ok, T),
    ets:delete(T),
    metric_io:close(IO),
    ok.

reply(Reply, {_, ReqID, _} = Sender, #state{node=N, partition=P}) ->
    riak_core_vnode:reply(Sender, {ok, ReqID, {P, N}, Reply}).
