-module(metadata_vnode).
-behaviour(riak_core_vnode).

-include_lib("riak_core/include/riak_core_vnode.hrl").
-include("metadata_vnode.hrl").


-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
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

update_index(Preflist, ReqID, Bucket, Metrics) ->
    riak_core_vnode_master:command(Preflist,
                                   {update_index, ReqID, Bucket, Metrics},
                                   {raw, ReqID, self()},
                                   ?MASTER).

repair_index(IdxNode, Bucket, Metrics) ->
    riak_core_vnode_master:command(IdxNode,
                                   {repair_index, Bucket, Metrics},
                                   ignore,
                                   ?MASTER).

init([Partition]) ->
    process_flag(trap_exit, true),
    random:seed(erlang:phash2([node()]),
                erlang:monotonic_time(),
                erlang:unique_integer()),
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, IO} = metric_io:start_link(Partition),
    {ok, MIdx} = metric_index:start_link(Partition),
    State = #state{
               partition = Partition,
               n = N,
               node = node(),
               io = IO,
               index = MIdx
              },
    {ok, State}.

handle_command({update_index, _ReqID, Bucket, Metrics}, _Sender,
               State=#state{index=MIdx}) ->
    ok = metric_index:update(MIdx, Bucket, Metrics),
    {reply, ok, State};

handle_command({get_index, ReqID, Bucket}, _Sender,
               State=#state{partition = Idx, index=MIdx}) ->
    {ok, Metrics1} = metric_index:get(MIdx, Bucket),
    {reply, {ok, ReqID, Idx, Metrics1}, State};

handle_command({repair_index, Bucket, Metrics}, _Sender,
               State=#state{index=MIdx}) ->
    ok = metric_index:repair(MIdx, Bucket, Metrics),
    {noreply, State}.

handle_handoff_command(_Message, _Sender, State) ->
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(_Data, State) ->
    {reply, ok, State}.

encode_handoff_item(_ObjectName, _ObjectValue) ->
    <<>>.

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_info({'EXIT', Index, normal}, State = #state{index = Index}) ->
    {ok, State};

handle_info({'EXIT', Index, E}, State = #state{index = Index}) ->
    {stop, E, State};

handle_info(_, State) ->
    {ok, State}.

handle_exit(Index, normal, State = #state{index = Index}) ->
    {ok, State};

handle_exit(Index, E, State = #state{index = Index}) ->
    {stop, E, State};

handle_exit(_PID, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, #state{index = _Index}) ->
    ok.
