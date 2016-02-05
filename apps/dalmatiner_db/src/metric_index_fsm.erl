%% @doc Coordinates incoming requests for a list of metrics.  It also enforces
%% the consistency semantics of N, R.
%% Anti-entropy services such as read-repair may also be performed in the case
%% that any differences are discovered.
-module(metric_index_fsm).
-behavior(gen_fsm).

-include("apps/metric_vnode/src/metadata_vnode.hrl").

-define(DEFAULT_TIMEOUT, 5000).

%% API
-export([start_link/4, get/2]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2, wait_for_n/2, finalize/2]).

-record(state, {req_id,
                from,
                bucket,
                r,
                n,
                preflist,
                num_r=0,
                size,
                timeout=?DEFAULT_TIMEOUT,
                vnode,
                system,
                replies=[]}).

-ignore_xref([
              code_change/4,
              different/1,
              execute/2,
              finalize/2,
              handle_event/3,
              handle_info/3,
              handle_sync_event/4,
              init/1,
              needs_repair/2,
              prepare/2,
              reconcile/1,
              repair/3,
              start_link/5,
              terminate/3,
              unique/1,
              wait_for_n/2,
              waiting/2,
              start/3
             ]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, VNodeInfo, From, Bucket) ->
    gen_fsm:start_link(?MODULE,
                       [ReqID, VNodeInfo, From, Bucket], []).

%% Gets the index for the specified bucket
get(VNodeInfo, Bucket) ->
    ReqID = mk_reqid(),
    metric_index_fsm_sup:start_index_fsm(
      [ReqID, VNodeInfo, self(), Bucket]),
    receive
        {ReqID, ok, Index} ->
            {ok, Index}
    after ?DEFAULT_TIMEOUT ->
            {error, timeout}
    end.

%%%===================================================================
%%% States
%%%===================================================================

%% Initialize state data and proceed to prepare state
init([ReqId, {VNode, System}, From, Bucket]) ->
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, R} = application:get_env(dalmatiner_db, r),
    State = #state{req_id=ReqId,
                   r=R,
                   n=N,
                   from=From,
                   vnode=VNode,
                   system=System,
                   bucket=Bucket},
    {ok, prepare, State, 0}.

%% @doc Calculate the preference list, which is the preferred set of vnodes
%% that should participate in the request.
prepare(timeout, State=#state{bucket=Bucket,
                              system=System,
                              n=N}) ->
    DocIdx = riak_core_util:chash_key({?METRIC_INDEX_BUCKET, Bucket}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, System),
    {next_state, execute, State#state{preflist=Preflist}, 0}.

%% @doc Execute the `get_index' operation Op, and place coordinator in a
%% waiting state
execute(timeout, State=#state{req_id=ReqId,
                              bucket=Bucket,
                              preflist=Preflist}) ->
    metadata_vnode:get_index(Preflist, ReqId, Bucket),
    {next_state, waiting, State}.

%% @doc Wait for R replies and then respond to From (original client). The
%% coordinator will remain in this state until R replies have been received.
%% A reply is sent to the client (get function) as soon as a canonical merged
%% copy is available from a quorum of R primaries.
waiting({ok, ReqID, IdxNode, ReplyIndex},
        State0=#state{from=From, num_r=NumR0, replies=Replies0,
                      r=R, n=N, timeout=Timeout}) ->
    NumR = NumR0 + 1,
    Replies = [{IdxNode, ReplyIndex}|Replies0],
    State = State0#state{num_r=NumR, replies=Replies},
    case NumR of
        Min when Min >= R ->
            Merged = merge(Replies),
            From ! {ReqID, ok, btrie:fetch_keys(Merged)},
            case NumR of
                N ->
                    {next_state, finalize, State, 0};
                _ ->
                    {next_state, wait_for_n, State, Timeout}
            end;
        _ ->
            {next_state, waiting, State}
    end.

%% Once N replies have been received and merged, the coordinator moves to a
%% finalize state
wait_for_n({ok, _ReqID, IdxNode, ReplyIndex},
           State=#state{n=N, num_r=NumR, replies=Replies0}) when NumR == N - 1 ->
    Replies = [{IdxNode, ReplyIndex}|Replies0],
    {next_state, finalize, State#state{num_r=N, replies=Replies}, 0};

%% Await the remaining replies from the replicas.  The reply would have already
%% been sent off the originally quorum replies.
wait_for_n({ok, _ReqID, IdxNode, ReplyIndex},
           State=#state{num_r=NumR0, replies=Replies0, timeout=Timeout}) ->
    NumR = NumR0 + 1,
    Replies = [{IdxNode, ReplyIndex}|Replies0],
    {next_state, wait_for_n, State#state{num_r=NumR, replies=Replies}, Timeout};

wait_for_n(timeout, State) ->
    {stop, timeout, State}.

finalize(timeout, SD=#state{
                        replies=Replies,
                        bucket=Bucket}) ->
    CanonicalIndex = merge(Replies),
    case needs_repair(CanonicalIndex, Replies) of
        true ->
            CanonicalKeys = btrie:fetch_keys(CanonicalIndex),
            repair(Bucket, CanonicalIndex, CanonicalKeys, Replies),
            {stop, normal, SD};
        false ->
            {stop, normal, SD}
    end.

handle_info(_Info, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_event(_Event, _StateName, StateData) ->
    {stop, badmsg, StateData}.

handle_sync_event(_Event, _From, _StateName, StateData) ->
    {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _State) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @pure
%%
%% @doc Given the merged object `CanonicalIndex' and a list of `Replies',
%% determine if repair is needed.
needs_repair(CanonicalIndex, Replies) ->
    Indices = [ ReplyIndex || {_, ReplyIndex} <- Replies],
    lists:any(different(CanonicalIndex), Indices).

%% @pure
different(A) -> fun(B) -> A =/= B end.

%% @impure
%%
%% @doc Repair any vnodes that do not have the correct object.
repair(_, _, _, []) -> ok;

repair(Bucket, CanonicalIndex, CanonicalKeys, [{IdxNode, Index}|Replies]) ->
    case CanonicalIndex == Index of
        true ->
            repair(Bucket, CanonicalIndex, CanonicalKeys, Replies);
        false ->
            metadata_vnode:repair_index(IdxNode, Bucket, CanonicalKeys),
            repair(Bucket, CanonicalIndex, CanonicalKeys, Replies)
    end.

%% @pure
%% Merge the reply with the canonical index, where the btries do not have
%% values associated with the metric names.
merge(Replies) when is_list(Replies) ->
    Indices = [Index || {_, Index} <- Replies, Index =/= undefined],
    Seed = btrie:new(),
    lists:foldl(fun(Index, Acc) ->
                        btrie:merge(fun(_, _, _) -> t end, Index, Acc)
                end, Seed, Indices).

mk_reqid() ->
    erlang:unique_integer().
