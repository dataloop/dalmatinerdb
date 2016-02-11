%% @doc The coordinator for stat get operations.  The key here is to
%% generate the preflist just like in wrtie_fsm and then query each
%% replica and wait until a quorum is met.
-module(dalmatiner_read_fsm).
-behavior(gen_fsm).

-define(DEFAULT_TIMEOUT, 5000).

%% API
-export([start_link/6, start/2, start/3, start/4]).


-export([reconcile/1, different/1, needs_repair/2, unique/1]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2, wait_for_n/2, finalize/2]).

-record(state, {req_id,
                from,
                entity,
                op,
                r,
                n,
                preflist,
                num_r=0,
                size,
                timeout=?DEFAULT_TIMEOUT,
                val,
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
              repair/5,
              start/2,
              start/4,
              start_link/6,
              terminate/3,
              unique/1,
              wait_for_n/2,
              waiting/2,
              start/3
             ]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, {VNode, System}, Op, From, Entity, Val) ->
    gen_fsm:start_link(?MODULE,
                       [ReqID, {VNode, System}, Op, From, Entity, Val], []).

start(VNodeInfo, Op) ->
    start(VNodeInfo, Op, undefined).

start(VNodeInfo, Op, User) ->
    start(VNodeInfo, Op, User, undefined).

start(VNodeInfo, Op, User, Val) ->
    ReqID = mk_reqid(),
    dalmatiner_read_fsm_sup:start_read_fsm(
      [ReqID, VNodeInfo, Op, self(), User, Val]
     ),
    receive
        {ReqID, ok} ->
            ok;
        {ReqID, ok, {Res, Data}} ->
            {ok, Res, Data};
        {ReqID, ok, Res} ->
            {ok, Res}
    after ?DEFAULT_TIMEOUT ->
            {error, timeout}
    end.

%%%===================================================================
%%% States
%%%===================================================================

%% Intiailize state data.
init([ReqId, {VNode, System}, Op, From]) ->
    init([ReqId, {VNode, System}, Op, From, undefined, undefined]);

init([ReqId, {VNode, System}, Op, From, Entity]) ->
    init([ReqId, {VNode, System}, Op, From, Entity, undefined]);

init([ReqId, {VNode, System}, Op, From, Entity, Val]) ->
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, R} = application:get_env(dalmatiner_db, r),
    SD = #state{req_id=ReqId,
                r=R,
                n=N,
                from=From,
                op=Op,
                val=Val,
                vnode=VNode,
                system=System,
                entity=Entity},
    {ok, prepare, SD, 0}.

%% @doc Calculate the Preflist.
prepare(timeout, SD0=#state{entity={B, M},
                            system=System,
                            n=N}) ->

    DocIdx = riak_core_util:chash_key({B, M}),
    Prelist = riak_core_apl:get_apl(DocIdx, N, System),
    SD = SD0#state{preflist=Prelist},
    {next_state, execute, SD, 0}.

%% @doc Execute the get reqs.
execute(timeout, SD0=#state{req_id=ReqId,
                            entity=Entity,
                            op=Op,
                            val=Val,
                            preflist=Prelist}) ->
    case Entity of
        undefined ->
            metric_vnode:Op(Prelist, ReqId);
        {Bucket, {Metric, _}} ->
            case Val of
                undefined ->
                    metric_vnode:Op(Prelist, ReqId, {Bucket, Metric});
                _ ->

                    metric_vnode:Op(Prelist, ReqId, {Bucket, Metric}, Val)
            end
    end,
    {next_state, waiting, SD0}.

%% @doc Wait for R replies and then respond to From (original client
%% that called `get/2').
%% `IdxNode' is a 2-tuple, {Partition, Node}, referring to the origin of this
%% reply
%% TODO: read repair...or another blog post?

waiting({ok, ReqID, IdxNode, Obj},
        SD0=#state{from=From, num_r=NumR0, replies=Replies0,
                   r=R, n=N, timeout=Timeout}) when is_tuple(IdxNode) ->
    NumR = NumR0 + 1,
    Replies = [{IdxNode, Obj}|Replies0],
    SD = SD0#state{num_r=NumR, replies=Replies},
    case NumR of
        Min when Min >= R ->
            case merge(Replies) of
                not_found ->
                    From ! {ReqID, ok, not_found};
                Merged ->
                    From ! {ReqID, ok, Merged}
            end,
            case NumR of
                N ->
                    {next_state, finalize, SD, 0};
                _ ->
                    {next_state, wait_for_n, SD, Timeout}
            end;
        _ ->
            {next_state, waiting, SD}
    end.

wait_for_n({ok, _ReqID, IdxNode, Obj},
           SD0=#state{n=N, num_r=NumR, replies=Replies0})
  when NumR == N - 1, is_tuple(IdxNode) ->
    Replies = [{IdxNode, Obj}|Replies0],
    {next_state, finalize, SD0#state{num_r=N, replies=Replies}, 0};

wait_for_n({ok, _ReqID, IdxNode, Obj},
           SD0=#state{num_r=NumR0, replies=Replies0, timeout=Timeout})
  when is_tuple(IdxNode) ->
    NumR = NumR0 + 1,
    Replies = [{IdxNode, Obj}|Replies0],
    {next_state, wait_for_n, SD0#state{num_r=NumR, replies=Replies}, Timeout};

%% TODO partial repair?
wait_for_n(timeout, SD) ->
    {stop, timeout, SD}.

finalize(timeout, SD=#state{
                        val = {Time, _},
                        replies=Replies,
                        entity=Entity}) ->
    MObj = merge(Replies),
    case needs_repair(MObj, Replies) of
        true ->
            repair(Time, Entity, MObj, Replies),
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

terminate(_Reason, _SN, _SD) ->
    ok.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @pure
%%
%% @doc Given a list of `Replies' return the merged value.
merge([{_, {R, _}} | _] = Replies) ->
    case [Data || {_, {_, Data}} <- Replies, is_binary(Data)] of
        [] ->
            not_found;
        Ds ->
            Ress = [Resolution || {_, {Resolution, _}} <- Replies],
            case lists:any(different(R), Ress) of
                true ->
                    lager:error("[merge] Resolution mismatch: ~p /= ~p",
                                [R, Ress]),
                    not_found;
                false ->
                    {R, mmath_comb:merge(Ds)}
            end
    end;
merge(_) ->
    not_found.

%% @pure
%%
%% @doc Reconcile conflicts among conflicting values.
-spec reconcile([A :: ordsets:ordset()]) -> A :: ordsets:ordset().

reconcile(Vals) ->
    merge(Vals).

%% @pure
%%
%% @doc Given the merged object `MObj' and a list of `Replies'
%% determine if repair is needed.
needs_repair(MObj, Replies) ->
    Objs = [Obj || {_, Obj} <- Replies],
    lists:any(different(MObj), Objs).

%% @pure
different(A) -> fun(B) -> A =/= B end.

%% @impure
%%
%% @doc Repair any vnodes that do not have the correct object.
repair(_, _, _, []) -> ok;

repair(Time, {Bkt, {Met, _}} = MetAndTime, MObj, [{IdxNode, Obj}|T]) ->
    case MObj == Obj of
        true ->
            repair(Time, MetAndTime, MObj, T);
        false ->
            {_, Data} = MObj,
            metric_vnode:repair(IdxNode, {Bkt, Met}, {Time, Data}),
            repair(Time, MetAndTime, MObj, T)
    end.

%% pure
%%
%% @doc Given a list return the set of unique values.
-spec unique([A::any()]) -> [A::any()].
unique(L) ->
    sets:to_list(sets:from_list(L)).

mk_reqid() ->
    erlang:unique_integer().
