-module(metric).

-include("apps/metric_vnode/src/metadata_vnode.hrl").

-export([
         put/4,
         mput/4,
         get/4,
         get/5,
         list/0,
         list/1,
         list/2,
         update_ttl/2
        ]).

-ignore_xref([update_ttl/2, get/4, put/4]).

mput(Nodes, Acc, {Bucket, Metrics, N}, W) ->
    update_metadata(Bucket, sets:to_list(Metrics), N),
    folsom_metrics:histogram_timed_update(
      mput, dict, fold,
      [fun(DocIdx, Data, ok) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Data, W);
          (DocIdx, Data, R) ->
               do_mput(orddict:fetch(DocIdx, Nodes), Data, W),
               R
       end, ok, Acc]).

put(Bucket, Metric, Time, Value) ->
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    PPF = dalmatiner_opt:ppf(Bucket),
    folsom_metrics:histogram_timed_update(
      put,
      fun() ->
              put(Bucket, Metric, PPF, Time, Value, N, W)
      end).

put(Bucket, Metric, PPF, Time, Value, N, W) ->
    do_put(Bucket, Metric, PPF, Time, Value, N, W).

get(Bucket, Metric, Time, Count) ->
    get(Bucket, Metric, dalmatiner_opt:ppf(Bucket), Time, Count).

get(Bucket, Metric, PPF, Time, Count) when
      Time div PPF =:= (Time + Count - 1) div PPF->
    folsom_metrics:histogram_timed_update(
      get, dalmatiner_read_fsm, start,
      [{metric_vnode, metric}, get, {Bucket, {Metric, Time div PPF}},
       {Time, Count}]).

update_ttl(Bucket, TTL) ->
    dalmatiner_opt:set([<<"buckets">>, Bucket, <<"lifetime">>], TTL),
    metric_coverage:start({update_times, Bucket}).

list() ->
    folsom_metrics:histogram_timed_update(
      list_buckets, metric_coverage, start, [list]).

list(Bucket) ->
    VNodeInfo = {metadata_vnode, metric_metadata},
    case metric_index_fsm:get(VNodeInfo, Bucket) of
        {ok, []} ->
            {ok, SyncList} =
                folsom_metrics:histogram_timed_update(
                  list_metrics, metric_coverage, start, [{metrics, Bucket}]),

            %% Sync the coverage index back to all the N replicas
            %% TODO: Clean up duplication, find the correct module where this
            %% function should reside. Should it be the responsibility of the
            %% FSM itself?
            DocIdx = riak_core_util:chash_key({?METRIC_INDEX_BUCKET, Bucket}),
            {ok, N} = application:get_env(dalmatiner_db, n),
            Preflist = riak_core_apl:get_apl(DocIdx, N, metric_metadata),
            metadata_vnode:repair_index(Preflist, Bucket, SyncList),

            {ok, SyncList};
        {ok, Metrics} ->
            {ok, Metrics}
    end.

list(Bucket, Prefix) ->
    folsom_metrics:histogram_timed_update(
      list_metrics, metric_coverage, start, [{metrics, Bucket, Prefix}]).

update_metadata(Bucket, Metrics, N) when is_list(Metrics) ->
    DocIdx = riak_core_util:chash_key({?METRIC_INDEX_BUCKET, Bucket}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, metric_metadata),
    ReqID = make_ref(),
    metadata_vnode:update_index(Preflist, ReqID, Bucket, Metrics),
    ok.

do_put(Bucket, Metric, PPF, Time, Value, N, W) ->
    update_metadata(Bucket, [Metric], N),
    DocIdx = riak_core_util:chash_key({Bucket, {Metric, Time div PPF}}),
    Preflist = riak_core_apl:get_apl(DocIdx, N, metric),
    ReqID = make_ref(),
    metric_vnode:put(Preflist, ReqID, Bucket, Metric, {Time, Value}),
    request_coordinator:do_wait(W, ReqID).

do_mput(Preflist, Data, W) ->
    ReqID = make_ref(),
    metric_vnode:mput(Preflist, ReqID, Data),
    request_coordinator:do_wait(W, ReqID).
