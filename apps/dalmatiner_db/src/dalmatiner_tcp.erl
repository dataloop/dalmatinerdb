-module(dalmatiner_tcp).

-behaviour(ranch_protocol).

-include_lib("dproto/include/dproto.hrl").
-include_lib("mmath/include/mmath.hrl").

-export([start_link/4]).
-export([init/4]).

-record(state,
        {n = 1 :: pos_integer(),
         w = 1 :: pos_integer()
        }).

-record(sstate,
        {last = undefined :: non_neg_integer() | undefined,
         max_diff = 1 :: pos_integer(),
         dict :: bkt_dict:bkt_dict()}).


-type state() :: #state{}.

-type stream_state() :: #sstate{}.

start_link(Ref, Socket, Transport, Opts) ->
    Pid = spawn_link(?MODULE, init, [Ref, Socket, Transport, Opts]),
    {ok, Pid}.

init(Ref, Socket, Transport, _Opts = []) ->
    {ok, N} = application:get_env(dalmatiner_db, n),
    {ok, W} = application:get_env(dalmatiner_db, w),
    State = #state{n=N, w=W},
    ok = Transport:setopts(Socket, [{packet, 4}]),
    ok = ranch:accept_ack(Ref),
    loop(Socket, Transport, State).

-spec loop(port(), term(), state()) -> ok.

loop(Socket, Transport, State) ->
    case Transport:recv(Socket, 0, 5000) of
        {ok, Data} ->
            case dproto_tcp:decode(Data) of
                buckets ->
                    Res = metric:list(),
                    send_listing(Socket, Transport, buckets, Res, []),
                    loop(Socket, Transport, State);
                {list, Bucket} ->
                    Res = metric:list(Bucket),
                    send_listing(Socket, Transport, metrics, Res,
                                    [Bucket]),
                    loop(Socket, Transport, State);
                {list, Bucket, Prefix} ->
                    Res = metric:list(Bucket, Prefix),
                    send_listing(Socket, Transport, metrics, Res,
                                    [Bucket, Prefix]),
                    loop(Socket, Transport, State);
                {get, B, M, T, C} ->
                    do_send(Socket, Transport, B, M, T, C),
                    loop(Socket, Transport, State);
                {stream, Bucket, Delay} ->
                    lager:info("[tcp] Entering stream mode for bucket '~s' "
                               "and a max delay of: ~p", [Bucket, Delay]),
                    ok = Transport:setopts(Socket, [{packet, 0}]),
                    stream_loop(Socket, Transport,
                                #sstate{max_diff = Delay,
                                        dict = bkt_dict:new(Bucket,
                                                            State#state.n,
                                                            State#state.w)},
                                {incomplete, <<>>})
            end;
        {error, timeout} ->
            loop(Socket, Transport, State);
        {error, closed} ->
            ok;
        E ->
            lager:error("[tcp:loop] Error: ~p~n", [E]),
            ok = Transport:close(Socket)
    end.

send_listing(Socket, Transport, Entity, Res, Args) ->
    case Res of
        {ok, Ms} ->
            Transport:send(Socket, dproto_tcp:encode_metrics(Ms));
        {error, Error} ->
            lager:error("[tcp] Error listing ~p with args ~p: ~p",
                        [Entity, Args, Error])
    end.

do_send(Socket, Transport, B, M, T, C) ->
    PPF = dalmatiner_opt:ppf(B),
    %% Assume that splits is always non-empty
    Splits = mstore:make_splits(T, C, PPF),
    %% Set the socket to no package control so we can do that ourselfs.
    Transport:setopts(Socket, [{packet, 0}]),
    %% TODO: make this math for configureable length
    %% 8 (resolution + points)
    Size = 8 + (C * 8),
    PartNum = 0,
    send_parts(Socket, Transport, Size, PartNum, PPF, B, M, Splits).

send_part(Socket, Transport, Size, N, {PPF, B, M, T, C} = PartCriteria) ->
    case metric:get(B, M, PPF, T, C) of
        {ok, Resolution, Points} ->
            Padding = mmath_bin:empty(C - mmath_bin:length(Points)),

            case N of
                PartNum when PartNum =:= 0 ->
                    Transport:send(Socket, <<Size:32/integer,
                                             Resolution:64/integer,
                                             Points/binary,
                                             Padding/binary>>);
                _ ->
                    Transport:send(Socket, <<Points/binary, Padding/binary>>)
            end;
        {error, Error} ->
            lager:error("[tcp] ~p error getting metric part with args ~p",
                        [Error, PartCriteria]),
            {error, Error}
    end.

send_parts(Socket, Transport, _Size, _N, _PPF, _B, _M, []) ->
    %% Reset the socket to 4 byte packages
    Transport:setopts(Socket, [{packet, 4}]);

send_parts(Socket, Transport, Size, N, PPF, B, M, [{T, C} | Splits]) ->
    ReadInfo = {PPF, B, M, T, C},
    send_part(Socket, Transport, Size, N, ReadInfo),
    send_parts(Socket, Transport, Size, N+1, PPF, B, M, Splits).

-spec stream_loop(port(), term(), stream_state(),
                  {dproto_tcp:stream_message(), binary()}) ->
                         ok.
stream_loop(Socket, Transport,
            State = #sstate{dict = Dict},
            {flush, Rest}) ->
    Dict1 = flush(Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1},
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport,
            State = #sstate{dict = Dict, last = undefined},
            {{stream, Metric, Time, Points}, Rest}) ->
    Dict1 = bkt_dict:add(Metric, Time, Points, Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1, last = Time},
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport,
            State = #sstate{last = _L, max_diff = _Max, dict = Dict},
            {{stream, Metric, Time, Points}, Rest})
  when Time - _L > _Max ->
    Dict1 = flush(Dict),
    Dict2 = bkt_dict:add(Metric, Time, Points, Dict1),
    stream_loop(Socket, Transport, State#sstate{dict = Dict2, last = undefined},
                dproto_tcp:decode_stream(Rest));

stream_loop(Socket, Transport, State = #sstate{dict = Dict},
            {{stream, Metric, Time, Points}, Acc}) ->
    Dict1 = bkt_dict:add(Metric, Time, Points, Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1},
                dproto_tcp:decode_stream(Acc));

stream_loop(Socket, Transport, State = #sstate{dict = Dict},
            {{batch, Time}, Acc}) ->
    %% When entering batch mode we make sure to drain the dict first and
    %% set last as undefined since we'll flush at the end too.
    %% TODO: figure out if this flushing makes sense or if we can make it
    %% conditional
    Dict1 = flush(Dict),
    batch_loop(Socket, Transport, State#sstate{dict = Dict1, last = undefined},
               Time, dproto_tcp:decode_batch(Acc));

stream_loop(Socket, Transport, State = #sstate{max_diff = D},
            {incomplete, Acc}) ->
    case Transport:recv(Socket, 0, min(D * 1000, 5000)) of
        {ok, Data} ->
            Acc1 = <<Acc/binary, Data/binary>>,
            stream_loop(Socket, Transport, State,
                        dproto_tcp:decode_stream(Acc1));
        {error, timeout} ->
            stream_loop(Socket, Transport, State, {incomplete, Acc});
        {error, closed} ->
            bkt_dict:flush(State#sstate.dict),
            ok;
        E ->
            error(E, Transport, Socket, State)
    end.

-spec batch_loop(port(), term(), stream_state(), non_neg_integer(),
                 {dproto_tcp:batch_message(), binary()}) ->
                        ok.

batch_loop(Socket, Transport, State = #sstate{dict = Dict}, _Time,
           {batch_end, Acc}) ->
    Dict1 = flush(Dict),
    stream_loop(Socket, Transport, State#sstate{dict = Dict1},
               dproto_tcp:decode_stream(Acc));

batch_loop(Socket, Transport, State  = #sstate{dict = Dict}, Time,
           {{batch, Metric, Point}, Acc}) ->
    Dict1 = bkt_dict:add(Metric, Time, Point, Dict),
    batch_loop(Socket, Transport, State#sstate{dict = Dict1}, Time,
               dproto_tcp:decode_batch(Acc));


batch_loop(Socket, Transport, State, Time, {incomplete, Acc}) ->
    case Transport:recv(Socket, 0, 1000) of
        {ok, Data} ->
            Acc1 = <<Acc/binary, Data/binary>>,
            batch_loop(Socket, Transport, State, Time,
                        dproto_tcp:decode_batch(Acc1));
        {error, timeout} ->
            batch_loop(Socket, Transport, State, Time, {incomplete, Acc});
        {error, closed} ->
            bkt_dict:flush(State#sstate.dict),
            ok;
        E ->
            error(E, Transport, Socket, State)
    end.

flush(Dict) ->
    Dict1 = bkt_dict:flush(Dict),
    drain(),
    Dict1.

drain() ->
    receive
        _ ->
            drain()
    after
        0 ->
            ok
    end.

error(E, Transport, Socket, #sstate{dict = Dict}) ->
    lager:error("[tcp:stream] Error: ~p~n", [E]),
    bkt_dict:flush(Dict),
    ok = Transport:close(Socket).
