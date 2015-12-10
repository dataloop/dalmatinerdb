-module(metric_vnode_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").

-define(P(Prop, Config), ct_property_test:quickcheck(Prop, Config)).
-define(EQC, metric_vnode_eqc).

all() ->
    [{group, eqc}].

groups() ->
    [{eqc, [], [prop_gb_comp,
                prop_is_empty,
                prop_empty_after_delete,
                prop_handoff]}].

init_per_suite(Config) ->
    ok = start_cover(Config),
    ct_property_test:init_per_suite(Config).

end_per_suite(_) ->
    R = cover:analyse_to_file(),
    ct:pal("coverage results: ~p~n", [R]),
    ok.

%%
%% Tests
%%

prop_gb_comp(Config) ->
    ?P(?EQC:prop_gb_comp(), Config).

prop_is_empty(Config) ->
    ?P(?EQC:prop_is_empty(), Config).

prop_empty_after_delete(Config) ->
    ?P(?EQC:prop_empty_after_delete(), Config).

prop_handoff(Config) ->
    ?P(?EQC:prop_handoff(), Config).

%%
%% Helpers
%%

start_cover(Config) ->
    {ok, _} = cover:start(),
    Modules = [metric_vnode], %app_modules(metric_vnode),
    ct:pal("modules: ~p~n", [Modules]),
    [ {module, M} = code:ensure_loaded(M) || M <- Modules ],
    Paths = [ path(M) || M <- Modules ],
    Compiled = cover:compile(Paths),
    ct:pal("code dir: ~p~n", [code:root_dir()]),
    ct:pal("config: ~p~n", [Config]),
    ct:pal("compiled: ~p~n", [Compiled]),
    case lists:all(fun ({ok, _}) -> true;
                       (_)       -> false end,
                   Compiled)
    of
        true -> ok;
        _    -> error
    end.

app_modules(App) ->
    case application:load(App) of
        ok -> ok;
        {error, {already_loaded, App}} -> ok
    end,
    {ok, Keys} = application:get_all_key(App),
    {modules, Modules} = lists:keyfind(modules, 1, Keys),
    Modules.

path(Mod) ->
    CompileInfo = Mod:module_info(compile),
    {source, Path} = lists:keyfind(source, 1, CompileInfo),
    Path.
